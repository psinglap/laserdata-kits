"""The click-to-install scorer, fed by the click-install lane: the stand-in for mFaaS's real-time CTIT check.

Reads click-install directly, partition by partition, on --readers connections of its own (no consumer group:
a group reader visits one partition per poll, so with 16 partitions a record waits for its partition's turn).
Records are keyed by device, so a device's click is always ahead of its install on the same partition:
  * click:   device, publisher, redirect time, the node's sequence for that device;
  * install: device, app, the node's sequence for that device.
A repeated or replayed record (sequence not newer than the last one applied) changes nothing.

On each install it takes the device's last click (last-click attribution), computes click-to-install time
(CTIT) and adds it to that publisher's window of the last --window-s seconds. decide() is where your rule goes:
here a publisher is BLOCKED once it has at least --min-installs attributed installs in the window and at least
--short-share of them came in under --inject-s seconds after their click (click injection: a click fired as an
install begins, to take the credit). Each block is published on publisher-blocks, which every attribution node
reads (attribution_nodes.py). A block carries the publisher and the resulting state, so a restarted scorer
rebuilds every device's last click and every publisher's window from the log.

Prints event-to-scorer p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, the installs scored with
their click present, and the blocks.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import BLOCKED, BLOCKS, DEFAULT_URL, EVENTS, INJECT_S, STREAM, Window, ensure_topics, load, msg


class PubWindow:
    """The last --window-s seconds of attributed installs for one publisher, with running counts."""
    def __init__(self):
        self.q = deque()
        self.n = self.short = 0

    def add(self, ns, short, span):
        self.q.append((ns, short))
        self.n += 1
        self.short += short
        while self.q and self.q[0][0] < ns - span:
            _, s = self.q.popleft()
            self.n -= 1
            self.short -= s


def decide(w: PubWindow, a) -> bool:
    """Stands in for your mFaaS rule (CTIT share here; add redirect time, network signals, device scores)."""
    return w.n >= a.min_installs and w.short >= a.short_share * w.n


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), EVENTS, BLOCKS)
    out = await (await own_client()).producer(STREAM, BLOCKS, topic_partitions_count=1)
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (node, device) -> last sequence applied
    last_click = {}                         # device -> (publisher, click ns)
    clicks_by_age = deque()                 # (click ns, device), to forget clicks past the lookback
    window = defaultdict(PubWindow)         # publisher -> recent attributed installs
    blocked = {}                            # publisher -> ns the block was decided
    pending = []                            # blocks waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays events from before it joined
    span, lookback = int(a.window_s * 1e9), int(a.lookback_s * 1e9)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_event(m):
        d = load(m.payload())
        now = time.time_ns()
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        dev, sk = d["d"], (d["w"], d["d"])
        if d["seq"] <= seq.get(sk, 0):
            stats["dropped"] += 1           # older or repeated event: already applied
            return
        seq[sk] = d["seq"]
        t = d.get("t", d["ns"])             # event time (the bridge carries yours); the kit's are the same as ns
        if d["e"] == "c":
            stats["clicks"] += 1
            last_click[dev] = (d["pub"], t)
            clicks_by_age.append((t, dev))
            while clicks_by_age and clicks_by_age[0][0] < t - lookback:
                ns0, d0 = clicks_by_age.popleft()
                if last_click.get(d0, (None, 0))[1] == ns0:
                    del last_click[d0]
            return
        stats["installs"] += 1
        c = last_click.get(dev)
        if c is None:
            stats["orphans"] += 1           # install with no click on the lane yet: organic, or its click is late
            return
        pub, click_ns = c
        ctit = (t - click_ns) / 1e9
        short = ctit < a.inject_s
        stats["short"] += short
        if pub in blocked:
            return
        w = window[pub]
        w.add(t, short, span)
        if not decide(w, a):
            return
        blocked[pub] = t
        stats["blocks"] += 1
        if age > a.replay_verdict_s * 1000:
            return                          # replayed from long ago: state rebuilt, nothing to tell the nodes
        pending.append(SendMessage(msg(pub=pub, state=BLOCKED, n=w.n, short=w.short, reason="ctit")))
        if caught_up[0]:
            print(f"[{a.name}] {pub} -> BLOCKED  {w.short}/{w.n} installs under {a.inject_s:g} s CTIT", flush=True)

    async def send_blocks():
        # Blocks are small and rare; one batch every 2 ms keeps them off a send-per-block loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def read(pids):
        # Each partition in turn, from the start of the log (state is rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = Consumer.Single(f"{a.name}-{os.getpid()}")
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_event(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier events this second, "
                      f"last click held for {len(last_click):,} devices, {len(blocked):,} publishers blocked",
                      flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            inst = int(stats["installs"])
            print(f"[{a.name}] {r['n']:>7,} events/s  event-to-scorer p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  clicks {int(stats['clicks']):,}  "
                  f"installs {inst:,} (with their click {inst - int(stats['orphans']):,}, short CTIT "
                  f"{int(stats['short']):,})  blocks sent {int(stats['sent']):,}  "
                  f"blocked {len(blocked):,}", flush=True)
            stats.clear()

    parts = list(range((await (await own_client()).get_topic(STREAM, EVENTS)).partitions_count))
    await asyncio.gather(send_blocks(), report(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--readers", type=int, default=2, help="connections reading the click-install partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--name", default="ctit")
    p.add_argument("--budget-ms", type=int, default=50, help="event-to-scorer p99 budget for the lane")
    p.add_argument("--inject-s", type=float, default=INJECT_S, help="CTIT under which an install counts as short")
    p.add_argument("--window-s", type=float, default=30, help="seconds of attributed installs judged per publisher")
    p.add_argument("--min-installs", type=int, default=15, help="installs needed before a publisher can be blocked")
    p.add_argument("--short-share", type=float, default=0.5, help="share of short-CTIT installs that blocks")
    p.add_argument("--lookback-s", type=float, default=600,
                   help="how long a device's last click is kept (your attribution window; compressed here)")
    p.add_argument("--replay-verdict-s", type=float, default=10,
                   help="after a restart, still publish blocks decided on events younger than this")
    asyncio.run(main(p.parse_args()))
