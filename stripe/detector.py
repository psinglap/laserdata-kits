"""The card testing detector, fed by the attempt lane.

Reads every partition of card-attempts directly (no consumer group: a group reader visits one partition per
poll, so with 16 partitions an attempt waited for its partition's turn), on --readers connections of its own,
tracking its own position. Each record is one card setup or payment attempt: node, business, surface, amount,
card fingerprint, outcome, a per-(node, business) sequence. A replayed or repeated record is recognised by its
sequence and changes nothing.

Per business, over the last --window-ms, it decides whether the business is under a card testing attack.
under_attack() is a stand-in for your models: many attempts, mostly declined or failing a challenge, mostly
small or zero amounts, each on a different card. A flash sale is a spike of real buyers with normal amounts
and few declines, so it does not match. When a business matches, the detector publishes CONTROLS (here, a
challenge on every attempt) on attack-controls, which every payments API node reads (api_nodes.py). When the
business has seen no tester-shaped attempt for --ease-after s, it publishes EASED. Each change carries the
business's resulting state with a sequence (never "+1"), so a restarted detector or node rebuilds it from the log.

Prints attempt-to-detector p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the changes sent.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import ATTEMPTS, CONTROLS, DEFAULT_URL, PARTITIONS, STREAM, Window, ensure_topics, load, msg

BAD = ("declined", "challenge_failed")


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ATTEMPTS, CONTROLS)
    out = await (await own_client()).producer(STREAM, CONTROLS, topic_partitions_count=PARTITIONS[CONTROLS])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # (node, business) -> last attempt sequence applied
    recent = defaultdict(deque)             # business -> (ns, bad, small, card) of attempts inside the window
    controlled = {}                         # business -> ns of its last tester-shaped attempt
    pending = []                            # changes waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays attempts from before it joined
    window_ns = a.window_ms * 1_000_000
    replay_ns = a.replay_controls_s * 1_000_000_000
    gc.freeze()                             # keep start-up objects out of the collector's way

    def under_attack(rs) -> tuple[int, int] | None:
        """Stands in for your card testing models. Here: a burst of small attempts on fresh cards, mostly failing."""
        n = len(rs)
        if n < a.min_attempts:
            return None
        bad = sum(1 for _, x, _, _ in rs if x)
        small = sum(1 for _, _, x, _ in rs if x)
        cards = len({c for _, _, _, c in rs})
        if bad >= a.min_bad * n and small >= 0.8 * n and cards >= 0.9 * n:
            return n, bad
        return None

    def publish(b, state, age_ns, **evidence):
        if age_ns > replay_ns:
            return                          # replayed from long ago: this change is already in attack-controls
        stats[state] += 1
        pending.append(SendMessage(msg(b=b, seq=time.time_ns(), state=state, **evidence)))

    def on_attempt(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest attempt seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        sk = (d["s"], d["b"])
        if d["seq"] <= seq.get(sk, 0):
            stats["dropped"] += 1           # older or repeated record: already counted
            return
        seq[sk] = d["seq"]
        stats["attempts"] += 1
        b, bad, small = d["b"], d["out"] in BAD, d["amt"] < 2
        if b in controlled:
            if bad and small:
                controlled[b] = d["ns"]     # the attack is still running: keep the controls
            return
        rs = recent[b]
        rs.append((d["ns"], bad, small, d["card"]))
        while rs and rs[0][0] < d["ns"] - window_ns:
            rs.popleft()
        hit = under_attack(rs)
        if hit:
            controlled[b] = d["ns"]
            publish(b, "controls", now - d["ns"], action="challenge", attempts=hit[0], failed=hit[1])
            del recent[b]
        elif len(rs) == 1 and len(recent) > 200_000:
            for q in [q for q, v in recent.items() if v[-1][0] < d["ns"] - window_ns]:
                del recent[q]               # forget quiet businesses

    async def ease():
        # Every 100 ms, from its due time: ease the controls on businesses whose attack has stopped.
        due = time.monotonic()
        quiet_ns = int(a.ease_after * 1e9)
        while True:
            due += 0.1
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if not caught_up[0]:
                continue
            now = time.time_ns()
            for b, t in list(controlled.items()):
                if now - t > quiet_ns:
                    del controlled[b]
                    publish(b, "eased", now - t - quiet_ns, quiet_s=a.ease_after)

    async def read(pids):
        # Each partition in turn, from the start of the log (the windows are rebuilt from it), then from our
        # own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, ATTEMPTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_attempt(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_changes():
        # Changes are small and come in bursts; one batch every 2 ms keeps them off a send-per-change loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of attempts arrived inside the budget; partitions interleave,
                # so one fresh attempt alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier attempts this second, "
                      f"{len(controlled):,} businesses under controls so far", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} attempts/s  attempt-to-detector p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"changes sent {int(stats['sent']):,} (controls {int(stats['controls']):,}, "
                  f"eased {int(stats['eased']):,})  businesses under controls {len(controlled):,}", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[ATTEMPTS]))
    await asyncio.gather(send_changes(), report(), ease(), *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="detector")
    p.add_argument("--budget-ms", type=int, default=50, help="attempt-to-detector p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the attempt partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window-ms", type=float, default=1000, help="attack window per business")
    p.add_argument("--min-attempts", type=int, default=25, help="attempts inside the window to judge a business")
    p.add_argument("--min-bad", type=float, default=0.6, help="share declined or failing a challenge to call an attack")
    p.add_argument("--ease-after", type=float, default=2, help="seconds without tester-shaped attempts to ease")
    p.add_argument("--replay-controls-s", type=float, default=30,
                   help="after a restart, still publish changes for evidence younger than this")
    asyncio.run(main(p.parse_args()))
