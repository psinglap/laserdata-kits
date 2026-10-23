"""The conversation view in Shopify Inbox: each chatting buyer's cart and order state, kept current from the
lane, and every reply checked against it.

Two readers, each reading every partition of its topic directly on connections of its own (no consumer
group: a group reader visits one partition per poll, so with 16 partitions a record waits its turn):
  * buyer-signals (the lane): applies each cart and checkout event to the buyer's conversation (the events
    shown in the conversation, and the header's status, item quantity and subtotal), ignoring any older or
    repeated sequence, so a late cart event never reopens a cart after the order. Signal-to-conversation
    latency is timed here.
  * inbox-replies: plays the Inbox agent and the merchant's staff. It asks: did the conversation already
    show the buyer's last signal when the reply started? If not, the reply was written from a stale view,
    and if that view showed another status or subtotal, the reply saw the wrong cart. Worst case: the
    buyer had already placed the order and the conversation still showed an open cart.

--hold-ms is a what-if: apply each signal only once it is that old, as a batching path would.
Prints signal-to-conversation p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the
business numbers: replies written without the buyer's last signal, on a wrong cart, and after an order
the conversation did not show yet.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import (DEFAULT_URL, KEEP_EVENTS, ORDERED, PARTITIONS, REPLIES, SIGNALS, STREAM, Window, ensure_topics,
                    load)


async def main(a):
    async def own_client():                 # one connection per reader, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SIGNALS, REPLIES)

    # buyer -> [seq, status, items, subtotal, order, when applied (ns), last seen (monotonic), recent events]
    conv = {}
    lat, stats = Window(), defaultdict(int)
    caught_up = [False]                     # a new reader first replays signals sent before it joined
    held = deque()                          # what-if only: signals waiting until they are --hold-ms old
    gc.freeze()

    def apply(v):
        b = v["buyer"]
        c = conv.get(b)
        if c and v["seq"] <= c[0]:
            stats["dropped"] += 1           # older or repeated signal: already shown
            return
        now = time.time_ns()
        if c is None:
            c = conv[b] = [0, "", 0, 0, "", 0, 0.0, deque(maxlen=KEEP_EVENTS)]
        # Each signal carries the buyer's resulting state, so the header is replaced, never recomputed.
        c[0], c[1], c[2], c[3], c[4], c[5], c[6] = (v["seq"], v["status"], v["items"], v["subtotal"], v["order"],
                                                    now, time.monotonic())
        c[7].append((v["kind"], v["product"] or v["order"]))
        age = (now - v["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest signal seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)

    async def on_signal(m):
        v = load(m.payload())
        if a.hold_ms:
            held.append(v)
        else:
            apply(v)

    async def release():                    # what-if only: apply held signals once they are old enough
        while True:
            cut = time.time_ns() - a.hold_ms * 1_000_000
            while held and held[0]["ns"] <= cut:
                apply(held.popleft())
            await asyncio.sleep(0.002)

    async def on_reply(m):
        r = load(m.payload())
        if not caught_up[0] or time.time_ns() - r["ns"] > 5e9:
            return                          # replayed history, not a reply being written now
        c = conv.get(r["buyer"])
        seq, status, subtotal, applied = (c[0], c[1], c[3], c[5]) if c else (0, "", 0, 0)
        stats["replies"] += 1
        # The reply started at r["ns"]. Did the conversation already show the buyer's last signal?
        # (Both clocks are wall clocks; across machines, keep them NTP-synced.)
        if seq < r["needs"] or (seq == r["needs"] and applied > r["ns"]):
            stats["stale"] += 1
            if (status, subtotal) != (r["status"], r["subtotal"]):
                stats["wrong"] += 1         # the reply saw another cart than the buyer has
                if r["status"] == ORDERED:
                    stats["unseen_order"] += 1   # the buyer had paid; the conversation still showed a cart

    async def sweep():
        while True:
            await asyncio.sleep(30)         # forget buyers idle past a conversation's life (--keep-s)
            cut = time.monotonic() - a.keep_s
            for b in [b for b, c in conv.items() if c[6] < cut]:
                del conv[b]

    async def report():
        while True:
            await asyncio.sleep(1)
            # Move what survived the last second (conversations) out of the collector's reach, so a full
            # collection never walks hundreds of thousands of long-lived objects and pauses the readers.
            gc.freeze()
            if not caught_up[0]:
                # caught up once a whole second of signals arrived inside the budget; partitions interleave,
                # so one fresh signal alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms + a.hold_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {stats['replayed']:,} earlier signals this second", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms + a.hold_ms else "FAIL"
            stale = 100 * stats["stale"] / stats["replies"] if stats["replies"] else 0
            print(f"[{a.name}] {r['n']:>7,} signals/s  signal-to-conversation p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms + a.hold_ms}ms)  conversations {len(conv):,}",
                  flush=True)
            print(f"[replies] {stats['replies']:,}  written without the last signal {stale:.3f}%  "
                  f"on a wrong cart {stats['wrong']:,}  order placed but not shown {stats['unseen_order']:,}", flush=True)
            stats.clear()

    async def read(topic, handler, pids):
        # Each partition in turn, from the start of the log (the conversations are rebuilt from it), then from
        # this reader's own position; nothing is stored on the broker. A full batch means more is waiting.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{topic}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, topic, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        await handler(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def readers(topic, handler):
        parts = list(range(PARTITIONS[topic]))
        return [read(topic, handler, parts[r::a.readers]) for r in range(a.readers)]

    await asyncio.gather(*readers(SIGNALS, on_signal), *readers(REPLIES, on_reply), release(), sweep(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="inbox")
    p.add_argument("--budget-ms", type=int, default=50, help="signal-to-conversation p99 budget for the lane")
    p.add_argument("--hold-ms", type=int, default=0, help="what-if: apply each signal only once it is this old")
    p.add_argument("--readers", type=int, default=2, help="connections reading each topic's partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--keep-s", type=float, default=7 * 86400, help="seconds to keep an idle buyer's conversation (7 days)")
    asyncio.run(main(p.parse_args()))
