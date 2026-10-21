"""A launch: one project's traffic jumps while its team watches Live.

The project (its own token, written only by this script) sends --base events a second, then from --at it
ramps to --peak for --duration seconds (a launch post, a Product Hunt day, a launch email going out), then
falls back. Its events all land on one lane partition, in order, so the live node must keep showing every
one of them, and the other projects' views on the same node must stay as fresh as before.

One batch per --tick, sized to what the tick owes, so the spike itself does not flood the broker with tiny
requests.
"""
import argparse
import asyncio
import gc
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, LANE, LAUNCH, STREAM, ensure_topics, msg, part


async def main(a):
    client = IggyClient.from_connection_string(a.url)     # one connection for this producer
    await client.connect()
    parts = await ensure_topics(client, LANE, partitions={LANE: a.partitions})
    out = await client.producer(STREAM, LANE, topic_partitions_count=parts,
                                mode=DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=500)))
    pid = Partitioning.partition_id(part(LAUNCH, parts))
    r = random.Random(9)
    paths = ["/", "/launch", "/pricing", "/docs/getting-started", "/signup"]
    seq, sent, owed = 0, 0, 0.0
    gc.freeze()
    t0 = due = last_print = time.monotonic()
    async with out:
        while a.run_for == 0 or time.monotonic() - t0 < a.run_for:
            el = time.monotonic() - t0
            rate = a.peak if a.at <= el < a.at + a.duration else a.base
            owed += rate * a.tick
            n = int(owed)
            owed -= n
            if n:
                now_ns = time.time_ns()
                batch = []
                for _ in range(n):
                    seq += 1
                    batch.append(SendMessage(msg(t=LAUNCH, s=seq, d=f"v{r.randrange(a.visitors)}",
                                                 e=r.choice(EVENTS), p=r.choice(paths), ns=now_ns)))
                await out.send_with_partitioning(batch, pid)
                sent += n
            if time.monotonic() - last_print >= 1:
                last_print += 1
                phase = "LAUNCH" if a.at <= el < a.at + a.duration else "normal"
                print(f"[launch] {sent:,} events/s for {LAUNCH} ({phase})", flush=True)
                sent = 0
            due += a.tick
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[launch] done", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--base", type=float, default=200, help="events a second before and after the launch")
    ap.add_argument("--peak", type=float, default=10000, help="events a second during the launch")
    ap.add_argument("--at", type=float, default=30, help="seconds after start the launch begins")
    ap.add_argument("--duration", type=float, default=60, help="seconds the launch traffic lasts")
    ap.add_argument("--visitors", type=int, default=200000, help="people who might visit")
    ap.add_argument("--partitions", type=int, default=256, help="lane partitions, used if this creates the topic")
    ap.add_argument("--tick", type=float, default=0.02)
    ap.add_argument("--run-for", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
