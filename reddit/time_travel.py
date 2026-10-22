"""A Time-Travel replay next door: reads the preliminary content stream from its first offset while the
live lane runs.

REV2's Time-Travel lets a Safety Operator run a new rule over content written before the rule existed: the
rule's consumer is moved back to a start time and works through the backlog. This script does that to the
lane's own content-preliminary topic: from --at seconds it reads every partition from the first offset, at
up to --rate messages a second, runs a cheap match on each message (a stand-in rule), and when it reaches
the end starts over (the next rule time-travels). It never writes. It is the neighbour for the isolation
test: the joiner and REV2 must stay inside their budgets while it runs.
"""
import argparse
import asyncio
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, PRELIM_TOPIC, STREAM, connect, ensure_all


async def main(a):
    admin = await connect(a.url)
    parts = await ensure_all(admin, a.partitions)
    await asyncio.sleep(a.at)
    c = await connect(a.url)
    consumers = {p: Consumer.Single(f"time-travel-p{p}") for p in range(parts)}
    nxt = {p: 0 for p in range(parts)}
    n, hits, passes, last = 0, 0, 0, time.monotonic()
    budget = a.rate / 1000 * a.tick_ms                  # messages per tick
    print(f"[time-travel] replaying {STREAM}/{PRELIM_TOPIC} from the first offset, up to {a.rate:,.0f} msgs/s",
          flush=True)
    tick = time.monotonic()
    while True:
        got = 0
        for p in range(parts):
            if got >= budget:
                break
            ms = await c.poll_messages(STREAM, PRELIM_TOPIC, consumer=consumers[p], partition_id=p,
                                       count=int(min(a.batch, budget - got)) or 1, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt[p]))
            if ms:
                nxt[p] = ms[-1].offset() + 1
                got += len(ms)
                hits += sum(1 for m in ms if b'"media":1' in m.payload())   # the stand-in rule
        n += got
        if got < budget:                                # reached the head of every partition: the next rule
            nxt = {p: 0 for p in range(parts)}
            passes += 1
        if time.monotonic() - last >= 1:
            print(f"[time-travel] {n:>7,} msgs/s replayed  {hits:>6,} matched  passes {passes}", flush=True)
            n, hits, last = 0, 0, time.monotonic()
        tick += a.tick_ms / 1000
        await asyncio.sleep(max(0.0, tick - time.monotonic()))
        tick = max(tick, time.monotonic() - 0.1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=100_000, help="replay rate cap, messages a second")
    p.add_argument("--at", type=float, default=0, help="start after this many seconds")
    p.add_argument("--partitions", type=int, default=32, help="partitions for new lane topics")
    p.add_argument("--batch", type=int, default=2000)
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
