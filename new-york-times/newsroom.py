"""The live desk: every version of every asset being published right now, written once to the publish-to-site lane.

Stands in for the denormalized log's output for the assets editors are working on: live coverage taking a new
post every ~45 s, breaking stories updated every ~2 min, and articles updated or corrected every ~15 min. One
writer per partition, each with its own connection, writing once per --tick-ms with the versions that came in
since the last write, so every asset's versions stay in order. A message is the whole asset at that version
(about --body-bytes of bundled article), never a diff.

On start it writes version 1 of every live asset (the state before the run). Newsroom time is compressed:
--speed 20 (the default) publishes 20 times faster than real time so every second has versions to time;
--speed 1 is real time. Headlines and ids are illustrative.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, KINDS, LANE, PARTITIONS, STREAM, Window, connect, ensure_topic, kind_of, live_uris, \
    pack, partition_of

DESKS = ["Election Results", "Markets", "Storm Tracker", "World", "Sports", "Climate", "Science", "Metro"]


class Asset:
    def __init__(self, uri: str, kind: str, i: int):
        self.uri, self.kind, self.version, self.due = uri, kind, 0, 0.0
        self.headline = f"{'Live: ' if kind == 'live' else ''}{DESKS[i % len(DESKS)]} story {i}"

    def first(self, body: int) -> bytes:
        self.version += 1
        return pack(self.uri, self.version, "publish", self.headline, body)

    def next(self, rng, body: int, waited_ns: int) -> bytes:
        self.version += 1
        if self.kind == "live":
            why = "post"                                        # a new post on live coverage is a new version
        else:
            why = rng.choices(["update", "correction"], [9, 1])[0]
        return pack(self.uri, self.version, why, self.headline, body, waited_ns)


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    assets = [Asset(u, kind_of(i), i) for i, u in enumerate(live_uris(a.assets))]
    by_part = [[x for x in assets if partition_of(x.uri) == p] for p in range(PARTITIONS)]
    rng = random.Random(1851)
    acks, sent, t0 = Window(), [0], time.monotonic()
    ready, started = [0], asyncio.Event()

    def gap(x):
        return KINDS[x.kind] / a.speed * rng.uniform(0.5, 1.5)

    async def writer(pid):
        c = await connect(a.url)
        mine = by_part[pid]
        if mine:
            await c.send_messages(STREAM, LANE, pid, [SendMessage(x.first(a.body_bytes)) for x in mine])
        ready[0] += 1
        await started.wait()
        for x in mine:                                          # first updates spread over one interval
            x.due = t0 + rng.random() * KINDS[x.kind] / a.speed
        due = time.monotonic()
        while True:
            now = time.monotonic()
            batch = []
            for x in mine:
                while x.due <= now:
                    # A version is ready at x.due and waits for this write; that wait counts as latency.
                    batch.append(SendMessage(x.next(rng, a.body_bytes, int((now - x.due) * 1e9))))
                    x.due += gap(x)
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, LANE, pid, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
                sent[0] += len(batch)
            due += a.tick_ms / 1000                             # from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def go():
        nonlocal t0
        while ready[0] < PARTITIONS:
            await asyncio.sleep(0.05)
        kinds = {}
        for x in assets:
            kinds[x.kind] = kinds.get(x.kind, 0) + 1
        print(f"[newsroom] version 1 of {len(assets)} live assets ("
              + ", ".join(f"{n} {k}" for k, n in kinds.items()) + f") across {PARTITIONS} partitions", flush=True)
        gc.freeze()                                             # keep startup objects out of the collector's way
        t0 = time.monotonic()
        started.set()

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            print(f"[newsroom] {sent[0]:>5,} versions/s  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  "
                  f"p99 {r['p99']:.1f}ms", flush=True)
            sent[0] = 0

    await asyncio.gather(go(), report(), *(writer(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--assets", type=int, default=400, help="assets on the live desk at once")
    p.add_argument("--speed", type=float, default=20, help="newsroom time vs real time (1 = real time)")
    p.add_argument("--body-bytes", type=int, default=4000, help="size of the bundled asset in each version")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    asyncio.run(main(p.parse_args()))
