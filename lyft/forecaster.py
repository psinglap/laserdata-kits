"""The source of the lane: a stand-in for the real-time forecasting models, publishing a forecast frame for every
region every --cadence-s seconds.

Lyft's forecasting post describes demand and supply forecasts at geohash-6 level for every 5-minute interval of
the next hour, refit and run every minute, with the results synced to a Kafka topic that Rider Pricing, Driver
Earnings and other services read in real time. This script publishes that shape onto Apache Iggy: for each region,
the frame's cells in chunks (each cell: its geohash-6, 12 demand values and 12 supply values), then a seal with the
frame number and the chunk count. Every message of a region goes to the region's partition, so the seal arrives
after the last chunk, and a reader that has the seal has the whole frame.

The frame number is the per-region sequence; a reader ignores a frame older than the one it holds. Values are
illustrative (a few precomputed variants per cell, so a large frame costs little Python time to build); one
region gets a concert letting out at --event-at seconds, so its demand forecast jumps for a few frames.

Each partition's messages are stamped and sent as soon as they are built, so build time is not hidden inside
the measured latency. Frames are scheduled from their due time, never from "now".
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, FORECASTS, HORIZONS, PARTITIONS, STREAM, cell, connect, ensure_topics, part_of,
                    region_key, region_prefix)

VARIANTS = 4


async def main(a):
    await ensure_topics(await connect(a.url), FORECASTS)
    writer = await connect(a.url)
    rng = random.Random(7)
    regions = [region_key(i) for i in range(a.regions)]
    by_part = defaultdict(list)
    for i, r in enumerate(regions):
        by_part[part_of(r)].append((i, r))

    # Precomputed cell fragments: for each region and cell, VARIANTS versions of '["9q8yyk",[d x12],[s x12]]'.
    def fragments(prefix, spike=1.0):
        out = []
        for j in range(a.cells):
            base_d, base_s = rng.uniform(0.2, 9), rng.uniform(0.5, 8)
            vs = []
            for _ in range(VARIANTS):
                d = [round(spike * base_d * rng.uniform(0.8, 1.2) * (1 + 0.02 * h), 1) for h in range(HORIZONS)]
                s = [round(base_s * rng.uniform(0.85, 1.15), 1) for _ in range(HORIZONS)]
                vs.append('["%s",[%s],[%s]]' % (cell(prefix, j), ",".join(map(str, d)), ",".join(map(str, s))))
            out.append(vs)
        return out

    frags = [fragments(region_prefix(i)) for i in range(a.regions)]
    concert = fragments(region_prefix(0), spike=3.0)       # region R000 when the venue lets out
    print(f"[forecaster] {a.regions} regions x {a.cells:,} geohash-6 cells = {a.regions * a.cells:,} cells per frame, "
          f"every {a.cadence_s:g} s, {a.chunk} cells per chunk", flush=True)
    gc.freeze()

    st = defaultdict(float)
    t0 = time.monotonic()

    async def frame(f):
        t_start = time.perf_counter()
        event = a.event_at and a.event_at <= time.monotonic() - t0 < a.event_at + a.event_for
        sends = []
        for p, rs in by_part.items():
            ms = []
            for i, r in rs:
                fr = concert if (i == 0 and event) else frags[i]
                n = (a.cells + a.chunk - 1) // a.chunk
                for c in range(n):
                    body = ",".join(fr[j][(f + j) % VARIANTS] for j in range(c * a.chunk, min(a.cells, (c + 1) * a.chunk)))
                    ms.append(SendMessage(('{"t":"c","r":"%s","f":%d,"i":%d,"ns":%d,"cells":[%s]}'
                                           % (r, f, c, time.time_ns(), body)).encode()))
                ms.append(SendMessage(('{"t":"s","r":"%s","f":%d,"of":%d,"ev":%d,"ns":%d}'
                                       % (r, f, n, 1 if (i == 0 and event) else 0, time.time_ns())).encode()))
                st["chunks"] += n
                st["seals"] += 1
            sends.append(asyncio.create_task(writer.send_messages(STREAM, FORECASTS, Partitioning.partition_id(p), ms)))
            await asyncio.sleep(0)          # let this partition's send go out while the next one is built
        await asyncio.gather(*sends)
        st["frames"] += 1
        st["cells"] += a.regions * a.cells
        st["publish_ms"] = max(st["publish_ms"], 1000 * (time.perf_counter() - t_start))

    async def frames():
        f, due = 0, time.monotonic()
        while True:
            f += 1
            st["late_ms"] = max(st["late_ms"], 1000 * (time.monotonic() - due))
            await frame(f)
            due += a.cadence_s
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            ev = " concert spike on R000" if a.event_at and a.event_at <= time.monotonic() - t0 < a.event_at + a.event_for else ""
            print(f"[forecaster] frames {int(st['frames'])}  cells {int(st['cells']):,}  chunks {int(st['chunks']):,}  "
                  f"seals {int(st['seals']):,}  build+send max {st['publish_ms']:.0f}ms  frame start late max "
                  f"{st['late_ms']:.0f}ms{ev}", flush=True)
            st.clear()

    await asyncio.gather(frames(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--regions", type=int, default=300, help="regions (Lyft operates in hundreds)")
    p.add_argument("--cells", type=int, default=400, help="geohash-6 cells per region (at most 1,024 here)")
    p.add_argument("--cadence-s", type=float, default=5, help="seconds between forecast frames (60 = once a minute)")
    p.add_argument("--chunk", type=int, default=200, help="cells per message")
    p.add_argument("--event-at", type=float, default=20, help="seconds in when a concert lets out in R000 (0 = never)")
    p.add_argument("--event-for", type=float, default=20)
    asyncio.run(main(p.parse_args()))
