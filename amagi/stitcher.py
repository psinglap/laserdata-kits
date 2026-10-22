"""FAST channels and the viewers watching them, as THUNDERSTORM's stitchers see them, plus an audit of every beacon.

Each linear channel takes an ad break every --cycle-s seconds (staggered across channels), of --slots ads of --ad-s
seconds each, cut into --seg-s segments. On a linear channel the break is the same moment for everyone watching,
so every viewer on the channel reaches it within --ramp-ms (player buffers differ a little), and the lane takes
the whole channel's segments at once. A share of viewers (--leave) zap away partway through an ad: they fetch no
more of its segments, so its later quartiles must never fire.

Every ad segment served goes on ad-segments, keyed by session, one batch per partition per tick (--tick-ms), all
sent at once. With --dup-rate a share of segment events is sent twice, as a retried or at-least-once pipeline
does; the tracker must fire nothing twice.

The audit reads every partition of beacons-fired on its own connection and checks each beacon against the
quartiles the served segments made due: beacons fired twice, beacons fired for a quartile that was never reached,
and, once the run has drained, beacons that never fired. It also times each beacon record from the segment that
made it due (both hops).

Prints a line a second, and an audit line at the end.
"""
import argparse
import asyncio
import gc
import math
import random
import time
import zlib
from collections import defaultdict

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FIRED, PARTITIONS, SEGMENTS, STREAM, Stalls, Window, channel, connect,
                    due_quartiles, ensure_topics, load, msg, part, session)


async def main(a):
    await ensure_topics(await connect(a.url), SEGMENTS, FIRED)
    writer, auditor = await connect(a.url), await connect(a.url)    # one connection each way
    P = PARTITIONS[SEGMENTS]
    seg_ms, ad_ms = int(a.seg_s * 1000), int(a.ad_s * 1000)
    nsegs = math.ceil(ad_ms / seg_ms)
    cycle_ms = int(a.cycle_s * 1000)
    assert a.ramp_ms + a.slots * ad_ms < cycle_ms, "a break must end before the channel's next break"
    sess = [session(i) for i in range(a.viewers)]
    spart = [part(s, P) for s in sess]
    chans = [channel(c) for c in range(a.channels)]
    offset = [c * cycle_ms // a.channels for c in range(a.channels)]   # breaks staggered across channels
    # Per channel, every segment fetch of one break, sorted by time into the break: (ms, viewer, slot, segment).
    sched = [[] for _ in chans]
    for i in range(a.viewers):
        c, d = i % a.channels, zlib.crc32(sess[i].encode()) % max(1, int(a.ramp_ms))
        for k in range(a.slots):
            for g in range(nsegs):
                sched[c].append((d + k * ad_ms + g * seg_ms, i, k, g))
    for s in sched:
        s.sort()
    rng = random.Random(7)
    start_ns = time.time_ns()
    start_ms = start_ns // 1_000_000

    expected = {}                    # (impression, quartile) -> ns the segment that made it due was served
    got = set()                      # (impression, quartile) recorded in beacons-fired
    st, run, lat, stalls = defaultdict(int), defaultdict(int), Window(), Stalls()
    sending = [True]
    gc.freeze()                      # keep start-up tables out of the collector's way

    async def serve():
        cur = [None] * a.channels    # the break each channel is in
        ptr = [0] * a.channels
        left = [dict() for _ in chans]   # (viewer, slot) -> first segment this viewer no longer fetches
        a0 = [dict() for _ in chans]     # (viewer, slot) -> when this viewer's ad began
        due = time.monotonic()
        end = due + a.duration
        while time.monotonic() < end:
            now_ms = time.time_ns() // 1_000_000
            batches = defaultdict(list)
            breaking = 0
            for c in range(a.channels):
                b = (now_ms - offset[c]) // cycle_ms
                into = now_ms - offset[c] - b * cycle_ms
                if b != cur[c]:
                    cur[c], left[c], a0[c] = b, {}, {}
                    ptr[c] = 0 if offset[c] + b * cycle_ms >= start_ms else len(sched[c])   # skip one under way
                s, lc, ac = sched[c], left[c], a0[c]
                if ptr[c] < len(s):
                    breaking += 1
                while ptr[c] < len(s) and s[ptr[c]][0] <= into:
                    _, i, k, g = s[ptr[c]]
                    ptr[c] += 1
                    vk = (i, k)
                    if g == 0:
                        if rng.random() < a.leave:
                            lc[vk] = rng.randrange(1, nsegs)    # zaps away partway through this ad
                            run["left"] += 1
                        ac[vk] = time.time_ns()
                    if g >= lc.get(vk, nsegs):
                        continue
                    imp = f"{sess[i]}:{chans[c]}:{b}:{k}"
                    ad = f"cr-{(zlib.crc32(imp.encode()) % 240):03d}"
                    m = msg(s=sess[i], ch=chans[c], i=imp, ad=ad, g=g, n=nsegs, d=ad_ms, sm=seg_ms, a0=ac[vk])
                    now = time.time_ns()
                    for q in due_quartiles(g, nsegs, ad_ms, seg_ms):
                        expected[(imp, q)] = now
                        st["due"] += 1
                        run["imps"] += q == 0
                    batches[spart[i]].append(SendMessage(m))
                    if a.dup_rate and rng.random() < a.dup_rate:
                        batches[spart[i]].append(SendMessage(m))   # delivered twice
                        run["dups"] += 1
                    st["segments"] += 1
            st["breaking"] = breaking
            # One batch per partition, all sent at once: a session's segments stay in order, one round trip a tick.
            if batches:
                await asyncio.gather(*(writer.send_messages(STREAM, SEGMENTS, Partitioning.partition_id(p), ms)
                                       for p, ms in batches.items()))
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))
        sending[0] = False

    async def audit():
        who = Consumer.Single("audit")
        nxt = {}
        since = PollingStrategy.Timestamp(start_ns // 1000)
        while True:
            more = False
            for p in range(P):
                ms = await auditor.poll_messages(STREAM, FIRED, consumer=who, partition_id=p, count=a.batch,
                                                 auto_commit=False,
                                                 polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        key = (e["i"], e["q"])
                        if key in got:
                            run["twice"] += 1        # the same beacon fired again
                            continue
                        got.add(key)
                        if key not in expected:
                            run["not_due"] += 1      # a quartile the viewer never reached
                        st["recorded"] += 1
                        lat.add((now - e["sg"]) / 1e6)
                if len(ms) == a.batch:
                    more = True
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = f"{st['breaking']} of {a.channels} channels in a break" if sending[0] else "drained, auditing"
            print(f"[stitcher] {st['segments']:>6,} segments/s ({phase})  beacons due {st['due']:,}  "
                  f"recorded {st['recorded']:,}  segment-to-beacon-record p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"fired twice {run['twice']}  not due {run['not_due']}  {stalls.read()}", flush=True)
            st.clear()

    side = [asyncio.ensure_future(c) for c in (audit(), report(), stalls.watch())]
    print(f"[stitcher] {a.viewers:,} viewers on {a.channels} channels; a break of {a.slots} x {a.ad_s:g} s ads "
          f"({nsegs} segments of {a.seg_s:g} s) every {a.cycle_s:g} s per channel", flush=True)
    await serve()
    await asyncio.sleep(a.drain_s)   # let every tracker catch up before the final audit
    missing = sum(1 for k in expected if k not in got)
    print(f"[audit] beacons due {len(expected):,} (impressions {run['imps']:,})  recorded {len(got):,}  "
          f"missing {missing:,}  fired twice {run['twice']:,}  not due {run['not_due']:,}  "
          f"segment events sent twice {run['dups']:,}  ads left partway {run['left']:,}", flush=True)
    for t in side:
        t.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--viewers", type=int, default=100_000, help="concurrent viewers across the channels")
    p.add_argument("--channels", type=int, default=50, help="FAST channels")
    p.add_argument("--cycle-s", type=float, default=900, help="seconds between a channel's ad breaks")
    p.add_argument("--slots", type=int, default=4, help="ads in a break")
    p.add_argument("--ad-s", type=float, default=30, help="ad length in seconds")
    p.add_argument("--seg-s", type=float, default=2, help="segment length in seconds")
    p.add_argument("--ramp-ms", type=float, default=2000, help="viewers on a channel reach the break within this")
    p.add_argument("--leave", type=float, default=0.08, help="share of ads a viewer leaves partway through")
    p.add_argument("--dup-rate", type=float, default=0.005, help="share of segment events sent twice")
    p.add_argument("--duration", type=float, default=1800, help="seconds to serve")
    p.add_argument("--drain-s", type=float, default=8, help="seconds to keep auditing after the last segment")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the stitchers send their segment events")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between audit reads when idle")
    p.add_argument("--batch", type=int, default=2000)
    asyncio.run(main(p.parse_args()))
