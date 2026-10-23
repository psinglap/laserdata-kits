"""A serving node: reads every published version from the lane and serves each asset from its newest version.

This is the part of the service that serves an article on nytimes.com which has to hold the latest version
within the one-second SLA. It reads every partition with its own connection and its own offsets (no shared
consumer group), so adding nodes before a big night rebalances nothing, and it keeps the newest version of
every asset it has seen in memory. A repeat or an older version never replaces a newer one.

On start it rebuilds from the log (the news day kept on the lane), then times live versions. Prints a line a
second:
  * publish-to-node latency for editor versions (publish, update, correction, live post: created on the
    lane -> servable here), PASS or FAIL against --budget-ms, and how many landed inside the one-second SLA;
  * page views served the previous version: views that arrive between a version's creation and the moment
    this node holds it get the older one. Estimated from --page-views spread over the live assets by
    readership, times how late each version was;
  * republishes (versions written because a referenced tag, image or byline changed), timed but not judged;
  * gaps: a missing version of an asset (must stay 0).
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, LANE, PARTITIONS, REPUBLISH, SLA_MS, STREAM, Window, connect, live_uris, readership, \
    unpack


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for newsroom.py to create the publish-to-site lane…", flush=True)
        await asyncio.sleep(1)

    live = live_uris(a.assets)
    w = [readership(i) for i in range(len(live))]
    views = {u: a.page_views * w[i] / sum(w) for i, u in enumerate(live)}   # page views a second per live asset
    served = {}                                             # uri -> (version, why, headline, body): what a request gets
    gc.freeze()                                             # keep startup tables out of the collector's way

    def serve(uri: str) -> tuple:
        # What a request for this asset gets from this node right now: the newest version it holds.
        v, why, headline, body = served.get(uri, (0, None, None, ""))
        return v, headline, len(body)

    lat, rep = Window(), Window()
    st = dict(live=0, sla=0, behind=0.0, gaps=0, replayed=0, rebuilt=0, max_age=0.0, republished=0)
    started, head, caught = time.monotonic(), set(), [None, False]   # partitions at the log head; [rebuilt at, timing live]

    def apply(ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            uri, ver = v["u"], v["v"]
            last = served.get(uri)
            if last is not None:
                if ver <= last[0]:
                    continue                                # a repeat or an older version: never serve it over a newer one
                if ver > last[0] + 1:
                    st["gaps"] += 1                         # a version of this asset never arrived
            served[uri] = (ver, v["w"], v["h"], v["b"])     # newest wins
            if ver == 1:
                continue                                    # the state before the run is not a live publish
            age = now - v["ns"]
            if not caught[1]:
                st["replayed"] += 1
                st["rebuilt"] += 1
                st["max_age"] = max(st["max_age"], age / 1e6)
            elif v["w"] == REPUBLISH:
                rep.add(age / 1e6)
                st["republished"] += 1
            else:
                lat.add(age / 1e6)
                st["live"] += 1
                st["sla"] += age < SLA_MS * 1e6
                st["behind"] += views.get(uri, 0.0) * age / 1e9   # page views that got the previous version meanwhile

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                                # more is waiting (the rebuild or a burst): read on at once
            if pid not in head:
                head.add(pid)                               # this partition's replay reached the head of the log
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    n_live = sum(1 for u in live if u in served)
                    print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: latest version of {n_live} of "
                          f"{len(live)} live assets and {len(served) - n_live:,} other assets, {st['rebuilt']:,} versions "
                          f"replayed; without the log this node would start empty and fetch every asset one by one",
                          flush=True)
            await asyncio.sleep(a.poll_ms / 1000)           # writers write once per few ms; wait for the next write

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                # Live once the log is replayed and a whole second of versions arrived inside the budget;
                # partitions interleave, so one fresh version alone doesn't mean the replay is finished.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} versions replayed this second, "
                      f"{len(head)} of {PARTITIONS} partitions at the head of the log", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                rep.flush()
                continue
            r, n, rr = lat.flush(), st["live"], rep.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            pct = 100 * st["behind"] / a.page_views if a.page_views else 0.0
            top_v, _, _ = serve(live[0])                    # what a request for the most-read live asset gets now
            reps = (f"republishes {st['republished']:,}/s p99 {rr['p99']:.1f}ms" if st["republished"]
                    else "republishes 0/s")
            print(f"[{a.name}] {n:>4,} editor versions/s  publish-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  inside the 1 s SLA {st['sla']:,} of {n:,}  "
                  f"page views served the previous version {st['behind']:,.1f} of {a.page_views:,}/s ({pct:.4f}%)  "
                  f"{reps}  gaps {st['gaps']}  top asset v{top_v}", flush=True)
            st.update(live=0, sla=0, behind=0.0, republished=0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="serving-1")
    p.add_argument("--assets", type=int, default=400, help="same as newsroom.py")
    p.add_argument("--page-views", type=int, default=20000, help="page views a second across the live assets (illustrative)")
    p.add_argument("--budget-ms", type=float, default=50, help="p99 budget for publish-to-node: 5%% of the one-second SLA")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
