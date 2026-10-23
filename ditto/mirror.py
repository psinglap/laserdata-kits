"""A downstream mirror of the collection, fed by the lane: what an ops dashboard, a catering system or a
warehouse loader keeps from Ditto CDC.

Each process is one downstream system; run several. Every mirror reads every partition of cabin-orders directly
(its own offsets, no consumer group, nothing committed) on its own connection, so each one moves at its own pace
and none holds another back. On start it rebuilds the mirror from the log before it times anything, so a system
added later starts from the lane instead of a full requery.

It applies events the way Ditto's CDC docs say to:
  * per document, by txnId: an event whose txnId is not above the last one applied for that _id is a repeat
    (Ditto: "no two events for a given document will share the same txnId") and is dropped;
  * upsert and snapshot write the document; remove and evict delete it;
  * requeryRequired is counted and printed with its txnId (in production: re-query the bridge's dataset over
    Ditto's HTTP API with X-DITTO-TXN-ID set to it).
It also checks every change chain: an upsert's oldValue (or a remove's value) must be the version the mirror
holds. A break means a change was lost, applied twice or applied out of order.

Prints change-to-mirror latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import CHANGES, DEFAULT_URL, PARTITIONS, STREAM, Window, connect, doc_key, ensure_topics, load


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), CHANGES)
    reader = await connect(a.url)
    P = PARTITIONS[CHANGES]
    start_ns = time.time_ns()

    last = {}                               # _id -> last txnId applied
    mirror = {}                             # _id -> _version held, absent once removed
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    requery_seen = set()
    caught = [None]

    def apply(e):
        if e.get("type") == "requeryRequired":
            if e["txnId"] not in requery_seen:
                requery_seen.add(e["txnId"])
                st["requery"] += 1
                print(f"[{a.name}] requeryRequired at txnId {e['txnId']:,}: re-query the bridge's dataset "
                      f"(X-DITTO-TXN-ID: {e['txnId']})", flush=True)
            return
        c = e["change"]
        m = c["method"]
        doc = c.get("newValue") if m == "upsert" else c.get("value")
        if doc is None:
            return                          # an evict whose tombstone was already gone
        k = doc_key(doc["_id"])
        if e["txnId"] <= last.get(k, 0):
            st["dup"] += 1                  # a repeat: the newest state is already applied
            return
        last[k] = e["txnId"]
        held = mirror.get(k)
        if m == "upsert":
            base = c.get("oldValue")
            if (base["_version"] if base else None) != held:
                st["breaks"] += 1           # the change was made on a version this mirror never had
            mirror[k] = doc["_version"]
        elif m == "snapshot":
            mirror[k] = doc["_version"]     # full state: upsert by _id, no chain to check
            st["snapshots"] += 1
        else:                               # remove or evict
            if m == "remove" and doc["_version"] != held:
                st["breaks"] += 1
            mirror.pop(k, None)
            st["removes"] += 1
        st["applied"] += 1

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        nxt, done = {}, set()
        last_pass = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last_pass))   # longest pass over all partitions
            last_pass, more = now_m, False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, CHANGES, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        live = (caught[0] is not None and e["ns"] >= start_ns and e.get("type") == "documentChanged"
                                and e["change"]["method"] != "snapshot")
                        if live:
                            lat.add((now - e["ns"]) / 1e6)     # a live change, not the rebuild
                        else:
                            run["replayed"] += 1
                        apply(e)
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        gc.freeze()         # keep the rebuilt mirror out of the collector's way
                        print(f"[{a.name}] rebuilt the mirror from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(mirror):,} documents from {run['replayed']:,} events, "
                              f"change chains broken {st['breaks']:,}", flush=True)
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the mirror from the log: {run['replayed']:,} events so far", flush=True)
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            run["breaks"] += st["breaks"]
            print(f"[{a.name}] {r['n']:>6,} changes/s  change-to-mirror p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  repeats dropped {st['dup']:,}  "
                  f"removes {st['removes']:,}  chains broken {st['breaks']:,} (total {run['breaks']:,})  "
                  f"requery {st['requery']:,}  mirror {len(mirror):,} docs  read pass max {st['gap_ms']:.0f}ms",
                  flush=True)
            st.clear()

    await asyncio.gather(read_lane(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="ops-dashboard")
    p.add_argument("--budget-ms", type=float, default=100, help="change-to-mirror p99 budget")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
