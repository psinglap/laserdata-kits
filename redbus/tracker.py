"""The boarding-point tracker, fed by the GPS lane: ENTRY, HALT and EXIT at every boarding point, and the
"your bus is arriving" alert to every passenger boarding there.

Reads gps-packets directly, partition by partition, on --readers connections of its own. Each journey's
assignment and packets share one key (SVC_DOJ), so they arrive in order on one partition: no lookup, no lock.
On every packet it runs the boarding-point rules (common.bp_step, the stateless distance-to-BP detection) and
publishes each ENTRY, HALT or EXIT to bp-events, carrying the packet's sequence; on ENTRY it also publishes one
alert per passenger boarding there to passenger-alerts, for the notification dispatcher. Both are written from a
separate task in small batches every few ms, never awaited inside the read loop. Downstream readers of
bp-events (booking-window extension, the delay feed, the live-tracking page) would subscribe to the same topic.

Prints packet-to-tracker latency every second (from the packet being created to this service having applied it)
with PASS or FAIL against --budget-ms. On start it rebuilds every journey by replaying the retained lane, so it
holds no state of its own; replayed packets are not counted as live latency, and events from packets older than
--resend-s are not sent again.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (ALERTS, BP_EVENTS, DEFAULT_URL, GPS, PARTITIONS, STREAM, Journey, Window, bp_step, ensure_topics,
                    load, msg, part, read_partitions)


async def main(a):
    async def connected():
        # One connection per reader and writer, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await connected(), GPS, BP_EVENTS, ALERTS)
    started_ns = time.time_ns()
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    ev_p = await (await connected()).producer(STREAM, BP_EVENTS, topic_partitions_count=PARTITIONS[BP_EVENTS], mode=cfg)
    al_p = await (await connected()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS], mode=cfg)

    journeys = {}                              # SVC_DOJ -> Journey
    ev_buf, al_buf = [], []
    lat, stats = Window(), defaultdict(int)
    to_broker, from_broker = Window(), Window()     # where the time goes (--why)
    live = [False]                             # True after a whole second of packets inside the budget
    gc.freeze()

    def on_packet(m):
        p = load(m.payload())
        k = p["k"]
        if p["t"] == "assign":
            journeys[k] = Journey(p["bps"])
            return
        if p["t"] == "end":
            journeys.pop(k, None)
            return
        j = journeys.get(k)
        if j is None or p["seq"] <= j.seq:
            stats["skipped"] += 1              # not validated for tracking, or an older or repeated packet
            return
        j.seq = p["seq"]
        fresh = p["ns"] >= started_ns - a.resend_s * 1e9     # a rebuild does not re-send old events
        for ev, bp in bp_step(j, p["lat"], p["lon"]):
            stats[ev] += 1
            if not fresh:
                continue
            ev_buf.append((k, SendMessage(msg(k=k, bp=bp, name=j.bps[bp][0], ev=ev, seq=p["seq"], ns=p["ns"]))))
            if ev == "ENTRY":
                for s in range(j.bps[bp][3]):  # one alert per passenger boarding here
                    al_buf.append((k, SendMessage(msg(k=k, bp=bp, pax=s, kind="arriving", name=j.bps[bp][0],
                                                      seq=p["seq"], ns=p["ns"]))))
                stats["alerts"] += j.bps[bp][3]
        now = time.time_ns()
        age = (now - p["ns"]) / 1e6
        if not live[0]:                        # still replaying: track the oldest packet seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        elif p["ns"] >= started_ns:            # replayed history is a rebuild, not live latency
            lat.add(age)
            if a.why:
                to_broker.add(m.timestamp() / 1e3 - p["ns"] / 1e6)
                from_broker.add(now / 1e6 - m.timestamp() / 1e3)

    async def flush(producer, buf, topic):
        # Every few ms, one batch per partition, sent at once, outside the read loop.
        async with producer:
            while True:
                if buf:
                    items = buf[:]
                    buf.clear()
                    batches = defaultdict(list)
                    for key, sm in items:
                        batches[part(key, PARTITIONS[topic])].append(sm)
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                                           for pid, ms in batches.items()))
                await asyncio.sleep(0.002)

    async def report():
        while True:
            await asyncio.sleep(1)
            gc.freeze()                        # journeys live for hours: keep them out of later GC passes
            r = lat.flush()
            if not live[0]:
                # live once a whole second of packets arrived inside the budget; partitions interleave,
                # so one fresh packet alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    live[0] = True
                print(f"[tracker] catching up: replayed {stats['replayed']:,} packets this second, "
                      f"journeys {len(journeys):,}", flush=True)
            else:
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[tracker] {r['n']:>6,} packets/s  packet-to-tracker p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                      f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  journeys {len(journeys):,}  "
                      f"ENTRY {stats['ENTRY']:,} HALT {stats['HALT']:,} EXIT {stats['EXIT']:,}  "
                      f"passenger alerts {stats['alerts']:,}", flush=True)
                if a.why:
                    tb, fb = to_broker.flush(), from_broker.flush()
                    print(f"[why]   packet->broker p50 {tb['p50']:.1f} p99 {tb['p99']:.1f}  "
                          f"broker->tracker p50 {fb['p50']:.1f} p99 {fb['p99']:.1f} ms", flush=True)
            stats.clear()

    pids = list(range(PARTITIONS[GPS]))
    readers = [read_partitions(await connected(), GPS, pids[r::a.readers], on_packet, f"tracker-{r}")
               for r in range(a.readers)]
    await asyncio.gather(*readers, flush(ev_p, ev_buf, BP_EVENTS), flush(al_p, al_buf, ALERTS), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--budget-ms", type=int, default=50, help="packet-to-tracker p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the lane's 16 partitions")
    p.add_argument("--resend-s", type=float, default=30, help="on a rebuild, events older than this are not sent again")
    p.add_argument("--why", action="store_true", help="also print where the time goes: packet to broker, broker to tracker")
    asyncio.run(main(p.parse_args()))
