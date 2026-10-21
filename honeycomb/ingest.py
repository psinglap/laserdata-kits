"""The source of truth: customer datasets sending event batches, the ingest service enqueuing them on the
main Kafka topic, and Retriever nodes applying each Kafka partition, as the freshness lane sees them.

Every dataset sends a batch every --every seconds. When the ingest service has enqueued a batch, it writes a
receipt to the lane, keyed by dataset: the dataset's receipt sequence, the Kafka partition and offset the
batch went to, the event count and when it was enqueued. Every --progress-every seconds each Retriever node
writes, per Kafka partition, the offset it has applied (that data is now queryable). Kafka itself is not
in this kit; the offsets stand in for it.

Two things happen on purpose, so the heartbeat check has something to get right:
  * customers go quiet: --silence-rate datasets a second stop sending for --silence-for seconds. A heartbeat
    trigger should fire for each of them, once --heartbeat-s has passed.
  * the pipeline falls behind: from --stall-at, Retriever stops applying --stall-share of the Kafka
    partitions for --stall-for seconds (a maintenance cutover, an offset reset, a lost partition leader),
    while ingest keeps accepting. A heartbeat that only queries recent data sees silence for every dataset
    whose partitions are all behind. Those datasets are still sending; nothing should page their owners.

ingest.py also reads every verdict the freshness service sends (heartbeat-verdicts) and checks it against
the truth: was a fired heartbeat a real silence, how long after the window did it fire, was any silence
missed, and was every held heartbeat a dataset that was still sending while Retriever was behind?
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, KAFKA_PARTITIONS, PARTITIONS, PROGRESS, RECEIPTS, STREAM, VERDICTS, Stalls,
                    Window, dataset, ensure_topics, load, msg, part)

TICK = 0.02   # seconds; each dataset still sends on its own schedule, stamped when enqueued


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), RECEIPTS, PROGRESS, VERDICTS)
    cfg = DirectProducerConfig(batch_length=5000, linger_time=timedelta(microseconds=500))
    rx_out = await (await own_client()).producer(STREAM, RECEIPTS, topic_partitions_count=PARTITIONS[RECEIPTS],
                                                 mode=cfg)
    pg_out = await (await own_client()).producer(STREAM, PROGRESS, topic_partitions_count=PARTITIONS[PROGRESS],
                                                 mode=cfg)
    verdicts = await (await own_client()).consumer_group(
        "ingest-truth-check", STREAM, VERDICTS, polling_strategy=PollingStrategy.Next(), batch_length=5000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),     # offsets stored once a second, not per poll
        poll_interval=timedelta(milliseconds=5))                    # a check, not on the lane's clock

    r = random.Random(7)
    kp_n = a.kafka_partitions
    names = [dataset(i) for i in range(a.datasets)]
    index = {n: i for i, n in enumerate(names)}
    # A team's datasets share the team's Kafka partitions (two here), as the ingest service assigns them.
    kps = []
    for i in range(a.datasets):
        t = random.Random(i // 12)
        first = t.randrange(kp_n)
        kps.append((first, (first + 1 + t.randrange(kp_n - 1)) % kp_n))
    seq = [0] * a.datasets
    sent = [deque(maxlen=int(a.heartbeat_s / a.every) + 8) for _ in range(a.datasets)]   # enqueue times, ns
    quiet_until = [0.0] * a.datasets
    kp_off = [0] * kp_n                     # newest offset written per Kafka partition
    applied = [0] * kp_n                    # what Retriever has applied per Kafka partition
    stalled, stall = set(), [0, 0]          # stalled Kafka partitions, and the stall's [start, end] in ns
    stalled_set = set()                     # the last stall's partitions, kept for the check after it ends
    spells = {}                             # dataset -> [last batch before going quiet (ns), fired]
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]       # each dataset sends in its own slot, staggered
    for i in range(a.datasets):
        ring[i % slots].append(i)
    w_ns = int(a.heartbeat_s * 1e9)
    stats, delays, stalls = defaultdict(int), Window(), Stalls()
    gc.freeze()                             # keep the datasets out of the collector's way

    async def send_by_partition(producer, topic, items):
        # One batch per partition, all sent at once: per-key order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, PARTITIONS[topic])].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def run():
        async with rx_out, pg_out:
            t0 = due = next_progress = next_second = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                el = now - t0
                if a.stall_for and a.stall_at <= el < a.stall_at + a.stall_for and not stalled:
                    stalled.update(r.sample(range(kp_n), round(kp_n * a.stall_share)))
                    stalled_set.update(stalled)
                    stall[0], stall[1] = now_ns, now_ns + int(a.stall_for * 1e9)
                elif stalled and el >= a.stall_at + a.stall_for:
                    stalled.clear()                          # Retriever applies again and catches up
                    stall[1] = now_ns
                if now >= next_second:
                    next_second += 1.0
                    for _ in range(a.silence_rate):          # customers who stop sending for a while
                        i = r.randrange(a.datasets)
                        if quiet_until[i] + 2 * a.every < now and sent[i]:   # sending again for a while
                            quiet_until[i] = now + a.silence_for
                            spells[i] = [sent[i][-1], False]
                            stats["silences"] += 1
                items = []
                for i in ring[tick % slots]:
                    if quiet_until[i] > now:
                        continue
                    seq[i] += 1
                    kp = kps[i][seq[i] % 2]
                    kp_off[kp] += 1
                    sent[i].append(now_ns)
                    items.append((names[i], SendMessage(msg(ds=names[i], seq=seq[i], kp=kp, off=kp_off[kp],
                                                            n=r.randint(20, 400), ns=now_ns))))
                stats["receipts"] += len(items)
                sends = [send_by_partition(rx_out, RECEIPTS, items)]
                if now >= next_progress:
                    next_progress += a.progress_every
                    prog = []
                    for kp in range(kp_n):
                        if kp not in stalled:
                            applied[kp] = kp_off[kp]
                        for node in range(a.nodes):
                            prog.append((str(kp), SendMessage(msg(kp=kp, node=node, off=applied[kp], ns=now_ns))))
                    sends.append(send_by_partition(pg_out, PROGRESS, prog))
                await asyncio.gather(*sends)
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    def stalled_at(i, t_ns):
        # Were all of this dataset's Kafka partitions behind on Retriever at t (2 s grace after the stall)?
        return stall[0] and stall[0] <= t_ns <= stall[1] + 2_000_000_000 and all(k in stalled_set for k in kps[i])

    async def on_verdict(m):
        d = load(m.payload())
        i = index.get(d["ds"])
        if i is None or d["v"] == "ok":
            stats["resumed"] += d.get("v") == "ok"
            return
        t_v = d["ns"]
        prior = [s for s in sent[i] if s <= t_v]
        if not prior:
            return                          # older than the check's memory: not judged
        gap = t_v - prior[-1]
        if d["v"] == "fire":
            if gap >= w_ns:
                stats["fired_right"] += 1
                delays.add((gap - w_ns) / 1e9)
                if i in spells:
                    spells[i][1] = True
            else:
                stats["spurious"] += 1      # paged an owner whose dataset had sent inside the window
        elif d["v"] == "hold":
            if gap < w_ns and stalled_at(i, t_v):
                stats["held_right"] += 1    # a page a query-only heartbeat would have sent, and nobody got
            else:
                stats["held_wrong"] += 1
                behind = [k for k in kps[i] if k in stalled_set]
                print(f"[ingest] wrong hold: {d['ds']} last batch {gap / 1e9:.2f}s before the verdict, "
                      f"Kafka partitions {kps[i]} of which behind {behind}", flush=True)

    async def report_line():
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            now_ns = time.time_ns()
            for i, (last, fired) in list(spells.items()):
                if fired:
                    del spells[i]
                elif now_ns - last > w_ns + int(a.grace_s * 1e9):
                    stats["missed"] += 1    # a real silence, and no heartbeat fired in time
                    del spells[i]
            d = delays.flush()
            quiet = sum(1 for q in quiet_until if q > now)
            print(f"[ingest] {stats['receipts']:>7,} receipts/s  datasets {a.datasets:,} (quiet {quiet:,})  "
                  f"Kafka partitions behind on Retriever {len(stalled)}/{kp_n}  heartbeats: silences started "
                  f"{stats['silences']:,}  fired right {stats['fired_right']:,} (after the window +{d['p50']:.2f}s, "
                  f"max +{d['max']:.2f}s)  missed {stats['missed']:,}  spurious pages {stats['spurious']:,}  "
                  f"held while Retriever behind {stats['held_right']:,} (wrong {stats['held_wrong']:,})  "
                  f"{stalls.read()}", flush=True)
            for k in ("receipts", "silences", "fired_right", "missed", "spurious", "held_right", "held_wrong",
                      "resumed"):
                stats[k] = 0

    await asyncio.gather(run(), verdicts.consume_messages(on_verdict, asyncio.Event()), report_line(),
                         stalls.watch())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--datasets", type=int, default=40000, help="datasets sending events")
    ap.add_argument("--every", type=float, default=1.0, help="seconds between batches per dataset")
    ap.add_argument("--kafka-partitions", type=int, default=KAFKA_PARTITIONS, help="partitions on the events topic")
    ap.add_argument("--nodes", type=int, default=2, help="Retriever nodes per Kafka partition")
    ap.add_argument("--progress-every", type=float, default=0.5, help="seconds between Retriever progress reports")
    ap.add_argument("--heartbeat-s", type=float, default=60, help="heartbeat window: no data for this long pages")
    ap.add_argument("--grace-s", type=float, default=3, help="a silence not fired this long after the window is missed")
    ap.add_argument("--silence-rate", type=int, default=5, help="datasets a second that stop sending")
    ap.add_argument("--silence-for", type=float, default=120, help="seconds a quiet dataset stays quiet")
    ap.add_argument("--stall-at", type=float, default=90, help="seconds after start Retriever stops applying")
    ap.add_argument("--stall-for", type=float, default=120, help="seconds Retriever stays behind (0 = never)")
    ap.add_argument("--stall-share", type=float, default=0.3, help="share of Kafka partitions behind")
    asyncio.run(main(ap.parse_args()))
