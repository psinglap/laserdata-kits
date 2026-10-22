# Failed-writes capture for Klaviyo's event pipeline, on LaserData

Every event whose MSK publish fails lands on Apache Iggy in milliseconds and is re-published inside
its SLO, even when half of all publishes fail. Only the failed-writes cluster changes, so MSK stays
as it is.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/UVZmVX356ytw4ys2XBYoAh
- **One slide:** [`LaserData_Klaviyo_FailedWrites.pdf`](LaserData_Klaviyo_FailedWrites.pdf)

## Why this use case

Klaviyo Engineering's three-part series on the new event pipeline describes it in its own words.
Part 1 (Aug 2025): "170,000 events per second at peak", processed "from real-time notifications
(5-second SLO) to analytics aggregates (20-minute SLO)", on "managed Kafka infrastructure on AWS MSK",
with one topic per priority lane (P10, P20, P30, Part 2). Part 3 (Oct 2025) is about the Event
Publisher: when a publish fails ("Kafka timeouts during spiky traffic, which can exhaust the client's
built-in retries"), "the event is written to a failed-writes topic on a self-hosted Kafka cluster and
is retried automatically", then to "a failed-writes-DLQ topic, where it's retained for 7 days". Both
topics live on "a separate self-hosted Kafka cluster from the main pipeline, so an outage in one does
not impact the other". About 20,000 events were re-published automatically in three months.

The same post names the hard case: "when the failed-writes topic must absorb a large share of traffic —
for example, half of all events for 10–20 minutes. Kafka can handle these bursts, but the bottleneck
shifts to the failed-writes processor that re-publishes failed events."

So this kit puts that separate cluster on Apache Iggy. The publisher writes every failed event to the
`failed-writes` topic, keyed by profile, with its priority lane, the time its first publish failed and a
per-partition sequence. Retriers read the partitions directly and re-publish; an event that fails again
goes back with its attempt count after a backoff, and after five attempts to `failed-writes-dlq` (7 days).
MSK, the producer proxy, the consumer proxy and the processors stay as they are. The capture write sits
on the request path of fire-and-forget APIs, and a P10 event that failed once still has its 5-second SLO
to make, so the kit times both.

## Run it

Needs Docker and Python 3.10+. Defaults: one publisher at 20,000 events a second (run several with
`--name pub-1 …` for Klaviyo's 170K/s peak), 0.1% of publishes failing in normal times, 16 partitions,
P10/P20/P30 lanes in a 20/30/50 mix, 2% of re-publishes failing again, five attempts before the DLQ, and
40 MB/s of recovered S3 events replayed next door.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0
python retrier.py --shard 0 --shards 2 &               # the failed-writes processor, two shards
python retrier.py --shard 1 --shards 2 &
python publisher.py --outage-at 60 --outage-for 900 &  # half of all publishes fail for 15 minutes, a minute in
python s3_replay.py                                    # bulk S3 replay on the same broker
```

Each retrier prints a line a second (one line in your terminal, wrapped here):

```
[retrier-1] 1,569 failed writes/s picked up  capture-to-retry p50 6.5ms  p99 11.7ms  p99.9 12.1ms  PASS (<100ms)
            re-published 1,513  on time P10 100.00% P20 100.00% P30 100.00%  retried 52  oldest 12.1ms
            lost 0  repeats dropped 0  total re-published 23,513 to DLQ 43
```

The business numbers: `on time`, the share of re-published events inside their lane's SLO counted from
the moment the first publish failed (P10 5 s; P20 60 s and P30 20 min are this kit's assumptions);
`lost`, events missing from failed-writes (a gap in a writer's per-partition sequence; it must stay 0);
`to DLQ`, events that kept failing; `oldest`, the oldest event picked up that second, which grows if the
retriers fall behind a surge. The publisher prints the capture write's ack time, which is what a
fire-and-forget API caller waits for when a publish fails.

**What the numbers mean.** The kit times each captured event from the capture write in `publisher.py`
to the moment a retrier has it, through Python clients, on whatever machine you run it on; that includes
the retrier's 5 ms idle poll. On a shared 2-vCPU GitHub runner, with the broker and four Python processes
on one box, 1% of 6,000 events a second failing and then 50% for 30 s (3,000 failed writes a second), and
4 MB/s of S3 replay next door, three CI runs held capture-to-retry p50 5.2 to 5.8 ms, p99 7.8 to 8.8 ms and
p99.9 7.8 to 8.8 ms (medians per second) over 64 to 65 steady-state seconds per retrier, every one inside the
100 ms budget (worst second 12 to 14 ms); in the surge seconds the median p99 was 11.0 to 12.0 ms (7.2 to
7.8 ms otherwise). Earlier runs had rare seconds of up to 350 ms on both retriers at once, 35 and 67 s into the
replay: the runner's kernel writing about 30 s of the replay's dirty pages to disk in one burst. CI now has
the runner flush dirty pages continuously (`vm.dirty_background_bytes` 32 MB, 2 s expiry), as a streaming host
is tuned; on a Linux box of your own, set the same before the replay test. The capture write's ack p99 had a
median of 1.4 to 2.2 ms per second (worst second 6.0 to 11.6 ms). Each run captured 92,759 failed writes and
lost none: 92,558, 92,597 and 92,576 were re-published, 201, 162 and 183 went to the DLQ (exactly the events with payloads MSK rejects), and every P10
event was re-published inside 5 seconds of its first failure. That is the end-to-end lane through this
demo code at a reduced scale, not the broker alone. The broker's own tail latency, the surge at full size
and the cost to keep the lane ready are measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `publisher.py` | The Event Publisher's capture path: MSK publishes simulated, a share failing (blips, then a surge), every failed event written to `failed-writes` keyed by profile, with lane, first-failure time and a per-partition sequence. Some carry a payload MSK will never accept. |
| `retrier.py` | The failed-writes processor, sharded by partition. Reads partitions directly, storing offsets on the broker once a second, re-publishes (`republish()` is the stand-in for your MSK produce), backs off and retries, sends to `failed-writes-dlq` after five attempts; drops repeats, counts sequence gaps as lost. |
| `s3_replay.py` | Recovered events from S3 replayed in large batches to their own topic, for the noisy-neighbour test. |
| `kafka_bridge.py` | Copies your failed-writes topic, or a share of your lane topics, into `failed-writes`, read-only, with confluent-kafka. |
| `common.py` | Names, lanes and SLOs, event names, partitioning, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, surge sizing and cost to keep the lane ready included; rules fixed before results. |

## Three tests

1. **The 15-minute surge at full size:** nine publishers (`--name pub-0` … `pub-8`, each `--rate 20000
   --outage-at 60 --outage-for 900`), about 85K failed writes a second, and as many retrier shards as it
   takes. Pass: capture-to-retry p99 under 100 ms, `oldest` stays flat, P10 `on time` stays at 100%, `lost 0`.
   This is the run that sizes the retrier fleet.
2. **Retries that keep failing:** `--retry-fail 0.3` on the retriers (MSK still struggling while they
   re-publish). Pass: P10 still on time through the backoff, `lost 0`; the DLQ holds the `--reject-share`
   events plus the few that fail five times in a row (0.3^5, about 0.2%).
3. **Restart a retrier:** stop one mid-surge and start it again. Each partition's offset is stored on the
   broker once a second (`--commit-s`; an offset write on every poll caused tail-latency spikes), so it
   resumes at most a second behind its last poll and re-publishes those events again (at-least-once).
   Pass: back under budget within seconds, `lost 0`. Note: the kit stores the offset with a poll, so a
   crash between that poll and the re-publish can skip that batch; a production retrier stores its offset
   only after MSK acknowledges (the SDK's consumer reader has `store_offset` for that).

## Shadow your own traffic

```
pip install confluent-kafka
# every real failure, from the failed-writes topic on your self-hosted cluster
python kafka_bridge.py --bootstrap <broker:9092> --kafka-topics failed-writes
# or rehearse the surge: half of your live lane traffic, read-only from MSK
python kafka_bridge.py --bootstrap <msk:9098> --kafka-topics <P10 topic> <P20 topic> <P30 topic> \
  --lane <P10 topic>=P10 --lane <P20 topic>=P20 --lane <P30 topic>=P30 --share 0.5 \
  -X security.protocol=SASL_SSL -X sasl.mechanism=AWS_MSK_IAM
python retrier.py                                   # re-publishes nothing: republish() is a stand-in
```

The bridge joins as its own consumer group, `iggy-shadow`, and only reads: the publishing clients, the
proxies, the processors and your current retrier are untouched. Map your record shape in `to_event()`:
the kit needs the profile, the lane, the metric name and the event id.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads,
consumer offsets stored on the broker, message expiry per topic (1 day on failed-writes, 7 days on the
DLQ here), at-least-once delivery (dedupe by event id, as the kit does with its sequence), and an official
Python SDK, which fits your Python services. Not yet: a Kafka API gateway (in progress), so the publishing
client and retrier would write and read with the Iggy SDK, and the trial uses `kafka_bridge.py`; and
published numbers for a cluster that sits idle and then takes half of a 170K/s pipeline at once, which
the evaluation measures.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored failed writes and a rehearsed surge
beside MSK, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
