# Benchmark plan: the Mux Data streaming export lane, one topic per streaming export

**Question:** with one topic per streaming export, does every finished view of every healthy export
reach its export worker within 50 ms at p99 while 1% of customer destinations are unreachable for an
hour and others throttle at their Kinesis shard limits, with raw player beacons running next door;
does each stalled export backfill from its last exported view with nothing lost and in order per
view; how many export topics can one cluster carry before that stops being true; and what does the
lane cost per billion exported views, on Apache Iggy and on the same lane in Kafka with Kafka
Connect sinks?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| View record size | 1.2 KB | A VideoView record (mux-protobuf `video_view`) with its playback, player and QoE fields; measured on your records in week 1. |
| Exports | 500 → 2,000 → 5,000 topics, long-tail environment sizes, the largest 5% with 4 partitions | One topic per streaming export (one environment, one Kinesis stream or Pub/Sub topic); Kafka gets the same layout. |
| Finished-view rate | 2K → 10K → 30K views/s, plus 3% updated views | "Millions of video views each day" (Mux, 2019) and live events of 8 to 30 million concurrent viewers (Mux, 2023); the top step is a large live event ending at once. |
| Readers | Export worker shards with direct partition reads, a write limit per destination | Each export keeps its own offset; no consumer-group rebalances, no tasks to restart. |
| Destination limits | Kinesis 1,000 records/s per shard; Pub/Sub at a set limit | AWS's per-shard write quota; throttled exports must slow only themselves. |
| Spurious write failures | 1% of destination writes fail once | "Spurious write-failures that immediately succeed upon retry" (Mux, Kafka Connect post). |
| Destination outages | 1% of exports unreachable for 60 min, then backfill at their limit | A changed IAM role or Pub/Sub permission on the customer's side. |
| Retention | 24 h message expiry per export topic | How long views wait for a destination that is down; agreed with you in week 1. |
| Beacons next door | 250 MB/s of raw player beacons on the same nodes | The collector-to-processor traffic of a large live event, scaled to the lane's cluster. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A view a customer pays to export cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`; Kafka Connect with `errors.retry.timeout` set, as Mux recommends | Kafka on its best settings, as Mux runs it. |
| Failover | Kill the leader at minute 15 | Time until exports flow again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift, expiry and open-file growth show over hours. |
| Hardware | 3 brokers each side, same AWS instance family and region | Like for like. |
| Cost | Nodes, broker CPU, disk for 24 h of retention, sink workers, open files and monthly cost for the lane | Reported as $ per billion exported views, every reader included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's export worker shards, Kafka by the OpenMessaging
Benchmark plus Kafka Connect sinks with the same workload shape. The kit runs alongside at each
step, so the report shows exports behind budget, views lost, views past the one-minute promise and
backfill time next to p99.

**Pass line:** p99 under 50 ms for every healthy export at 10K views/s across 2,000 export topics,
replicated, through the outages and with beacon traffic next door; every stalled export backfills
from its last exported view with zero views lost; no healthy export hands a view over later than
the one-minute promise; and a lower $ per billion exported views than the same lane on Kafka.

**Published:** p99 and p99.9 vs rate and vs export count (one line per system), the outage and
backfill traces, failover recovery, broker CPU, open files, disk, node count, monthly cost and
$ per billion views, and every config and raw output.
