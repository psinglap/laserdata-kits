# Benchmark plan: the kitchen state lane

**Question:** at New Year's Eve rates, does every kitchen change (order accepted, Food Order Ready,
picked up) reach every reader (FPT features, restaurant stress monitoring, dispatch) within 20 ms
at p99, with a benchmark replay running on the same cluster, on Apache Iggy and on the same lane in
Confluent Cloud?

Rules are fixed before any result exists. If Confluent Cloud wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | Restaurant id, sequence, event, cooking and counter counts, five running orders, last five prep times, timestamp. |
| Rate | 500 → 2K → 10K → 50K kitchen events/s | NYE 2023 peaked at 8,400 orders a minute, about 420 kitchen events/s at three events per order; the higher steps are headroom for growth and every Eternal business on one cluster. |
| Partitions | 16, keyed by restaurant id | Each kitchen's changes apply in order. |
| Readers | 4 consumer groups | FPT feature view, restaurant stress monitoring, dispatch, one analytics reader. |
| Durability | Iggy `replicated` × 3 · Confluent Cloud `acks=all`, RF 3 | Kitchen state cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Confluent Cloud on its best latency settings. |
| Rush | 3× for 10 min | The NYE and dinner peaks, where one kitchen's queue changes every few seconds. |
| Neighbour | Benchmark replay at 50 → 500 MB/s on its own topic | A replay backfill averages 52M messages a minute; it must not move the kitchen lane's tail. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | Iggy on 3 nodes in the same region as the Confluent Cloud cluster | Like for like; nodes and $ per month reported against the Confluent bill for the lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Confluent Cloud by the OpenMessaging Benchmark, with the same
workload shape. The kit runs alongside at each rate, so the report shows the share of FPT
predictions made on a stale kitchen view next to p99.

**Pass line:** event-to-reader p99 under 20 ms at 10K kitchen events/s with four readers, replicated,
through the 3× rush and with the replay running; stale-view predictions under 0.5%.

**Published:** p99 and p99.9 vs rate (one line per system), the rush trace, failover recovery, broker
CPU, node count and monthly cost for the lane, and every config and raw output.
