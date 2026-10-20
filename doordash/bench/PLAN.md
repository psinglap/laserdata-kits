# Benchmark plan: the store feedback lane

**Question:** at Super Bowl Sunday rates, does every Dasher event at a store (assigned, arrived,
picked up) reach every reader (the dispatch coordinators, one per region, and the ETA feature
path) within 20 ms at p99, with client analytics flooding the same cluster, on Apache Iggy and on
the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 250 B | Store id, sequence, event, Dashers heading there and waiting, last five waits, market utilization, timestamp. |
| Rate | 500 → 2K → 10K → 50K feedback events/s | 970M orders in Q2 2026 is about 123 orders/s on average, roughly 370 events/s at three Dasher events per order; dinner peaks, Super Bowl Sunday and Wolt and Deliveroo on one platform are the higher steps. |
| Partitions | 16, keyed by store id | Each store's feedback applies in order. |
| Readers | 4 consumer groups | Dispatch coordinators (two regions), ETA features, one analytics reader. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | Dispatch must not lose a store's feedback on a broker failure. A second run at Iggy `replicated` × 2 against RF 2 / leader acks, Iguazu's settings. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings (Iguazu's proxy uses 50–100 ms linger for throughput). |
| Rush | 3× for 10 min | Super Bowl Sunday and Friday dinner, where a busy store's counter changes every few seconds. |
| Neighbour | Client analytics at 50 → 500 MB/s on its own topic | The bulk of the event volume; it must not move the feedback lane's tail. |
| Failover | Kill the leader at minute 15 | Time until coordinators receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | Iggy on 3 nodes in the same region and AZs as the Kafka cluster | Like for like; nodes, cross-AZ bytes and $ per month reported for the lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows the share of dispatch decisions made on
stale store feedback, and the share it would have flipped between speed and batching, next to p99.

**Pass line:** event-to-reader p99 under 20 ms at 10K feedback events/s with four readers, replicated,
through the 3× rush and with the analytics flood running; stale-feedback decisions under 0.5%.

**Published:** p99 and p99.9 vs rate (one line per system), the rush trace, failover recovery, broker
CPU, node count and monthly cost for the lane, and every config and raw output.
