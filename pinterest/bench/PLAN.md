# Benchmark plan: the in-flight spend lane

**Question:** with heavy insertion and engagement logs on the same brokers, does every insertion's in-flight
spend reach the budget view of every ad server within 10 ms at p99, on Apache Iggy and on the same lane in
your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~100 B | Ad group, budget day, writer, sequence (the event ID), running in-flight spend, timestamp. |
| Rate | 20K → 50K → 200K insertions/s | Your published requirement was "tens of thousands of events per second"; the top step leaves headroom. |
| Partitions | 32, keyed by ad group | An ad group's spend applies in order on every reader. |
| Readers | 100 → 1,000 → 2,000 budget-view readers, each reading every partition directly | Your requirement: "fan out updates to more than 1,000 consumer machines". |
| Repeats | 0.5% of insertions delivered twice | At-least-once delivery; the reader drops repeats by event ID. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Spend decides what advertisers are charged and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Neighbour | 100 MB/s of insertion and engagement logs on a separate topic, same brokers | A heavy logging feed must not move the lane's tail. |
| Restart | Restart 10% of readers at peak | Time to rebuild the budget view from the log, and p99 meanwhile. |
| Failover | Kill the leader at peak | Time until every reader receives again. |
| Duration | 30 min per step + 24 h soak | A full budget day; drift shows over hours. |
| Hardware | 3 brokers each side, same instance type, same AWS region | Like for like; nodes and cost per billion insertions reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows overdelivery (spend past budget) next to p99.

**Pass line:** spend-to-budget-view p99 under 10 ms at 50K insertions/s with 1,000 readers, replicated, with
the neighbour running; overdelivery under 0.01% of spend; no insertion counted twice.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), restart and failover
recovery, broker CPU, node count and cost per billion insertions, and every config and raw output.
