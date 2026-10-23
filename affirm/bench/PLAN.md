# Benchmark plan: the customer aggregate lane

**Question:** through a Black Friday and Cyber Monday peak, does every user-level aggregate reach the online
feature store within 20 ms at p99, so a checkout that starts a moment after a loan event finds the stored
value current, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Aggregate size | ~300 B | Customer, counter, schema version, total repaid, outstanding, charged off (and unpaid), overdue, open disputes, timestamp. |
| Rate | 500 → 2K → 10K → 50K aggregates/s | Affirm reported about 53 million transactions in the June 2026 quarter, plus their repayments; the top steps are headroom, not a claim about Affirm's volume. |
| Partitions | 16, keyed by customer | Each customer's aggregates apply in counter order. |
| Readers | 3 | The feature platform's consumer, a fraud-rule reader with a tight freshness bound, and the offline reconstruction reader. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A lost aggregate is a credit decision on old data. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Peak | 5× for 30 min | Black Friday and Cyber Monday checkouts and the repayments that follow. |
| Redelivery | 1% of aggregates sent twice, some late | The write path must keep the newer value (duplicate, older). |
| Backfill neighbour | 50 MB/s on a separate topic | A history backfill must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows checkouts found current next to p99.

**Pass line:** change-to-store p99 under 20 ms at 10K aggregates/s with three readers, replicated, through
the 5× peak and with the backfill running; at least 99.9% of checkouts that start 50 ms or more after a loan
event find the stored aggregate current; no redelivered aggregate replaces a newer one.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery, broker CPU,
node count and monthly cost for the lane, and every config and raw output.
