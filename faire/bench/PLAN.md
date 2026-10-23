# Benchmark plan: the Faire FBF stock lane

**Question:** through a Faire Market order burst and with lakehouse CDC on the same broker, does every Fulfilled by
Faire stock change (receipt, pick, cycle count, reservation) reach every reader of the multi-brand checkout's stock
view within 20 ms at p99, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | ~200 B | brand:SKU, sequence, on hand, reserved, available, case pack, kind, timestamp. |
| Rate | 500 → 2K → 10K → 30K changes/s | FBF stores a growing set of brands' inventory; steady churn is low, Faire Markets and the holiday season are not. The top steps are headroom, not a Faire number. |
| Partitions | 16, keyed by brand:SKU | Each SKU's changes apply in order; a big brand's receipt spreads across partitions. |
| Readers | 3 | Multi-brand checkout, cart and product pages, a rebuild reader. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A reserved unit must not come back as available. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Faire Market | 5× order reservations for 10 min | Market events concentrate retailer orders. |
| Neighbour | 50 MB/s lakehouse CDC on a separate topic | The CockroachDB and MySQL ingestion into Iceberg shares the broker. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Rebuild | Restart a checkout reader at minute 20 | Seconds to replay the retained lane and serve again. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows the periodic-sync comparison and warehouse short lines next to p99.

**Pass line:** change-to-reader p99 under 20 ms at 10K changes/s with three readers, replicated, through the 5×
Market burst and with the CDC neighbour running; a restarted reader serving again within 10 s.

**Published:** p99 and p99.9 vs rate (one line per system), the Market trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
