# Benchmark plan: the Tekion vehicle context lane

**Question:** through a dealer group's repricing pass and with lakehouse CDC on the same broker, does every vehicle
change (status, price, miles) reach every reader of the agent context within 20 ms at p99, so the context agents
read is as current as the DMS, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | ~180 B | rooftop:VIN, sequence, status, price, miles, kind, timestamp. |
| Rate | 1K → 5K → 20K → 50K changes/s | 3,000+ rooftops; steady churn is low, repricing passes and month-end are not. The top steps are headroom, not a Tekion number. |
| Partitions | 16, keyed by rooftop:VIN | Each vehicle's changes apply in order; a big group's pass spreads across partitions. |
| Readers | 3 | Agent context service (Salesperson AI, T1 Pro), digital retail, a rebuild reader. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A sold vehicle must not come back as In Stock. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Repricing pass | 10× for 10 min, most extra changes on one group's rooftops | A group reprices its used inventory in one run. |
| Neighbour | 50 MB/s lakehouse CDC on a separate topic | The data platform's move to real-time ingestion shares the broker. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Rebuild | Restart a context reader at minute 20 | Seconds to replay the retained lane and serve again. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows the TTL-cache comparison and DMS declines next to p99.

**Pass line:** change-to-reader p99 under 20 ms at 20K changes/s with three readers, replicated, through the 10×
repricing pass and with the CDC neighbour running; a restarted reader serving again within 10 s.

**Published:** p99 and p99.9 vs rate (one line per system), the repricing trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
