# Benchmark plan: the payment downtime lane

**Question:** through a match-day surge, with a heavy workflow-event stream on the same brokers, does
every payment status event reach the downtime detector, and every downtime signal reach Checkout and the
webhook dispatcher, within 20 ms at p99, on Apache Iggy and on the same lane in your MSK?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | 150 B status event · 200 B signal | Cohort, payment ID, status, sequence, timestamp; cohort state, success rate, begin. |
| Rate | 2K → 10K → 25K payment status events/s | 500M+ transactions a month on average; the UPI Switch was rebuilt for a 10,000 TPS target. |
| Partitions | 16 status, 4 signals, keyed by cohort | One consolidated topic per hop: no topic per event type, few partitions per broker. |
| Readers | Detector on status; Checkout, webhook dispatcher, dashboard on signals | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | As on MSK today; a missed signal is a failed payment. |
| Kafka tuning | franz-go-equivalent client settings, `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, MSK Provisioned with SASL/SCRAM, same instance class. |
| Surge | 5× for 15 min | An IPL or Asia Cup evening. |
| Downtimes | One every 5 min, a busy cohort at 10% success for 2 min | Each scored: detection window, flag to Checkout, offered after the flag. |
| Neighbour | 50 MB/s of workflow events on a separate topic, same brokers | About 15 workflow events per payment must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until Checkout receives again, and the next minute's p99. |
| Duration | 30 min per step + one 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same EC2 family and EBS class, in your AWS account | Like for like; nodes, cores and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `offered after the flag` next to p99.

**Pass line:** status-to-detector and detector-to-Checkout p99 under 20 ms each at 10K payment status
events a second, replicated, through the 5× surge with the neighbour running; zero checkouts offered a
method after its flag.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, every downtime's timings,
failover recovery, broker CPU, node count and monthly cost for the lane, and every config and raw output.
