# Benchmark plan: the playback health lane

**Question:** through the death overs, with full heartbeats on the same brokers, does every minimized
heartbeat reach the health check, and every CDN health signal reach routing and alerting, within 20 ms
at p99, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | 1.1 KB heartbeat · 200 B signal | The minimized HeartbeatEvent (80-90 fields); pair, state, severity, score, sequence. |
| Rate | 50K → 250K → 1M heartbeats/s | A slice of 2.1M messages a second, then a full IPL-night share of it. |
| Partitions | 64 heartbeats, 8 signals, keyed by cohort and CDN | Each pair's heartbeats in order; few partitions per broker. |
| Readers | Health check on heartbeats; routing, alerting, QoS dashboard on signals | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | At-least-once on both sides, like the splitter's contract. |
| Kafka tuning | confluent-kafka-go, `linger.ms=0`, `fetch.max.wait.ms=0`, cooperative-sticky | Kafka on its best latency settings, same instance class. |
| Death overs | 3× for 15 min | The last overs of an IPL final. |
| Incidents | One every 5 min, a busy pair at 25% failures for 2 min | Each scored: detection window, flag to routing, plays sent after the flag. |
| Neighbour | 7.5 KB full heartbeats at 200 MB/s on a separate topic, same brokers | Full-data consumers must not move the lane's tail. |
| Reader churn | Add and remove a health-check instance at minute 10 | Rebalance at peak: time until the lane is back under budget. |
| Failover | Kill the leader at minute 20 | Time until routing receives again, and the next minute's p99. |
| Duration | 30 min per step + one 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and disks, in your cloud account | Like for like; nodes, cores, network and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `plays sent after the flag` next to p99.

**Pass line:** heartbeat-to-check and check-to-routing p99 under 20 ms each at 250K heartbeats a second,
replicated, through the 3× death overs with the neighbour running; zero plays sent to a pair after its flag.

**Published:** p99 and p99.9 vs rate (one line per system), the death-overs trace, every incident's timings,
rebalance and failover recovery, broker CPU, node count and monthly cost for the lane, and every config and
raw output.
