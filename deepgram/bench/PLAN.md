# Benchmark plan: the streaming usage lane

**Question:** at peak live-stream counts, with a heavy request-log stream on the same brokers, does every usage
message from the speech engine reach the per-project usage summary within 10 ms at p99 on Apache Iggy, with
every closed stream reconciled and no retry billed, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B usage message · ~110 B summary | Server, request ID, project, API key, model, sequence, audio so far in ms, chunk, fin flag, timestamp. |
| Rate | 50K → 200K → 500K → 1M usage messages/s | Live streams × messages per stream per second; the real rate is yours to set. |
| Partitions | 32 on usage, keyed by project · 1–4 on summaries | A project's usage applies in order to one meter. |
| Retries | 0.5% of messages sent twice, plus one meter restart per step | Exactly-once billing has to hold under at-least-once delivery. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Usage is revenue and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× live streams for 10 min | A surge of new calls, compressed. |
| Neighbour | 50 MB/s of request logs on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the meter receives again, the next minute's p99, and reconciliation after. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows streams reconciled and audio over-billed next to p99.

**Pass line:** usage-to-summary p99 under 10 ms at 500K usage messages/s, replicated, through the 3× peak and
with the neighbour running; every closed stream reconciled to the millisecond; zero audio billed twice.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery, reconciliation
counts, broker CPU, node count and monthly cost for the lane, and every config and raw output.
