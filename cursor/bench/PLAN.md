# Benchmark plan: the spend-limit lane

**Question:** at peak request rates, with a heavy Tab-events stream on the same brokers, does every billable
request's usage reach the spend-limit check, and every stop reach every API server, within 10 ms at p99 per
hop, on Apache Iggy, and how does the same lane compare on WarpStream and on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~140 B usage record · ~110 B decision | API server, user, period, sequence, running paid usage in USD, timestamp. |
| Rate | 50K → 200K → 500K → 1M usage records/s | Billable requests across millions of developers; the real rate is yours to set. |
| Partitions | 32 on usage, keyed by user · 1–4 on decisions | A user's usage applies in order; every API server reads every decision. |
| Readers | The check on usage; 50 → 500 API servers on decisions | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 · WarpStream defaults | Usage is billing-relevant and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 5× for 10 min | A busy hour, compressed. |
| Neighbour | 50 MB/s of Tab accept/reject events on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the check and the API servers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows usage past the limit in USD next to p99.

**Pass line:** usage-to-limit-check p99 and decision-to-API-server p99 each under 10 ms at 500K usage
records/s with 500 API servers, replicated, through the 5× peak and with the neighbour running; usage past
the limit under 0.1% of paid usage.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
