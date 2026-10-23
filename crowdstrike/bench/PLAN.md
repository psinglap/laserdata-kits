# Benchmark plan: the identity enforcement lane

**Question:** at Monday-morning logon rates, with a heavy endpoint-telemetry stream on the same brokers, does every
authentication reach the identity detection engine, and every block or MFA step reach every domain controller,
within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Apache Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~180 B authentication record · ~150 B enforcement | DC, account with customer ID, source host, destination host, protocol, sequence, timestamp. |
| Rate | 50K → 200K → 500K authentications/s | One small record per authentication across many customers' domain controllers; a slice of a platform that processes almost 3 trillion events a day (CrowdStrike job posts, 2026). |
| Partitions | 64 on auth-events, keyed by account · 1–4 on identity-enforcement | An account's authentications are judged in order; every DC reads every enforcement for its customer. |
| Readers | Detection workers on auth-events; 100 → 2,000 DC sensors on identity-enforcement | The fan-out back to the DC fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A block is a security record and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms` low, same region | Kafka on its best latency settings. |
| Peak | 3× for 10 min | Monday 9 am logons, or a password spray hitting many accounts at once. |
| Neighbour | 500 MB/s of endpoint telemetry on a separate topic, same brokers | The firehose must not move the identity lane's tail. |
| Failover | Kill a broker at minute 15 | Time until detection and DCs receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows attacker authentications let through after a block next
to p99.

**Pass line:** auth-to-detection p99 and enforcement-to-DC p99 each under 10 ms at 200K authentications/s with
2,000 DC sensors, replicated, through the 3× peak and with the neighbour running; under 1% of what a lane one
second behind would let through on a blocked account.
