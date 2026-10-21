# Benchmark plan: the Kraken execution report lane

**Question:** at volatile-market rates, does every execution report reach every FIX and WebSocket gateway within
5 ms at p99, and does a reconnecting session get every missed report replayed from the log, in order, within
50 ms, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Report size | ~250 B | account, sequence, order id, pair, side, exec_type, order_status, quantities, price, timestamp. |
| Rate | 15K → 30K → 60K → 120K reports/s | From "over 1mm requests/min" (Kraken, 2023) up to several times that. |
| Partitions | 16, keyed by account | Each account's reports stay in sequence order. |
| Readers | 4 gateways, each reading every partition | The load balancer can put any session on any gateway. |
| Reconnects | 1% of sessions per minute, away 0.3–3 s | Client restarts, network blips, gateway failovers. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A fill must never come back missing. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Volatile market | 5× for 10 min | Jitter held under 30 ms through the FTX week (Kraken, 2023); this checks the lane does too. |
| Neighbour | 50 MB/s ledger export on a separate topic | A bulk export must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until gateways receive again; resend success through it. |
| Rollover | Simulate the 22:00 UTC session rollover at minute 20 | Every session logs on again at once. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate (with a Rust gateway reader in place of the Python one), so the report shows resend time and
gaps next to p99.

**Pass line:** engine-to-gateway p99 under 5 ms at 60K reports/s with four gateways, replicated, through the 5×
burst and with the export neighbour running; zero sequence gaps and zero unrecoverable resends outside the failover
minute; resend p99 under 50 ms.

**Published:** p99 and p99.9 vs rate (one line per system), the burst trace, failover recovery, resend time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
