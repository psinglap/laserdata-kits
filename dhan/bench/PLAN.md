# Benchmark plan: the Dhan live order update lane

**Question:** at market-open rates, does every DEXT order update reach every order update gateway (the Dhan app,
web.dhan.co and DhanHQ API sockets) within 5 ms at p99, and does a reconnecting socket get every missed update
replayed from the log, in order, within 50 ms, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Update size | ~300 B | client ID, sequence, order number, leg, segment, symbol, side, product, status, quantities, prices, timestamp. |
| Rate | 10K → 20K → 50K → 100K updates/s | Dhan has not published updates per second; the steps bracket a plausible open and go well past it. |
| Partitions | 16, keyed by client ID | Each client's updates stay in sequence order, Super Order legs included. |
| Readers | 4 gateways, each reading every partition | The load balancer can put any socket on any gateway. |
| Reconnects | 1% of sockets per minute, away 0.3–3 s | Phones switching networks, algos restarting, a gateway failover. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A TRADED update must never come back missing. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Market open | 5× for 10 min | Dhan's AWS case study expects concurrent customers to jump a hundredfold at the open. |
| Neighbour | 50 MB/s compliance export on a separate topic | A bulk export must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until gateways receive again; resend success through it. |
| Sites | One run with gateways in a second AWS region | DEXT runs active-active across Mumbai and Bangalore. |
| Duration | 30 min per step + one full 9:15–15:30 session soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family, AWS ap-south-1 | Like for like; nodes and ₹ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate (with a Rust gateway reader in place of the Python one), so the report shows resend time and
gaps next to p99.

**Pass line:** DEXT-to-gateway p99 under 5 ms at 50K updates/s with four gateways, replicated, through the 5× open
and with the export neighbour running; zero sequence gaps and zero unrecoverable resends outside the failover
minute; resend p99 under 50 ms.

**Published:** p99 and p99.9 vs rate (one line per system), the open trace, failover recovery, resend time, broker
CPU, node count and monthly cost for the lane, and every config and raw output.
