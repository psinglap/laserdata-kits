# Benchmark plan: the Computer credit lane

**Question:** at peak agent-action rates, with a heavy agent-trajectory stream on the same brokers, does every
agent action's credits reach the credit ledger, and every pause and resume reach every agent runner, within
10 ms at p99 per hop, on Apache Iggy, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B action record · ~120 B decision | Runner, account, month, sequence, action kind, running credits, timestamp. |
| Rate | 50K → 200K → 500K → 1M action records/s | Every agent action of every Computer task; the real rate is yours to set. |
| Partitions | 32 on agent-actions, keyed by account · 1–4 on decisions | An account's credits apply in order; every runner reads every decision. |
| Readers | The ledger on agent-actions; 50 → 500 agent runners on decisions | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Credits are billing-relevant and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 5× for 10 min | A launch day, compressed. |
| Neighbour | 50 MB/s of agent trajectories on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the ledger and the runners receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows credits past zero next to p99.

**Pass line:** action-to-ledger p99 and decision-to-runner p99 each under 10 ms at 500K action records/s with
500 runners, replicated, through the 5× peak and with the neighbour running; credits past zero under 0.1% of
credits spent.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
