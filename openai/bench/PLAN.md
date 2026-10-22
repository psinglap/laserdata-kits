# Benchmark plan: the key shutoff lane

**Question:** at launch-day request rates, with a heavy request-log stream on the same brokers, does every
served request reach the enforcement service, and every key disable reach every request router, within 10 ms
at p99 per hop, on Apache Iggy and on the same lane in Apache Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B request record · ~150 B disable | Router, key ID (never the secret), project, source network, sequence, cost, timestamp. |
| Rate | 50K → 200K → 500K request records/s | The lane carries one small record per request; OpenAI's API serves 6 billion tokens a minute (DevDay 2025), so the upper steps are a slice of real traffic. |
| Partitions | 64 on api-requests, keyed by key · 1–4 on key-status | A key's requests are judged in order; every router reads every disable. |
| Readers | Enforcement workers on api-requests; 100 → 1,000 routers on key-status | The fan-out back to the routing fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A disable is a security record and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms` low, same region | Kafka on its best latency settings. |
| Peak | 3× for 10 min | A model launch. |
| Neighbour | 500 MB/s of request and response logs on a separate topic, same brokers | The logging pipeline must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until enforcement and routers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same Azure region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows requests served on disabled keys next to p99.

**Pass line:** request-to-enforcement p99 and disable-to-router p99 each under 10 ms at 200K records/s with
1,000 routers, replicated, through the 3× peak and with the neighbour running; under 1% of what a lane one
second behind would serve on a disabled key.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
