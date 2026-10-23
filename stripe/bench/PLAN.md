# Benchmark plan: the card testing control lane

**Question:** at Black Friday attempt rates, with a heavy API request-log stream on the same brokers, does every
card attempt reach the card testing detector, and every intervention and easing reach every payments API node,
within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Apache Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~220 B attempt record · ~150 B control | Node, business, surface, amount, card fingerprint (never the number), outcome, sequence, timestamp. |
| Rate | 50K → 200K → 500K attempt records/s | One small record per card setup or payment attempt; Stripe processed more than $1 trillion in a year (Stripe blog, Dec 2024), so the upper steps are a slice of real traffic. |
| Partitions | 64 on card-attempts, keyed by business · 1–4 on attack-controls | A business's attempts are judged in order; every node reads every control. |
| Readers | Detector workers on card-attempts; 100 → 1,000 API nodes on attack-controls | The fan-out back to the API fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A control is a risk decision and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms` low, same region | Kafka on its best latency settings. |
| Peak | 3× for 10 min | Black Friday. |
| Neighbour | 500 MB/s of API request logs on a separate topic, same brokers | The logging pipeline must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the detector and nodes receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows tester attempts reaching issuers after controls, and
flash sales flagged, next to p99.

**Pass line:** attempt-to-detector p99 and control-to-node p99 each under 10 ms at 200K records/s with 1,000
nodes, replicated, through the 3× peak and with the neighbour running; under 1% of what a lane one second behind
would let through after controls; no flash sale flagged.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
