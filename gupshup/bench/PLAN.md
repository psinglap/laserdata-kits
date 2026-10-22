# Benchmark plan: the app wallet lane

**Question:** at festival-campaign send rates, with a heavy inbound-messages stream on the same brokers, does
every message's fee reach the app's wallet, and every block and top-up reach every send API node, within
10 ms at p99 per hop, on Apache Iggy, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~160 B charge record · ~90 B wallet state | Node, app, sequence, running messages and fees, timestamp. |
| Rate | 4K → 20K → 50K → 100K charge records/s | 120B+ messages a year is ~3,800/s on average; campaigns burst far above it. |
| Partitions | 32 on charges, keyed by app · 1–4 on wallet state | An app's charges apply in order; every send node reads every state change. |
| Readers | The wallet service on charges; 20 → 200 send API nodes on wallet state | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Charges are money and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 10× for 10 min | A festival campaign blast, compressed. |
| Neighbour | 50 MB/s of inbound messages and media on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the wallet and the send nodes receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows messages accepted past the overdraft next to p99.

**Pass line:** charge-to-wallet p99 and state-to-send-node p99 each under 10 ms at 50K charge records/s with
200 send nodes, replicated, through the 10× peak and with the neighbour running; messages accepted past the
overdraft under 0.01% of messages.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
