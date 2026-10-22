# Benchmark plan: the device throttle lane

**Question:** at Amplitude's ingestion rates, with a heavy Session Replay stream on the same brokers, does every
ingestion node's accepted event count reach the throttle service, and every throttle reach every ingestion node,
within 10 ms at p99 per hop, on Apache Iggy, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~110 B count record · ~90 B decision | Node, device, second, running count, sequence, timestamp. |
| Rate | 100K → 400K → 1M → 2M count records/s | More than 1 trillion events a month is about 400K events/s on average; peaks are yours to set. |
| Partitions | 32 on device-counts, keyed by device · 1–4 on throttle-state | A device's counts apply in order; every ingestion node reads every decision. |
| Readers | The throttle service on counts; 50 → 500 ingestion nodes on decisions | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A lost count lets a spam device through; a lost throttle leaves one node open. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Spam | 0.1% of devices at 40–300 events/s, starting over the run | Broken instrumentation, the rates the docs call spammy. |
| Peak | 3× for 10 min | A customer launch or a release that ships a tracking loop. |
| Neighbour | 100 MB/s of Session Replay segments on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the throttle service and the nodes receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows events let in past the throttle next to p99.

**Pass line:** count-to-throttle p99 and throttle-to-ingestion-node p99 each under 10 ms at 1M count records/s
with 500 ingestion nodes, replicated, through the 3× peak and with the neighbour running; no device at human
rates throttled.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
