# Benchmark plan: the Roku Exchange live floor lane

**Question:** at live-game peak auction rates, with a heavy impression log on the same brokers, does every
auction outcome reach the floor service, and every new floor reach every exchange node, within 10 ms at p99 per
hop, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~160 B outcome · ~110 B floor update | Node, segment, sequence, bids back, top and second bid, timestamp; segment, sequence, floor, timestamp. |
| Rate | 50K → 200K → 500K → 1M outcomes/s | The exchange prices every impression across 100M+ household devices; the real auction rate is yours to set. |
| Partitions | 32 on outcomes, keyed by segment · 1–4 on floors | A segment's outcomes apply in order; every exchange node reads every floor. |
| Segments | 400 → 5,000 → 50,000 | Content category × device type × break slot, and finer floor personalization. |
| Readers | Floor service on outcomes; 50 → 500 exchange nodes on floors | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Floors set revenue and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 10 min | A live game's ad breaks. |
| Neighbour | 50 MB/s of impression log on a separate topic, same brokers | A heavy reporting stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the floor service and the exchange nodes receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows RPM and fill next to p99.

**Pass line:** outcome-to-floor-service p99 and floor-to-exchange-node p99 each under 10 ms at 500K outcomes/s
with 500 exchange nodes, replicated, through the 3× peak and with the neighbour running; a node added mid-run
holds a full floor table within one second.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
