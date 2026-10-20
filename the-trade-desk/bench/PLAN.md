# Benchmark plan: the spend-to-pacing lane

**Question:** at live-sports peak rates, with a heavy bid-feedback log on the same brokers, does every
win's spend reach the pacing controllers, and every pause reach every bid server, within 10 ms at p99
per hop, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B spend record · ~120 B decision | Bid server, ad group, flight, sequence, running spend in USD, timestamp. |
| Rate | 50K → 200K → 500K → 1M spend records/s | Wins are a fraction of the 20M queries a second pacing decides on; the real win rate is yours to set. |
| Partitions | 32 on spend, keyed by ad group · 1–4 on decisions | An ad group's spend applies in order; every bid server reads every decision. |
| Readers | Pacing on spend; 50 → 500 bid servers on decisions | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Spend is billing-relevant and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 5× for 10 min | A live game's high-demand moments on short flights. |
| Neighbour | 50 MB/s of bid feedback on a separate topic, same brokers | A heavy reporting stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until pacing and the bid servers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows overspend in USD next to p99.

**Pass line:** spend-to-pacing p99 and decision-to-bid-server p99 each under 10 ms at 500K spend records/s
with 500 bid servers, replicated, through the 5× peak and with the neighbour running; overspend under
0.1% of spend.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
