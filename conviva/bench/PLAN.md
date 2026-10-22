# Benchmark plan: the live ad break lane

**Question:** at live-event ad rates, with every session on a channel entering the break together and the
full-census heartbeat firehose on the same brokers, does every ad event reach the ad break checker, and every
cohort alert reach every consumer, within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B ad event · ~300 B alert | Session, sequence, kind, ad server, market, device, break, slot, break counts, timestamp. |
| Rate | 1M → 10M concurrent sessions in breaks; 50K → 500K ad events/s, with each channel's break start and slot starts as bursts | Conviva measured 56 million peak concurrent plays at the 2026 FIFA World Cup and one prompt read 18 million ad attempts on opening day. |
| Partitions | 64 on ad events, keyed by session · 1–4 on alerts | A session's ad events are evaluated in order; every consumer reads every alert. |
| Readers | Checker workers on ad events; 3 → 30 consumers on alerts | Ad ops console, webhooks, agents and customer integrations, each from its own offset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | Ad delivery is billed and disputed; outcomes cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | All sessions of the largest channel entering one break in the same second | A live sports break is the same moment for everyone watching. |
| Neighbour | 500 MB/s of playback heartbeats on a separate topic, same brokers | The QoE firehose must not move the ad lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the checker and consumers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows breaks flagged while running next to p99.

**Pass line:** event-to-checker p99 and alert-to-consumer p99 each under 10 ms at 10M sessions with 30 consumers,
replicated, through the break-start burst and with the neighbour running; every failing cohort flagged while its
breaks are still running.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the burst trace, failover recovery, node
CPU and memory, node count and monthly cost for the lane, and every config and raw output.
