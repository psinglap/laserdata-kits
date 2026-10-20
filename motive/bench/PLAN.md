# Benchmark plan: the cargo alert lane

**Question:** at morning-dispatch door rates, with a heavy dead-zone backfill on the same brokers, does every
door change and reading reach the alert rules, and every alert reach every notifier, within 10 ms at p99 per
hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B sensor event · ~200 B alert | Asset, sequence, door state and since when, temperature, set point, position, timestamp. |
| Rate | 300K → 1M assets, a reading a minute each, plus door changes (5K → 20K events/s) | Motive's posting: "hundreds of thousands of IoT devices"; one-minute environmental readings are documented. |
| Partitions | 32 on sensor events, keyed by asset · 1–4 on alerts | An asset's events are evaluated in order; every notifier reads every alert. |
| Readers | Alert-rule workers on sensor events; 3 → 30 notifiers on alerts | Driver app, dashboard, webhooks and partner integrations, each from its own offset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | Alerts and door events are a record customers dispute (spoiled loads); they cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | 5× door openings for 10 min | Morning dispatch at the yards. |
| Neighbour | 500 MB/s of buffered readings on a separate topic, same brokers | Reconnect storms after dead zones must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the rules and notifiers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows door alerts delivered while the door was open next to p99.

**Pass line:** event-to-rules p99 and alert-to-notifier p99 each under 10 ms at 1M assets with 30 notifiers,
replicated, through the 5× peak and with the neighbour running; every door alert delivered while its door is
open, with your real door durations.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU and memory, node count and monthly cost for the lane, and every config and raw output.
