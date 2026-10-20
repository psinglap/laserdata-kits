# Benchmark plan: the vehicle state lane

**Question:** with the whole fleet reporting every five seconds and the full signal stream on the same
brokers, does every report's app-facing state reach the latest-state service within 10 ms at p99, on Apache
Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B per report on the lane · ~16 KB per full report next door | Vehicle, sequence, the app signals' resulting values, timestamp. The full report stands in for 5,500+ signals. |
| Rate | 150K → 500K vehicles, a report every 5 s each (30K → 100K reports/s) | "Over 150,000 vehicles stream 5,500+ signals every five seconds" (RV Tech, 2025), and growing with R2. |
| Partitions | 32 on the lane, keyed by vehicle | A vehicle's reports are applied in order; the latest-state service scales by partition. |
| Readers | 1 → 8 latest-state workers, one consumer group | Each worker owns its vehicles' partitions; offsets stored once a second. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | State the owner acts on (locked, charging) is not dropped. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings, not the bulk path's `linger.ms=100`. |
| Peak | 2× vehicles awake for 10 min | Evening plug-in and morning departure. |
| Neighbour | 300 MB/s of full signal reports on a separate topic, same brokers | "Hundreds of megabytes per second" of telemetry must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the state service receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows the share of app reads on the vehicle's
latest report next to p99.

**Pass line:** report-to-state p99 under 10 ms at 500K vehicles, replicated, through the 2× peak and with the
neighbour running; no app read two or more reports behind.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery, node CPU and
memory, node count and monthly cost for the lane, and every config and raw output.
