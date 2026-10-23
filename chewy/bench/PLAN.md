# Benchmark plan: the fulfillment center edge lane

**Question:** on a small broker inside one fulfillment center, does every carton event reach the floor view
within 10 ms at p99 on Apache Iggy, applied once with handheld retries and replays, while the control system's
telemetry shares the broker; and when the building's link to AWS drops, does the floor keep its p99, do the
events wait in the building's log, and do they reach AWS afterwards in order, each once, with every carton's
history complete? How does the same lane compare on Kafka at the same footprint?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~210 B carton event | Site, carton, order, step, per-carton sequence, dock door, device and timestamp. |
| Rate | 100 → 500 → 2,000 → 5,000 carton events/s per building | Your building's real rate is yours to set; the top step leaves room for peak season and more automation. |
| Partitions | 8–16 on carton-events, keyed by carton · 4 on wcs-telemetry | A carton's events apply in order to one reader. |
| Neighbour | 5–20 MB/s of conveyor, sorter and scanner telemetry on its own topic | The floor's heaviest stream must not move the carton lane. |
| Retries | 0.5% of events sent twice, plus one floor view restart per step | Applied-once has to hold under at-least-once delivery. |
| Footprint | One small edge host per building (for example 4 vCPU, 8 GB, local SSD) | The broker has to fit where the WMS and control servers already run. |
| Durability | Iggy with fsync per batch on the edge host · Kafka `acks=all` on the same host | A ship-confirm cannot be lost if the edge host restarts. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Link outage | WAN to AWS down for 1 min, 15 min and 2 h | The backlog held in the building, the time to drain it, and the floor's p99 meanwhile. |
| Edge restart | Restart the edge broker at minute 20 | Time until the floor view reads again, and no carton event lost. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours, and a soak crosses shift changes. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows retries absorbed, events held and drained, and AWS reconciled
against the building next to p99.

**Pass line:** scan-to-floor-view p99 under 10 ms at 2,000 carton events/s with the telemetry neighbour, through
a 15-minute link outage; zero gaps in the building and in AWS; every carton's history in AWS equal to the
building's; the backlog drained within minutes of the link coming back.

**Published:** p99 and p99.9 vs rate (one line per system), the outage and restart traces, edge host CPU and
memory, drain time per minute of outage, reconciliation counts, and every config and raw output.
