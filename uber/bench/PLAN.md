# Benchmark plan: the surge window lane

**Question:** at Uber's demand and supply rates, through a demand spike and with a heavy analytics stream on the
same brokers, does every rider and driver event reach the surge window that owns its hexagon within 10 ms at p99
on Apache Iggy, with no event arriving after its window closed, every window exact against an independent count,
both regions' workers agreeing on every window, and a restarted worker rebuilding its windows from the log, and
how does the same lane compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~100 B event · ~110 B multiplier | Kind, hexagon, product, rider or driver ID, event time. |
| Rate | 50K → 120K → 500K → 1M events/s | 120k events/s is the average input Uber published for its per-hexagon demand and supply features (Aug 2021); the steps above are headroom. |
| Partitions | 64 on surge-events, keyed by hexagon · 8 on multipliers | All of a hexagon's events in one partition, in order. |
| Windows | 60 s tumbling, 1 s grace, distinct riders and drivers per hexagon and product | The one-minute shape of Uber's demand and supply features; late events do not count, as in surge. |
| Hexagons | 500K active per step, skewed; 5M in the largest step | Uber reported about 5M hexagons across 1,500 cities. |
| Regions | 2 worker fleets reading the same lane independently | Surge runs a redundant pipeline in every region and relies on consistent input to converge. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2; and Kafka `acks=1` as a second Kafka row | Uber runs surge on a Kafka cluster tuned for throughput over lossless delivery; both settings are shown. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | Demand × 30 in 1% of hexagons for 5 min, and total traffic × 3 for 5 min | A stadium emptying, and a city-wide evening peak. |
| Neighbour | 100 MB/s of app analytics on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Worker restart | Stop one region's worker at minute 10; it replays and rebuilds | Time to catch up, and whether every rebuilt window is exact and agrees with the other region. |
| Failover | Kill the broker leader at minute 15 | Time until workers receive again, late events in that window, the next window's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows windows exact, late events and region agreement next to p99.

**Pass line:** event-to-window p99 under 10 ms at 500K events/s, replicated, through the peak and with the
neighbour running; 0 late events at a 1 s grace; every window exact against the independent count; both regions
agree on every window; a restarted worker catches up within 10 s with every rebuilt window exact.

**Published:** p99 and p99.9 vs rate (one line per system and durability setting), the peak trace, the worker
restart and failover recovery, late events per window, window checks, broker CPU, node count and monthly cost
for the lane, and every config and raw output.
