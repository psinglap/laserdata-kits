# Benchmark plan: the geohash forecast lane

**Question:** when the real-time forecasting models publish a frame of geohash-6 demand and supply forecasts,
does every region's frame reach Rider Pricing, Driver Earnings and the other readers complete, within 20 ms at
p99 after its seal, at the cell counts Lyft runs and at faster loops than once a minute, on Apache Iggy and on
the same lane in MSK?

Rules are fixed before any result exists. If MSK wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Frame | Hundreds of regions, every geohash-6 cell, 12 demand and 12 supply values per cell | The shape in Lyft's forecasting post: geohash-6, every 5-minute interval of the next hour. |
| Cells per minute | 1M → 4M → 8M | Lyft's post: forecasts for 4M geohashes per minute, per signal. |
| Cadence | 60 s → 15 s → 5 s | Today's minutely run, and the faster pricing loops Lyft's streaming team says it is building. |
| Message | One chunk of 200 cells per message, a seal per region per frame | Readers switch a region to a new frame only when it is complete. |
| Partitions | 32, keyed by region | A region's chunks and seal stay in order. |
| Readers | 4 | Rider Pricing, Driver Earnings, the real-time supply heatmap and one analytics reader. |
| Durability | Iggy `replicated` x 3 · MSK `acks=all`, RF 3, min ISR 2 | Same guarantees both sides. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | MSK on its best latency settings. |
| Neighbour | Analytics events at 50 → 200 MB/s on a separate topic, 3x for 10 minutes | Feature-generation input on the same cluster, with a Halloween-style peak. |
| Reader restart | Restart one reader every 5 minutes | A deploy: time until it has every region's latest frame again. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` and the kit's `forecaster.py` at full scale; MSK by the OpenMessaging Benchmark
with the same workload shape. The kit's readers run alongside at each step, so the report shows the share of
quotes priced on a superseded forecast next to p99.

**Pass line:** seal-to-ready p99 under 20 ms for every region at 4M cells a minute on a 5 s cadence, with four
readers, replicated, through the neighbour's peak; a restarted reader has every region's latest frame within
5 s.

**Published:** p99 and p99.9 vs cells per minute and cadence (one line per system), the neighbour-peak trace,
restart and failover recovery, broker CPU, node count and monthly cost for the lane, and every config and raw
output.
