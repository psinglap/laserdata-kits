# Benchmark plan: the time-wave watermark lane

**Question:** with every cell's time-wave generations and arrival tallies on the lane, and a cell's backlog
draining on the same brokers, does each message reach the watermark service within 10 ms at p99, on Apache
Iggy and on the same lane in Kafka, so that no alert window closes before its data is complete?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~80–300 B per generation record or tally · ~16 KB per backlog record next door | Cell, source pipeline, generation, point count or the partitions arrived, timestamp. The backlog record stands in for a batch of queued telemetry. |
| Rate | One cell (4 pipelines × 64 partitions, a generation a second) → a region's cells (40 → 200 cells), up to 50K msgs/s | The lane carries generations and tallies, never telemetry. Cell, pipeline and partition counts are illustrative until you share yours (more than 100 Kafka clusters in 2022). |
| Partitions | 32 on the lane, keyed by cell/pipeline | A cell pipeline's records are applied in order; the watermark service scales by partition. |
| Readers | 1 → 4 watermark workers, direct partition reads | Each worker owns its cell pipelines' partitions and keeps its own position. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A lost tally holds a watermark until the next generation completes it; a lost generation record is a gap. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings, on its own small cluster, not a cell's cluster. |
| Stall | A quarter of one cell's partitions frozen for 10 min, then drained | One unresponsive broker in a cell (your August 2021 post). |
| Neighbour | 300 MB/s of backlog drain on a separate topic, same brokers | The recovered cell's catch-up must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the watermark service receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, ARM and x86, same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows windows event flow alone would close on
partial data, early watermarks and behind flags next to p99.

**Pass line:** wave-to-watermark p99 under 10 ms at the region's rate, replicated, through the stall and the
drain; no early watermark; every stalled cell pipeline flagged within the overdue limit plus one second.

**Published:** p99 and p99.9 vs rate (one line per system), the stall and drain traces, failover recovery,
node CPU and memory, node count and monthly cost for the lane, and every config and raw output.
