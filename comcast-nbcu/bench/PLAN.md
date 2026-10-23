# Benchmark plan: the Peacock stream-limit lane

**Question:** at the kickoff of a big live event, with heavy player QoE telemetry on the same brokers, does
every playback session start, heartbeat and stop reach the stream-limit view of every playback node within
10 ms at p99, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B | Account, device, session ID, kind, writer, sequence (the event ID), timestamp. |
| Steady rate | 300K session events/s | Order of magnitude for tens of millions of concurrent streams with a heartbeat every 60 to 120 s; replace with your own heartbeat interval and concurrency. |
| Kickoff shape | 2M session starts within 60 s, then 300K/s; at the final whistle, the same number of stops within 60 s | Peacock's biggest live events arrive together: "the huge volume of audience coming to the platform at the same time" (Patrick Miceli, 2024). |
| Partitions | 64, keyed by account | An account's sessions apply in order on every reader. |
| Readers | 20 → 100 → 300 playback nodes, each reading every partition directly | Every node's view must see every account's sessions. |
| Repeats | 0.5% of events delivered twice | At-least-once from players and retries; the reader drops repeats by event ID. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | The lane is the record of which streams were running. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Neighbour | 100 MB/s of player QoE beacons on a separate topic, same brokers | Bulk telemetry must not move the lane's tail. |
| Restart | Restart 10% of playback nodes during the kickoff | Time to rebuild the view from the log, and that minute's p99. |
| Failover | Kill the leader during the kickoff | Time until every node receives again. |
| Two regions | One lane per region, mirrored; nodes in each region read both | Peacock runs multi-region; measure cross-region view lag separately from in-region. |
| Duration | 30 min per step + 24 h soak | A full prime-time evening; drift shows over hours. |
| Hardware | 3 brokers each side, same instance type, same AWS region | Like for like; nodes and cost per billion session events reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows viewers wrongly blocked and streams over the limit next to p99.

**Pass line:** session-event-to-view p99 under 10 ms at the kickoff step with 100 nodes, replicated, with the
neighbour running; no viewer wrongly blocked and no stream over the limit attributable to the lane.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), the kickoff trace, restart and
failover recovery, broker CPU, node count and cost per billion session events, and every config and raw output.
