# Benchmark plan: the Spotify episode publish status lane

**Question:** during a bulk delivery that fills transcoding, with the reprocessing job's segment log on the same
brokers, does every upload's receipt and every stage change reach the episode status service within 10 ms at
p99, and does the backlog watch alert within a second of the real crossing, on Apache Iggy and on the same lane
in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B | Episode key (show plus content digest), stage, priority queue, queued time, event number, timestamp. |
| Steady rate | 5K stage changes/s | Illustrative: about 1K uploads a second over five stages. Replace with your own upload and update rates; Spotify has not published them. |
| Bulk delivery | 4× new uploads for 10 minutes, more than transcoding can start | The June 24, 2026 shape: "large bulk spikes in content delivery" on top of a batch job (Spotify Engineering, July 2026). |
| Partitions | 32, keyed by episode | An episode's stage changes apply in order. |
| Readers | 2 → 6 status replicas, each reading every partition directly | The status view and the watch run in more than one place. |
| Repeats | 0.5% of stage changes delivered twice | At-least-once from workers and retries; the reader drops repeats by event number. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | The lane is the record of what each creator was told. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Neighbour | 100 MB/s of reprocessing segment logs on a separate topic, same brokers | Background work must not move the creator-facing lane. |
| Late replica | Start a status replica in the middle of the backlog | Time to rebuild the view from the log, and that it alerts on the backlog it finds. |
| Failover | Kill the leader during the bulk delivery | Time until every replica receives again; no receipt lost. |
| Duration | 30 min per step + 24 h soak | A full bulk delivery and drain; drift shows over hours. |
| Hardware | 3 brokers each side, same GCP machine type, same region | Like for like; nodes and cost per billion stage changes reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows receipts within a second, re-uploads answered and alert delay next to
p99.

**Pass line:** stage-change-to-view p99 under 10 ms during the bulk delivery with 6 replicas, replicated, with the
neighbour running; every receipt in the view within a second; every backlog crossing alerted within a second.

**Published:** p99 and p99.9 vs rate and vs replica count (one line per system), the bulk delivery trace, late
replica rebuild and failover recovery, broker CPU, node count and cost per billion stage changes, and every config
and raw output.
