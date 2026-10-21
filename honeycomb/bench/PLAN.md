# Benchmark plan: the ingest freshness lane

**Question:** with every dataset's receipts and every Retriever node's progress on the lane, and a backfill
running on the same brokers, does each message reach the freshness service within 10 ms at p99, on Apache
Iggy and on the same lane in Kafka, so that no heartbeat fires on a dataset that is still sending?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B per receipt or progress report · ~16 KB per backfill record next door | Dataset, sequence, Kafka partition and offset, event count, timestamp. The backfill record stands in for an event batch. |
| Rate | 40K → 200K receipts/s (one per dataset per batch or per 100 ms), plus 100 partitions × 2 Retriever nodes × 2 progress reports/s | Your ingest runs ~100 Kafka partitions at up to 50K msgs/s each (SREcon23); the lane carries one receipt per batch, not the events. Dataset count is illustrative until you share yours. |
| Partitions | 32 on the receipts topic, keyed by dataset · 4 on progress, keyed by Kafka partition | A dataset's receipts are applied in order; the freshness service scales by partition. |
| Readers | 1 → 4 freshness workers, one consumer group per topic | Each worker owns its datasets' partitions; offsets stored once a second. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A lost receipt is a missed heartbeat answer. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family (Graviton) | Kafka on its best latency settings, on its own small cluster, not the main events cluster. |
| Peak | 2× receipts for 10 min | A large customer onboarding or a traffic spike on a few partitions. |
| Stall | Retriever progress frozen on 30% of Kafka partitions for 10 min | The cutover and offset-reset cases from your 2026 incident reports. |
| Neighbour | 300 MB/s of backfill on a separate topic, same brokers | The replay after a maintenance window must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the freshness service receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, ARM (Graviton) and x86, same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows fired, missed, spurious and held heartbeats
next to p99.

**Pass line:** receipt-to-freshness p99 under 10 ms at 200K receipts/s, replicated, through the peak, the stall
and the backfill; no spurious page and no missed silence.

**Published:** p99 and p99.9 vs rate (one line per system), the peak and stall traces, failover recovery, node
CPU and memory, node count and monthly cost for the lane, and every config and raw output.
