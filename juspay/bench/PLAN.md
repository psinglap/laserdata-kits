# Benchmark plan: the release guard lane

**Question:** with API-log traffic on the same brokers, does every payment attempt's final status,
tagged with its release, reach the A/B comparison within 20 ms at p99, so the time to ABORT a bad
release is set by the statistics and not by the pipeline, on Apache Iggy and on the same lane in
your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B outcome | Payment ID, attempt sequence, release, payment method, merchant, status, error code, timestamp. |
| Rate | 3.5K → 10K → 25K → 50K outcomes/s | About 300 million payments a day on average, up to the 50,000+ TPS Juspay says it is built for. |
| Partitions | 16, keyed by payment ID | A payment's attempts apply in order; the comparison dedupes by attempt sequence. |
| Readers | The A/B comparison, one monitoring reader, one analytics reader | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | An outcome the comparison relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Release | Ladder 1% → 5% → 10% → 25% → 50% (set to Autopilot's), one injected defect per run | The comparison must scale a healthy release and abort a bad one. |
| Peak | 10× for 10 min | Juspay's blog: traffic "could surge as much as 10x in a matter of minutes". |
| Neighbour | 50 MB/s of API logs on a separate topic, same brokers | About 10 API calls per payment; that traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the comparison receives again, and the next minute's p99. |
| Duration | 30 min per step + one full-day soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same machine class on your infrastructure | Like for like; nodes, cores, disks and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows time to ABORT and payments failed by the
defect next to p99.

**Pass line:** outcome-to-comparison p99 under 20 ms at 10K outcomes/s, replicated, through the 10×
peak with the neighbour running; no false ABORT on a healthy release; ABORT within 3 s of a defect
that fails 20% of the canary's payments, at a 10% stage or above.

**Where it runs:** on your infrastructure, on copies of payment-attempt events only. The comparison
is advisory during the evaluation; Autopilot and the payment path are untouched.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery,
time to ABORT per run, broker CPU, node count and cost for the lane, and every config and raw output.
