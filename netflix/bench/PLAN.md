# Benchmark plan: the frequency-cap lane

**Question:** through a live ad break, with heavy ad playback beacons on the same brokers, does every enriched
impression reach every ad server's frequency-cap view within 10 ms at p99, on Apache Iggy and on the same lane
in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~170 B | Profile, campaign, cap window, writer, sequence (the event ID), running count, kind, timestamp. |
| Rate | 50K on demand, plus a break burst of 300K → 1M → 2M impressions/s | Your own load test: one region, one ad break, "hundreds of thousands of events per second". |
| Break shape | Ramp to the burst in 2 s, hold 60 s, drain in 30 s, every 10 min | A live audience reaches a break within seconds and leaves it together. |
| Partitions | 32, keyed by profile | A profile's impressions apply in order on every reader. |
| Readers | 20 → 100 → 300 cap-view readers, each reading every partition directly | Every ad server's view must see every impression. |
| Repeats | 0.5% of impressions delivered twice | At-least-once with best-effort de-duplication upstream; the reader drops repeats by event ID. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Impressions feed caps and billing and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Neighbour | 100 MB/s of ad playback beacons on a separate topic, same brokers | A heavy sessionizer and metrics feed must not move the lane's tail. |
| Restart | Restart 10% of readers in the middle of a break | Time to rebuild the cap view from the log, and that break's p99. |
| Failover | Kill the leader during a break | Time until every reader receives again. |
| Duration | 30 min per step + 24 h soak | A full daily cap window; drift shows over hours. |
| Hardware | 3 brokers each side, same instance type, same AWS region | Like for like; nodes and cost per billion impressions reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows over-cap impressions and live sponsor repeats next to p99.

**Pass line:** impression-to-cap-view p99 under 10 ms at a 1M impressions/s break with 100 readers, replicated,
with the neighbour running; over-cap impressions under 0.01%; no impression counted twice.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), the break trace, restart and
failover recovery, broker CPU, node count and cost per billion impressions, and every config and raw output.
