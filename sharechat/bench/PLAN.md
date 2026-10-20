# Benchmark plan: the viewed-posts lane

**Question:** with heavy engagement traffic on the same brokers, does every app flush of viewed
post IDs reach the feed's dedup service within 20 ms at p99, so the user's next feed request never
repeats a post they have just seen, on Apache Iggy and on the same lane in your Redpanda?

Rules are fixed before any result exists. If Redpanda wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B flush | User ID, sequence, five post IDs, timestamp. |
| Rate | 14K → 30K → 60K → 120K flushes/s | About 6 billion Moj video plays a day (Monster Scale Summit 2025) is ~70K views/s, ~14K flushes/s at five posts per flush; headroom for both apps and festival peaks. |
| Feed requests | 8K → 16K → 32K/s | Moj feed at 8,000 requests a second (P99 CONF 2023). |
| Partitions | 32, keyed by user | A user's flushes apply in order; dedup reads per user. |
| Readers | Dedup service, one monitoring reader, the Flink job that persists views | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Redpanda `acks=all`, RF 3 | A view the dedup step relies on cannot be lost. |
| Redpanda tuning | `linger.ms=0`, `fetch.max.wait.ms=0` on the clients | Redpanda on its best latency settings, on the same hardware. |
| Peak | 1.5× for 15 min | Redpanda's case study: 50%+ more traffic on Diwali and Independence Day. Night traffic below 10% of peak, so also a scale-down step. |
| Gap | Next feed request 0.2–2 s after a flush | Set it to what your app logs show. |
| Neighbour | 100 MB/s of engagement events on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until dedup receives again, and the next minute's p99. |
| Duration | 30 min per step + one full-day soak | Drift shows over hours, and the daily cycle matters. |
| Hardware | 3 nodes each side, same GCP machine type (for example n2d), one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Redpanda by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `repeats served` next to p99.

**Pass line:** flush-to-dedup p99 under 20 ms at 30K flushes/s, replicated, through the 1.5× peak
with the neighbour running; feed requests missing the user's last flush under 0.01%.

**Where it runs:** in your GCP project, on your machines, with copies of viewed-post events only.
Nothing is in the feed path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery,
broker CPU, node count, cross-zone bytes and cost for the lane, and every config and raw output.
