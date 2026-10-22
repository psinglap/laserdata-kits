# Benchmark plan: the Glance ad-state lane

**Question:** at the rate phones sync during a sale's first hour, with the content interaction feed on the same
brokers, does every impression and dismiss beacon reach every ad server within 10 ms at p99, on Apache Iggy and on
the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~170 B | User, beacon type, campaign or advertiser, cap window, collector, sequence, running total, timestamps. |
| Rate | 50K → 200K → 500K → 1M beacons/s | 300M+ devices and 150 unlocks per user per day; the share that carries ads is yours to set. |
| Partitions | 32, keyed by user | A user's beacons apply in order on every reader. |
| Readers | 20 → 100 → 300 ad servers, each reading every partition | The fan-out is the lane: any ad server can take a phone's next refill. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Impressions are revenue events and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 5× for 10 min | A sale's first hour or a big match: every phone unlocks at once. |
| Neighbour | 100 MB/s of content interaction events on a separate topic, same brokers | The heavy analytics feed must not move the lane's tail. |
| Restart | Restart 10% of ad servers at minute 10 | Time to rebuild every user's view from the log, and the next minute's p99. |
| Failover | Kill the leader at minute 15 | Time until every ad server receives again. |
| Duration | 30 min per step + 24 h soak | A full daily cap window; drift shows over hours. |
| Hardware | 3 brokers each side, same machine type, on GCP in your region | Like for like; nodes and cost per billion beacons reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows stale placements next to p99.

**Pass line:** beacon-to-ad-server p99 under 10 ms at 500K beacons/s with 100 ad servers, replicated, through the
5× peak and with the neighbour running; stale placements under 0.01% of ads placed.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), the peak trace, restart and
failover recovery, broker CPU, node count and cost per billion beacons, and every config and raw output.
