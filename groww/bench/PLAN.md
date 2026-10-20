# Benchmark plan: the position-update lane into Holdings

**Question:** at the 9:15 open, with a heavy DataPlatform stream on the same brokers, does every
fill's resulting position reach the Holdings service within 20 ms at p99, so the app's refresh about
500 ms after the fill always shows it, on Apache Iggy and on the same lane in your Strimzi Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | 200 B position update | User, instrument, sequence, net quantity, average price, order ID, timestamp. |
| Rate | 3K → 15K → 50K → 100K position updates/s | "Millions of trades executed daily", concentrated at the open; headroom for expiry days. |
| Partitions | 32, keyed by user ID | A user's fills apply in order; Holdings and its ETag are per user. |
| Readers | Holdings service, P&L, one analytics reader | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A position cannot be lost. (Today's order topic uses `acks=1` with a fallback to TR.) |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, Protobuf payloads | Kafka on its best latency settings, on the same GKE node type. |
| Market open | 5× for 15 min from 9:15 | The open is the Holdings peak: up to 1M requests a minute. |
| Neighbour | 50 MB/s on a separate topic, same brokers | A DataPlatform stream must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until Holdings receives again, and the next minute's p99. |
| Duration | 30 min per step + one full trading day soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same GKE machine series and Hyperdisk class | Like for like; nodes, cores and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `missing the new fill` next to p99.

**Pass line:** fill-to-holdings p99 under 20 ms at 15K position updates/s, replicated, through the
5× open with the neighbour running; post-trade refreshes missing the new fill under 0.01%.

**Published:** p99 and p99.9 vs rate (one line per system), the market-open trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
