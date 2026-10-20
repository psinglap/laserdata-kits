# Benchmark plan: the household frequency lane

**Question:** at live-sports break rates, with a heavy log-level feed on the same brokers, does every CTV
impression reach every ad server within 10 ms at p99, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~160 B | Household, campaign, cap window, ad server, sequence, running count, timestamp. |
| Rate | 100K → 300K → 1M → 2M impressions/s | Your CTV ad server takes millions of requests a second; the impression rate is yours to set. |
| Partitions | 32, keyed by household | A household's impressions apply in order on every reader. |
| Readers | 20 → 100 → 300 ad servers, each reading every partition | The fan-out is the lane: every server must see every impression. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Impressions are billable and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 5× for 10 min | A live-sports ad break: every household's slots at once. |
| Neighbour | 100 MB/s of log-level data on a separate topic, same brokers | A heavy reporting stream must not move the lane's tail. |
| Restart | Restart 10% of ad servers at minute 10 | Time to rebuild the cap view from the log, and the next minute's p99. |
| Failover | Kill the leader at minute 15 | Time until every ad server receives again. |
| Duration | 30 min per step + 24 h soak | A full daily cap window; drift shows over hours. |
| Hardware | 3 brokers each side, same machines, in your own data centre | Like for like; nodes and cost per billion impressions reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows over-cap impressions next to p99.

**Pass line:** exposure-to-ad-server p99 under 10 ms at 1M impressions/s with 100 ad servers, replicated,
through the 5× break and with the neighbour running; over-cap impressions under 0.01%.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), the break trace, restart
and failover recovery, broker CPU, node count and cost per billion impressions, and every config and raw output.
