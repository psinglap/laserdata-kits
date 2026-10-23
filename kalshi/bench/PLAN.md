# Benchmark plan: the Kalshi cross-shard order group lane

**Question:** at sports-peak fill rates, does every fill in an order group, from every matching engine shard,
reach the order group tracker within 5 ms at p99, and does every trigger reach every shard within 5 ms at p99,
on Apache Iggy and on the same lane on Kafka? And how many contracts fill past a group's trigger because of
the time the loop takes?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~170 B fill · ~110 B group state | Shard, order group, market, contracts, per-(shard, group) sequence, running total, timestamp. |
| Rate | 8K → 25K → 50K → 100K fills/s in order groups | Kit default up to several times it; Kalshi's real rate is Kalshi's to set. |
| Order groups | 20,000 → 100,000 active | Kalshi allows up to 100,000 order groups per user (changelog, Aug 2026). |
| Partitions | 32 on fills, keyed by order group · 1–4 on states | A group's fills from every shard land in one partition; every shard reads every state. |
| Shards | 4 → 8 matching engines, each reading the states topic | Kalshi adds shards incrementally; the second hop has to hold as it does. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A trigger is a risk control and must not be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Burst | 4× for 10 min | The final minutes of a full NFL Sunday slate, compressed. |
| Neighbour | 50 MB/s of public trades and book changes for the data lake, separate topic, same brokers | A bulk export must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the tracker and shards receive again; contracts past the trigger in that minute. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate (with Go or Rust readers in place of the Python ones), so the report shows contracts filled
past the trigger next to p99.

**Pass line:** fill-to-tracker p99 and state-to-shard p99 each under 5 ms at 50K fills/s with 8 shards,
replicated, through the 4× burst and with the neighbour running; contracts filled past the trigger under 0.01% of
contracts in order groups.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the burst trace, failover recovery,
contracts past the trigger, broker CPU, node count and monthly cost for the lane, and every config and raw output.
