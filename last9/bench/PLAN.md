# Benchmark plan: the streaming aggregation lane

**Question:** at Last9's sample rates, through a rollout's cardinality spike and with a heavy log stream on the
same brokers, does every raw sample reach the streaming aggregator that owns its shard key within 10 ms at p99
on Apache Iggy, with every timeslice exact against an independent count, no retry counted twice and no open
timeslice lost when an aggregator restarts or a partition moves, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B sample · ~120 B aggregate | Cluster, service, series id, le, sequence, cumulative value, scrape time. |
| Rate | 100K → 300K → 1M → 5M samples/s | 1M samples/s is the platform figure Last9 showed in May 2026; the step above is headroom. |
| Partitions | 64 on samples, keyed by (cluster, metric, shard_label) · 4 on aggregates | All data a rule needs sits with the aggregator that runs it. |
| Rule | `sum2 by (cluster, service, le)` over a histogram, 60 s timeslice, 2 s grace | The shape of a Last9 streaming aggregation; one metric per pipeline. |
| Cardinality | 1M input series per step, 2× during a rollout | The fold (series in to series out) is reported next to latency. |
| Retries | 0.5% of samples sent again up to 30 s later, plus counter resets | Counting once has to hold under at-least-once delivery; a late copy must not read as a reset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Samples are the customer's data. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | traffic × 10 for 5 min (100k to 300M a minute is the swing Last9 reported) | A spike, compressed. |
| Neighbour | 100 MB/s of raw logs on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Aggregator move | Stop one aggregator at minute 10; another takes its partitions and replays them from the log | Time to resume, and whether that timeslice is still exact. |
| Failover | Kill the broker leader at minute 15 | Time until aggregators receive again, the next timeslice's p99 and exactness. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows timeslices exact and retries absorbed next to p99.

**Pass line:** sample-to-aggregator p99 under 10 ms at 1M samples/s, replicated, through the peak and with the
neighbour running; every timeslice exact against the independent count; an aggregator move resumes within 5 s
with its open timeslice still exact.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, aggregator move and failover
recovery, timeslice checks, broker CPU, node count and monthly cost for the lane, and every config and raw output.
