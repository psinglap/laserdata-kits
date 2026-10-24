# Benchmark plan: the Nykaa ad counter lane

**Question:** at the rate shoppers load pages with sponsored slots in a Pink Friday first hour, with the
clickstream on the same brokers, does every sponsored ad impression and click reach every ad selector within
10 ms at p99, on Apache Iggy and on the same lane in your MSK?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~160 B | Shopper, event type, campaign, window, collector, sequence, running count, timestamps. |
| Rate | 20K → 50K → 100K → 250K ad events/s | 40M+ monthly active users (AWS case study); the share of page loads with sponsored slots is yours to set. |
| Partitions | 32, keyed by shopper | A shopper's events apply in order on every reader. |
| Readers | 10 → 50 → 150 ad selectors, each reading every partition | The fan-out is the lane: any selector can take a shopper's next ad request. |
| Durability | Iggy `replicated` × 3 · MSK `acks=all`, RF 3, min ISR 2 | Impressions and clicks are what brands are billed and reported on, and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same instance type. |
| Peak | 5× for 10 min | A sale's first hour: Pink Friday day 1 in 2022 drew 10M+ visits and 400+ orders a minute. |
| Neighbour | 100 MB/s of clickstream on a separate topic, same brokers | The heavy analytics feed must not move the lane's tail. |
| Restart | Restart 10% of selectors at minute 10 | Time to rebuild every shopper's counts from the log, and the next minute's p99. |
| Failover | Kill the leader at minute 15 | Time until every selector receives again. |
| Duration | 30 min per step + 24 h soak | A full daily window; drift shows over hours. |
| Hardware | 3 brokers each side, same EC2 instance type, in your AWS region (ap-south-1 if that is where MSK runs) | Like for like; nodes and cost per billion ad events reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows stale placements next to p99.

**Pass line:** event-to-selector p99 under 10 ms at 100K ad events/s with 50 selectors, replicated, through the
5× peak and with the neighbour running; stale placements under 0.01% of ads placed.

**Published:** p99 and p99.9 vs rate and vs reader count (one line per system), the peak trace, restart and
failover recovery, broker CPU, node count and cost per billion ad events, and every config and raw output.
