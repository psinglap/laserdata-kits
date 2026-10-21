# Benchmark plan: the live events lane

**Question:** with every captured event on a lane keyed by project, Live views opening and closing across
several livestream pods, and one project's launch at 10× its traffic, does each watched event reach the pod
serving its view within 100 ms at p99, on Apache Iggy and on the same lane in Kafka, with no watched event
missed, and how much of the firehose does each pod still read?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B per record | Token, sequence, distinct_id, event name, path, capture time. The full event still goes to the events topic and ClickHouse. |
| Rate | One region's live event rate, then 2× | PostHog has not published the firehose rate; livestream's Redis path filled its queue at ~20K events/s in March 2026 (PR #50877), so the floor is above that. |
| Projects | Your project count and traffic skew, from one hour of the events topic | Skew decides how much of the firehose a pod reads for its viewers. |
| Partitions | 256 → 1,024 on the lane, keyed by project token | More partitions mean each pod reads fewer projects it doesn't need; the cost is more partitions to poll. |
| Readers | 3 → 10 livestream pods, each with its own viewers | Each pod reads only its viewers' partitions directly; no consumer group. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | Like for like. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, `client.rack` set, same region and instance family | Kafka on its best latency settings, on its own small cluster, not the main MSK cluster. |
| Launch | One project at 10× for 10 min | Live is used for launches; one partition takes the spike. |
| Churn | Views opening and closing every second across pods | Pods start and stop partitions and fill new views from the log. |
| Failover | Kill a broker at minute 15 | Time until views receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, ARM and x86, same region | Nodes, memory, cross-AZ bytes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows missed and repeated events, users online and
view fill next to p99.

**Pass line:** capture-to-live p99 under 100 ms at 2× the region's rate, replicated, through the launch and the
churn; no watched event missed.

**Published:** p99 and p99.9 vs rate (one line per system), the launch and churn traces, failover recovery,
share of the firehose read per pod, node CPU and memory, node count and monthly cost for the lane, and every
config and raw output.
