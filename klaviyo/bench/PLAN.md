# Benchmark plan: the failed-writes lane for the Event Publisher

**Question:** with Apache Iggy as the separate failed-writes cluster, does every failed publish get
captured with a p99 ack under 10 ms and reach a retrier within 50 ms at p99, while half of all events
fail for 15 minutes at Klaviyo's peak; are P10 events still re-published inside their 5 s SLO counted
from the first failure; is every captured event re-published or in the DLQ with nothing lost; how many
retriers does that surge need; and what does the lane cost to keep ready, on Apache Iggy and on the same
lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~600 B event with properties | Profile, lane, metric name, ids, first-failure time, per-partition sequence. |
| Normal failure rate | 0.1% of 170K events/s | Transient Kafka timeouts; "roughly 20,000" automatic re-publishes in three months is far lower, so this is generous. |
| Surge | 50% of 170K → 250K events/s for 15 min | "half of all events for 10–20 minutes" (Klaviyo, Oct 2025); 250K/s is the BFCM 2025 peak hour average (900M events/hour). |
| Partitions | 16 → 64 on failed-writes, keyed by profile | One profile's events stay in order; Kafka gets the same layout. |
| Retriers | 2 → 16 shards with direct partition reads, 2% of re-publishes failing again, 5 attempts then DLQ | The failed-writes processor is the bottleneck Klaviyo names. |
| Bulk replay next door | 100 MB/s of recovered S3 events on the same nodes | Replays after a serialization bug is fixed. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A captured event cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`; consumer groups and manual assignment both tried | Kafka on its best latency settings. |
| Failover | Kill the leader at minute 7 of the surge | Time until capture writes succeed again, and the next minute's p99. |
| Duration | Idle hour, 15 min surge, idle hour; + 24 h soak | The cluster is idle most of the time and must be ready. |
| Hardware | 3 nodes each side, same AWS instance family and region | Like for like. |
| Cost | Nodes, broker CPU and memory at idle and in the surge, monthly cost | Reported as monthly $ to keep the lane ready, and $ per billion captured events. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's publisher and retriers, Kafka by the OpenMessaging
Benchmark with the same workload shape. The kit runs alongside at each step, so the report shows
on-time rate per lane, DLQ count and events lost next to p99.

**Pass line:** capture ack p99 under 10 ms and capture-to-retry p99 under 50 ms through the 15-minute
surge, replicated, with the bulk replay next door; P10 on-time rate ≥ 99.9%; every captured event
re-published or in the DLQ, zero lost; and a lower monthly cost to keep the lane ready than the same lane
on Kafka.

**Published:** p99 and p99.9 vs surge rate (one line per system), retriers needed per surge rate, the
surge and failover traces, broker CPU and memory at idle and peak, node count, monthly cost and $ per
billion events, and every config and raw output.
