# Benchmark plan: the link health alert lane

**Question:** at storm fade rates, with a slow consumer on the same topic and heavy log shipping on the same
brokers, does every link sample reach the rules, and every alert reach every consumer, within 10 ms at p99 per
hop, on Apache Iggy and on the same lane in Redpanda?

Rules are fixed before any result exists. If Redpanda wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~120 B link sample · ~200 B alert | Link ID, sequence, RSSI, SNR, BER, timestamp. |
| Rate | 1K → 10K links, a sample every 20 ms each (50K → 500K samples/s) | Jump has published "millions of messages per second across thousands of nodes" for its telemetry pipeline; link count and sample interval are placeholders until Jump's own are known. |
| Partitions | 32 on samples, keyed by link · 1–4 on alerts | A link's samples are evaluated in order against its own baseline; every consumer reads every alert. |
| Readers | Rule workers on samples; 3 → 30 consumers on alerts; 1 → 5 slow readers on samples | Console, on-call, status board and anything else, each from its own offset; slow readers stand in for research capture and backfills. |
| Durability | Iggy `replicated` × 3 · Redpanda `acks=all`, RF 3 | Jump has said it needs assurance of zero data loss. |
| Redpanda tuning | `linger.ms=0`, tuned fetch settings, same hardware | Redpanda on its best latency settings, on Jump's own configuration where Jump shares it. |
| Peak | 10× fade rate for 10 min | Weather crossing many links at once. |
| Neighbour | 500 MB/s of log shipping on a separate topic, same brokers | Telemetry and log files share the platform; logs must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the rules and consumers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, bare metal or Jump's cloud instances, same site | Like for like; nodes, memory and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Redpanda by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows degraded alerts delivered while the link was still
degraded next to p99.

**Pass line:** sample-to-rules p99 and alert-to-consumer p99 each under 10 ms at 10K links with 30 consumers,
replicated, through the 10× peak, with the slow readers and the log neighbour running; every degraded alert
delivered while its link is still degraded, with Jump's real fade durations.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, slow-reader isolation,
failover recovery, node CPU and memory, node count and cost for the lane, and every config and raw output.
