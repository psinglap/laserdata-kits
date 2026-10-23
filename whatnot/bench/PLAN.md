# Benchmark plan: the story SLO lane

**Question:** at Whatnot's story volume and above, with every client and backend event for 30 core features keyed
by story key and the rest of client telemetry on the same brokers, does every story event reach the story judge,
and every SLO alert reach every consumer, within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B story event · ~300 B alert or SLI snapshot | Story key, story type, role, client, surface, timestamp. |
| Rate | 5K → 50K story events/s (300K → 3M a minute) | Whatnot reports "hundreds of thousands of story events each minute" in September 2026, growing by 1–2 SLOs a month; 10× headroom. |
| Partitions | 32 on story events, keyed by story key · 1 on alerts | Every event of a story reaches one judge; every consumer reads every alert. |
| Readers | Judge workers on story events; 3 → 20 consumers on alerts | Pager, Datadog exporter, incident bot, status page, team dashboards, each from its own offset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | SLO verdicts page people; outcomes cannot be lost. |
| Delivery | 1% of events delivered twice | The current pipeline delivers at least once; every story must still be judged once. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | 3× the base rate for 10 minutes, then a 10× join burst in one second | Whatnot saw nearly 3× its regular load on Super Bowl weekend 2024, and single shows two orders of magnitude above the median. |
| Neighbour | 200 MB/s of client telemetry on a separate topic, same brokers | Analytics volume must not move the story lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the judge and consumers receive again, stories wrongly failed at their deadline during it (target 0). |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows time-to-page and stories wrongly failed next to p99.

**Pass line:** event-to-judge p99 and alert-to-consumer p99 each under 10 ms at 50K story events/s with 20
consumers, replicated, through the join burst and with the neighbour running; no story judged failed whose last
outcome reached the broker before its deadline; every story judged once.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the burst trace, failover recovery, node
CPU and memory, node count and monthly cost for the lane, and every config and raw output.
