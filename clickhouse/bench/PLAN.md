# Benchmark plan: an Apache Iggy source for ClickPipes

**Question:** on one mirrored log topic, does an Iggy source deliver every event into a ClickHouse Cloud table
exactly once, through a ClickHouse outage, a source crash and lost acknowledgements, with rows queryable within 1 s
at p99, at a cost per GB ingested no higher than the Kafka ClickPipe reading the same topic today?

Rules are fixed before any result exists. If the Kafka path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Data | One topic your ClickPipe already reads, mirrored by `kafka_shadow.py` (or collectors.py at the same shape) | Same rows on both paths. |
| Message size | Your topic's own (~0.3–1.6 KB per log event) | ClickPipes' own throughput figures use 1.6 KB messages. |
| Rate | Today's rate → 10× → 100× | Measured on the mirror first. |
| Target | A scratch database in the same ClickHouse Cloud service, same table engine and ORDER BY on both paths | Like for like on the ClickHouse side. |
| Semantics | Kafka ClickPipe with exactly-once on · Iggy source with the rules in this kit | Exactly-once against exactly-once. |
| Insert interval | 0.5 s and 5 s on the Iggy source; the ClickPipe's defaults | Freshness vs insert rate, both reported. |
| Durability | Iggy `replicated` × 3 · Kafka with its production settings | The source must not lose what it acknowledged. |
| Faults | ClickHouse service paused 1 min and 30 min; source killed (SIGKILL) every 10 min; 1 in 20 acknowledgements dropped | Exactly-once under the faults that break pipes. |
| Spike | 10× for 10 min | Deploy waves and incidents. |
| Duration | 1 h per step + 24 h soak | Merges, parts and drift show over hours. |
| Hardware | Iggy: 3 nodes; Kafka: your current cluster or the same instance family | Nodes and $ per month reported for both. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` and the kit, the Kafka path by your producers or `kafka-producer-perf-test` and the
same ClickPipe. `verify.py` runs at the end of every step on both target tables.

**Pass line:** zero missing rows, zero duplicates and zero per-source sequence gaps after every fault; event-to-
ClickHouse p99 under 1 s at 10× today's rate with a 0.5 s insert interval; drain after a 30 min outage at least 5×
faster than real time; source infrastructure cost per GB no higher than the Kafka path.

**Published:** freshness p50/p99/p99.9 vs rate for both paths, the outage and crash traces, drain time, parts
created per minute, CPU on the ClickHouse service, node count and monthly cost of each source, and every config and
raw output.
