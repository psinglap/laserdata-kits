# Benchmark plan: the publish-to-site lane

**Question:** at The New York Times' publishing shape, does every new version of every asset reach every
serving node within 10 ms at p99 on the broker hop, through a tag change that republishes tens of thousands of
articles on the same lane, with replication on, on Apache Iggy and on the same hop over the Kafka path the
Times runs today? And can a serving node added before a big news night serve the latest version of every
asset within seconds?

Rules are fixed before any result exists. If the current path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One version of one top-level asset as the denormalized log carries it: URI, version, cause, the bundled asset (4 KB and 32 KB steps) | The whole asset, never a diff, so a repeat is harmless and a gap is visible. |
| Rate | Editor publishes, updates, corrections and live posts at a big news night's rate, plus republish bursts of 10,000 to 100,000 versions | "When a dependency changes, it republishes every top-level asset that references it" (Confluent blog, 2017). Actual rates to be taken from the denormalized log. |
| Partitions | 8, 16 and 32, keyed by top-level asset URI | The denormalized log is partitioned by top-level asset URI; per-asset order is all it needs. |
| Readers | 10, 50 and 200 serving nodes, each reading every partition | Every node serves every asset; adding nodes must not slow or rebalance the others. |
| Budget | One second from the editor's click to the site (the Times' SLA); the lane's share measured separately | "One second after the editor clicks publish, the latest version has to be available on the site" (Software Engineering Daily, 2017). |
| Retention | One news day on the lane | Rebuilds and replay. The Monolog in Kafka stays the archive and source of truth. |
| Durability | Iggy `replicated` x 3; the current path as deployed | Like for like. |
| Scale-out | Add 50% more serving nodes during a republish burst | Time until each new node serves the latest version of every asset; p99 of the existing nodes meanwhile. |
| Failover | Kill the Iggy leader during a burst | Time until nodes receive again, and the next minute's p99. |
| Duration | One news evening per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the Compute Engine machine family the Kafka brokers use, in the same Google Cloud region | Nodes, vCPUs and $ per hour reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test` with the
same message size and reader count. The kit runs alongside at each step, so the report shows `inside the 1 s
SLA`, `page views served the previous version` and `gaps` next to p99.

**Pass line:** publish-to-node p99 under 10 ms and p99.9 under 20 ms with 50 nodes, replicated, through a
50,000-article republish burst; every editor version inside the one-second SLA; `gaps` 0; a node added during
the burst serving the latest version of every asset within 2 s.

**Published:** p99 and p99.9 vs node count (one line per system), the burst trace, the scale-out trace,
failover recovery, broker CPU, node count and every config and raw output.
