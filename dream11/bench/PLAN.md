# Benchmark plan: the live points lane

**Question:** at Dream11's match-day shape, does every ball reach every points node within 10 ms
at p99 on the broker hop, through the toss surge and a wicket spike, with replication on, on
Apache Iggy and on the same hop over Kafka as Dream11 runs it today? And can a points node added
mid-match serve correct team totals within seconds?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One ball: match id, per-match sequence, over, event, resulting fantasy points of the players it touched (about 120 B JSON) | Totals, not deltas, so a repeat is harmless and a gap is visible. |
| Rate | Real time (a ball every 30–40 s per match) up to 200 balls/s across all live matches, plus non-cricket sports' events | 12 sports and 12,000 matches a year (Dream11 at Current Bengaluru 2025); the lane is small, the fan-out is wide. |
| Partitions | 4, 8 and 16, keyed by match id | Each match's balls stay in order. |
| Readers | 10, 50 and 200 points nodes, each reading every partition | Every node needs every ball; adding nodes must not slow or rebalance the others. |
| Retention | One match day | Rebuilds and replay of a match. |
| Durability | Iggy `replicated` × 3; Kafka as deployed today | Like for like. |
| Toss surge | Team saves and contest joins on a neighbouring topic at 28,000/s | "20000+ team updates per second", "8000+ contest joins per second" (IPL 2020). |
| Scale-out | Add 50% more nodes 15 minutes before the start and at a wicket | Time until each new node serves correct totals; p99 of the existing nodes meanwhile. |
| Failover | Kill the Iggy leader mid-innings | Time until nodes receive again, and the next minute's p99. |
| Duration | One evening (two T20 matches back to back) per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the instance family the Kafka brokers use, in the same AWS region | Nodes, vCPUs and $ per hour reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test`
with the same message size and reader count. The kit runs alongside at each step, so the report
shows `team totals updated`, `gaps` and `totals off` next to p99.

**Pass line:** ball-to-node p99 under 10 ms and p99.9 under 20 ms with 50 nodes, replicated,
through the toss surge; `gaps` and `totals off` 0; a node added mid-match serving correct totals
within 2 s.

**Published:** p99 and p99.9 vs node count (one line per system), the toss-surge trace, the
scale-out trace, failover recovery, broker CPU, node count and every config and raw output.
