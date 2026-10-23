# Benchmark plan: live agent context from Apache Iggy through CocoIndex

**Question:** on one mirrored topic, does the CocoIndex flow on Apache Iggy keep an agent's context table as fresh
and as exact as the same flow on Kafka, through indexer crashes and bursts, at a broker cost no higher than the Kafka
cluster's share?

Rules are fixed before any result exists. If the Kafka path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Data | One keyed topic a CocoIndex app already reads, mirrored by `kafka_shadow.py` (or `review_events.py` at the same shape) | Same changes on both paths. |
| Flow | The same `process_*` function and Postgres table schema; `kafka.topic_as_map` on one path, `iggy.topic_as_map` on the other | Only the source differs. |
| Rate | Today's rate → 10× → 100× | Measured on the mirror first. |
| Deletes | The topic's own tombstones, as `{"_deleted": key}` on Iggy | Resolved items must leave the index. |
| Durability | Iggy `replicated` × 3 · Kafka with its production settings | The source must not lose what it acknowledged. |
| Faults | Indexer SIGKILLed every 10 min; Postgres paused 1 min; broker leader killed once per hour | What breaks live indexes. |
| Spike | 10× for 10 min | Bots and bulk edits. |
| Duration | 1 h per step + 24 h soak | Drift shows over hours. |
| Hardware | Iggy: 3 nodes; Kafka: your current cluster or the same instance family | Nodes and $ per month reported for both. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

`context_check.py` runs on both tables for the whole run and its final thread-by-thread check ends every step.
The broker alone is measured with `iggy-bench` and `kafka-producer-perf-test` on the same machines.

**Pass line:** missing 0, stale 0 and resolved-but-still-indexed 0 after every fault; change-to-index p99 under 1 s
at 10× today's rate; the index live again within 30 s of an indexer restart; broker cost per GB no higher than the
Kafka path.

**Published:** freshness p50/p99/p99.9 vs rate for both paths, stale agent answers, restart and replay traces, CPU on
the indexer and Postgres, node count and monthly cost of each broker, and every config and raw output.
