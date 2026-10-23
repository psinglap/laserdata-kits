# Benchmark plan: the Data Connector stream lane, one topic per stream subscription

**Question:** with one topic per stream subscription, does every datapoint of every healthy
subscription reach its connector within 50 ms at p99 while 1% of customer brokers are unreachable
for an hour and others throttle, with raw device uploads running next door; does each stalled
subscription backfill from its last streamed datapoint with nothing lost and in order per vehicle;
how many subscription topics can one cluster carry before that stops being true; and what does the
lane cost per billion streamed datapoints, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Datapoint size | 250 B | Datapoint id, org, vehicle ID, entity, per-vehicle sequence, timestamp, a reading or event type. |
| Subscriptions | 2,000 → 10,000 → 25,000 topics, long-tail fleet sizes, the largest 5% with 4 partitions | One topic per stream subscription, one entity each, as the Kafka Connector reference describes; Kafka gets the same layout. |
| Stream rate | 100K → 300K → 600K datapoints/s | GPS every 5 s for a vehicle that is on (Kafka Connector reference); 2.3M+ devices (Samsara job posts), of which only subscribed fleets stream. |
| Readers | Connector shards with direct partition reads, a write limit per destination | Each subscription keeps its own offset; no consumer-group rebalances. |
| Broker outages | 1% of subscriptions unreachable for 60 min, then backfill at their limit | "Data recovery: up to 6 hours", resuming "from the last streamed datapoint". |
| Retention | 6 h message expiry per subscription topic, plus a 6 h soak at the 6 h edge | The documented recovery window must hold on disk. |
| Ingest next door | 250 MB/s of raw uploads on the same nodes | Ingestion "handling gigabytes/sec of streaming data" (Samsara job post), scaled to the lane's cluster. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A datapoint a customer expects cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`; consumer groups and manual assignment both tried | Kafka on its best latency settings. |
| Failover | Kill the leader at minute 15 | Time until connectors stream again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift, expiry and open-file growth show over hours. |
| Hardware | 3 brokers each side, same AWS instance family and region (ARM and x86) | Like for like; the map's Mobility KPI asks for ARM. |
| Cost | Nodes, broker CPU, disk for 6 h of retention, open files and monthly cost for the lane | Reported as $ per billion streamed datapoints, every reader included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's connector shards, Kafka by the OpenMessaging Benchmark
with the same workload shape. The kit runs alongside at each step, so the report shows
subscriptions behind budget, datapoints lost and backfill time next to p99.

**Pass line:** p99 under 50 ms for every healthy subscription at 300K datapoints/s across 10,000
subscription topics, replicated, through the outages and with ingest traffic next door; every
stalled subscription backfills from its last streamed datapoint with zero datapoints lost; and a
lower $ per billion streamed datapoints than the same lane on Kafka.

**Published:** p99 and p99.9 vs rate and vs subscription count (one line per system), the outage
and backfill traces, failover recovery, broker CPU, open files, disk, node count, monthly cost and
$ per billion datapoints, and every config and raw output.
