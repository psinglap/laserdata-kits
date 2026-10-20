# Benchmark plan: the Currents connector lane, one topic per connector

**Question:** with one topic per Currents connector, does every event of every healthy connector
reach its connector within 50 ms at p99 while one partner is down for 30 minutes and another
throttles, with the event firehose running next door; does the stalled connector catch up from its
own offset with nothing lost; how many connector topics can one cluster carry before that stops
being true; and what does the lane cost per billion exported events, on Apache Iggy and on the
same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B engagement event | Event id, app, user ID, event type, per-user sequence, timestamp. |
| Connectors | 1,000 → 5,000 → 10,000 topics, long-tail app sizes, the largest 5% with 4 partitions | One topic per integration, as in Braze's export talk; Kafka gets the same layout. |
| Export rate | 100K → 250K → 500K events/s after fan-out | "Tens of billions of messages daily" through Currents (Braze job post) is roughly 120K–580K events/s on average. |
| Readers | Connector shards with direct partition reads, per-connector partner rate limits | Each connector keeps its own offset; no consumer-group rebalances. |
| Partner outage | 1% of connectors down for 30 min, then drain at their rate limit | Partner downtime and the July 2026 EU02 backlog (up to ~5% of connectors) in Braze's own words. |
| Firehose next door | 250 MB/s on the event-type topics | Every raw event on its event-type topic, on the same nodes. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | An exported event cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`; consumer groups and manual assignment both tried | Kafka on its best latency settings. |
| Failover | Kill the leader at minute 15 | Time until connectors receive again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift and open-file growth show over hours. |
| Hardware | 3 brokers each side, same AWS instance family and region | Like for like. |
| Cost | Nodes, broker CPU, open files and monthly cost for the lane | Reported as $ per billion exported events, every reader included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's connector shards, Kafka by the OpenMessaging Benchmark
with the same workload shape. The kit runs alongside at each step, so the report shows connectors
behind budget, events lost and catch-up time next to p99.

**Pass line:** p99 under 50 ms for every healthy connector at 250K events/s across 5,000 connector
topics, replicated, through the outages and with the firehose next door; every stalled connector
catches up from its own offset with zero events lost; and a lower $ per billion exported events
than the same lane on Kafka.

**Published:** p99 and p99.9 vs rate and vs connector count (one line per system), the outage and
catch-up traces, failover recovery, broker CPU, open files, node count, monthly cost and $ per
billion events, and every config and raw output.
