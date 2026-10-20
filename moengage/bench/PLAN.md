# Benchmark plan: the Smart Trigger lane, one topic per workspace

**Question:** with one topic per workspace, does every trigger event of every brand inside its
quota reach the trigger evaluator within 50 ms at p99, while one brand runs a 10× flash sale and
the ingestion firehose runs next door; how many workspace topics can one cluster carry before that
stops being true; and what does the lane cost per billion events, on Apache Iggy and on the same
lane in self-run Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B trigger event | Workspace, user ID, event name, per-user sequence, timestamp. |
| Workspaces | 1,000 → 5,000 → 10,000 topics, long-tail sizes, the largest 5% with 4 partitions | "Thousands of customers" (MoEngage ingestion post); Kafka gets the same layout, and also the shared-topic layout MoEngage runs today. |
| Trigger rate | 25K → 100K → 250K events/s | 250K events/s is MoEngage's published ingestion peak; trigger events are a share of it. |
| Readers | Evaluator shards with direct partition reads and per-workspace quotas | The trigger evaluator, sharded by workspace. |
| Flash sale | One workspace at 10× its quota for 10 min, every 30 min | One brand's spike, the case MoEngage's reliability post describes. |
| Ingestion next door | 250 MB/s on the unified topic | Enriched events for every streaming job, on the same nodes. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A trigger event cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, KRaft and ZooKeeper builds | Kafka on its best latency settings. |
| Failover | Kill the leader at minute 15 | Time until evaluators receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift and open-file growth show over hours. |
| Hardware | 3 brokers each side, same AWS instance family (c6g/c7g) and region | Like for like with MoEngage's own broker choice. |
| Cost | Nodes, broker CPU, open files and monthly cost for the lane | Reported as $ per billion trigger events, every reader included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's evaluator, Kafka by the OpenMessaging Benchmark with
the same workload shape. The kit runs alongside at each step, so the report shows the share of
triggers fired within 1 s and the flash-sale backlog next to p99.

**Pass line:** p99 under 50 ms for every workspace inside its quota at 100K events/s across 5,000
workspace topics, replicated, through the flash sale and with ingestion running next door; the
flash-sale brand's backlog clears at its quota; and a lower $ per billion events than the same lane
on Kafka.

**Published:** p99 and p99.9 vs rate and vs workspace count (one line per system), the flash-sale
trace, failover recovery, broker CPU, open files, node count, monthly cost and $ per billion
events, and every config and raw output.
