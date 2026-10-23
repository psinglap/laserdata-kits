# Benchmark plan: a Ditto CDC Data Bridge lane on Apache Iggy

**Question:** with Big Peer change events at landing-wave rates, does every `documentChanged` event reach every
downstream mirror in per-document order within 10 ms at p99, with no broken change chain, on Apache Iggy and on
the same lane on Kafka as a Data Bridge uses it today?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event | Ditto CDC `documentChanged`, ~450 B JSON | txnId, version, cdcTimestamp, type, collection, change (oldValue and newValue). |
| Rate | 2K → 10K → 25K → 50K events/s | Ditto publishes no CDC volume; the steps bracket one busy collection to many. |
| Partitions | 12, keyed by `_id` | Ditto's default Data Bridge partition count; per-document order. |
| Readers | 3 downstream mirrors, each reading every partition at its own offset | Several systems on one bridge, as consumer group prefixes allow. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A change once published must not disappear. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Landing wave | 5× for 10 min, concentrated on 50 flights' documents | Devices reconnecting together and flushing offline edits. |
| Repeats | 0.5% of events delivered twice | Dedupe by txnId + `_id`, as Ditto's docs advise. |
| Neighbour | 50 MB/s bulk export on a separate topic | A bulk load must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until mirrors receive again, the next minute's p99, chain breaks. |
| Late mirror | Add a fourth mirror at minute 20 | Time to rebuild every document from the log, with no HTTP requery. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows chain breaks and repeats dropped next to p99.

**Pass line:** change-to-mirror p99 under 10 ms at 25K events/s with three mirrors, replicated, through the 5×
landing wave and with the export neighbour running; zero broken change chains outside the failover minute; a
new mirror rebuilds a 1M-document collection from the log in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the landing-wave trace, failover recovery, rebuild
time, broker CPU and memory, node count and monthly cost for the lane, and every config and raw output.
