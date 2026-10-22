# Benchmark plan: the 5G core probe record lane

**Question:** through a signalling storm, with RAN counters and logs on the same brokers, does every vProbe record
reach the anomaly check, and every NF health alert reach the workflow engine, within 20 ms at p99, on Apache Iggy and
on the same lane in your current transport?

Rules are fixed before any result exists. If the incumbent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~600 B SDR · 200 B alert | One record per procedure; instance, state, severity, score, sequence. Replace with your real SDR size. |
| Rate | 100K → 1M → 5M SDRs/s | A slice of ATOM's 50M records a second (2021), then a circle-sized share of it. |
| Partitions | 64 SDRs, 8 alerts, keyed by NF instance | Each instance's records in order; few partitions per broker. |
| Readers | Anomaly check on SDRs; workflow engine and NOC view on alerts | Each reading its own partitions directly. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 (if Kafka is the comparison) | At-least-once on both sides. |
| Incumbent tuning | Its best latency settings (Kafka: `linger.ms=0`, `fetch.max.wait.ms=0`, cooperative-sticky) | Same instance class, same disks. |
| Signalling storm | 3× for 15 min | A large site coming back and its devices re-registering at once. |
| Incidents | One every 5 min, a busy AMF failing 25% of procedures for 2 min | Each scored: detection window, flag to workflow, procedures given after the flag. |
| Neighbour | RAN counters and logs at 200 MB/s on a separate topic, same brokers | Bulk correlation feeds must not move the lane's tail. |
| Reader churn | Add and remove an anomaly-check instance at minute 10 | Time until the lane is back under budget. |
| Failover | Kill a node at minute 20 | Time until the workflow engine receives again, and the next minute's p99. |
| Duration | 30 min per step + one 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same standard servers, in your own data centre | Like for like, on-premises; nodes, cores, network and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka (if used) by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows `procedures given after the flag` next to p99.

**Pass line:** SDR-to-check and check-to-workflow p99 under 20 ms each at 1M SDRs a second, replicated, through the
3× storm with the neighbour running; zero procedures given to an instance after its flag.

**Published:** p99 and p99.9 vs rate (one line per system), the storm trace, every incident's timings, reader churn
and failover recovery, broker CPU, node count and cost for the lane, and every config and raw output.
