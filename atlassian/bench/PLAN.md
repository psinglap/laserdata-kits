# Benchmark plan: the Forge usage alerts lane

**Question:** at Forge's usage volume and its growth, with a heavy product-analytics stream on the same
brokers, does every usage record reach its app's allowance check, and every crossing reach the alert
service, within 10 ms at p99 per hop, counted once, on Apache Iggy, and how does the same lane compare on
Kafka (MSK)?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B usage record · ~150 B alert | Host, app, cycle, sequence, usageUniqueId, running GB-seconds, timestamp. |
| Rate | 3.5K → 7K → 12K → 50K usage records/s | About 300M, 600M and 1B usage events a day (Atlassian's figures for today, six months and a year out), then headroom. |
| Partitions | 32 on usage, keyed by app · 1–4 on alerts | An app's usage applies in order; the alert service reads every crossing. |
| Readers | The tracker on usage (2–8 connections, direct partition reads); the alert service on alerts | Two hops, measured separately and end to end. |
| Duplicates | 1% of records sent twice | Retries happen; the business check is that none is counted twice. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Usage feeds alerts developers act on; it cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 10 min | A busy hour, compressed. |
| Neighbour | 50 MB/s of product analytics events on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the tracker and the alert service receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows retries absorbed and early alerts avoided next to p99.

**Pass line:** usage-to-tracker p99 and tracker-to-alert p99 each under 10 ms at 12K usage records/s,
replicated, through the 3× peak and with the neighbour running; zero usage counted twice.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
