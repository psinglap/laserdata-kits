# Benchmark plan: the Insights witness lane

**Question:** with Insights Agents uploading a batch every 5 s from every monitored pod, a busy hour and a
heavy agent-telemetry stream on the same brokers, does every upload batch reach witness_assembler within
10 ms at p99 after async_witnesses publishes it, with every batch accounted for, on Apache Iggy, and how does
the same hop compare on Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | 2–40 KB per upload batch (25–400 witnesses) | One message per agent upload, as async_witnesses receives it. |
| Rate | 1.7K → 5K → 15K → 50K witnesses/s | About 1B, 3B and 9B witnesses a week ("billions of requests a week on our dogfooding data alone", then customers), then headroom. |
| Agents | 300 → 3,000 → 30,000, one batch each every 5 s | Each monitored pod runs an agent; 278 pods were in one prod funnel check (agent PR #200). |
| Partitions | 32 on witness-batches, keyed by service | A service's batches, and so each agent's, apply in order. |
| Readers | witness_assembler, 2–8 connections, direct partition reads | The hop measured is asw publish → assembler receive. |
| Faults | 1% of uploads lost before the lane, 1% of batches published twice | The business check: every lost batch named, none counted twice. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Witnesses are what the endpoint views are built from. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 10 min | A busy hour, compressed. |
| Neighbour | 50 MB/s of agent telemetry on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until witness_assembler receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows missing batches named and duplicates absorbed next to p99.

**Pass line:** publish-to-assembler p99 under 10 ms at 15K witnesses/s, replicated, through the 3× peak and
with the neighbour running; every published batch assembled exactly once and every lost upload named.

**Published:** p99 and p99.9 vs rate for both systems, the peak trace, failover recovery, broker CPU, node
count and monthly cost for the lane, and every config and raw output. The same interval can be read from
Postman's own `asw_kafka_published` → `asm_kafka_received` timestamps for the Kafka side.
