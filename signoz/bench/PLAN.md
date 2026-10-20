# Benchmark plan: the OTLP ingest buffer

**Question:** at SigNoz Cloud-like rates, does every OTLP batch a collector accepts get acknowledged
within 10 ms at p99, through a reconnect burst and a ClickHouse stall, with nothing lost, on Apache
Iggy and on the same buffer in Kafka (the `signozkafkaexporter` / `signozkafkareceiver` path)?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Batch size | 100 log records, ~25 KB OTLP proto | A typical collector export batch. |
| Rate | 100 MB/s → 300 MB/s → 1 GB/s of OTLP | From a large self-hosted install to a Cloud cell. |
| Partitions | 64, keyed by tenant | Per-tenant order; big tenants spread, small ones share. |
| Writers | 1 consumer group, 4–16 writers, SigNoz's own ClickHouse exporters | Freshness measured with the real schema. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Accepted telemetry is not lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Burst | 10× for 5 min (agents reconnecting after an outage) | signoz#5956. |
| ClickHouse stall | Inserts refused for 5 min | A merge storm, restart or full disk. |
| Failover | Kill the leader at minute 15 | Time until collectors are acknowledged again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows records lost and receive-to-queryable next
to accept p99.

**Pass line:** collector accept p99 under 10 ms at 1 GB/s, replicated, through the 10× burst and the
ClickHouse stall; zero accepted records missing from ClickHouse after writer crashes and a leader
failover; writer memory bounded throughout.

**Published:** accept p99 and p99.9 vs rate (one line per system), the burst and stall traces,
drain time, failover recovery, broker CPU and memory, node count and monthly cost for the buffer,
and every config and raw output.
