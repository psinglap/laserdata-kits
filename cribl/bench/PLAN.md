# Benchmark plan: the SIEM outage lane behind a Cribl Stream Route

**Question:** with one Route's security events written once to one log and every Destination reading it from
its own offsets, does every healthy Destination get each event within 100 ms at p99 while the SIEM is down for
hours; when the SIEM is back, do fresh events reach it within budget at once while the held range drains
behind them; does a new SIEM backfill the retained log without slowing anyone; is anything lost; and what does
it cost per TB of Route output, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~400 B security event | Host, sourcetype, `_time`, `_raw`, per-host sequence. |
| Rate | 50K → 175K → 350K events/s | 175k EPS is the PQ sizing post's average (25 Worker Nodes, ~35 TB a day). |
| Writers | 25 Worker Nodes × 4–34 Worker Processes, one connection each, keyed by host | The post's deployment shape. |
| Destinations | SIEM, Cribl Lake, a new SIEM joining mid-run | Dual feeding during a migration, from Cribl's SIEM migration material. |
| SIEM outage | 14 hours, then fresh events first and the held range drained at 5% and at 25% of EPS | "Cribl can handle about 14 hours of Splunk downtime"; drain at ~5% of EPS (same post). |
| Worker loss | Kill one writer node mid-outage | Events already written are in the log, not on that node. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A routed event can't be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, manual assignment | Kafka on its best latency settings. |
| Failover | Kill the leader at hour 1 | Time until Destinations receive again, and the next minute's p99. |
| Hardware | 3 brokers each side, same AWS instance family and region, NVMe sized for 14 h at 175k EPS | Like for like. |
| Cost | Nodes, disk and monthly cost, next to 21 TB of Worker SSD for the same window | Reported as $ per TB of Route output, every reader included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's `workers.py` and `destination.py`; Kafka by the OpenMessaging
Benchmark with the same workload shape. The kit's per-second lines (live p99, waiting, drained, lost) are kept
next to the broker numbers at each step.
