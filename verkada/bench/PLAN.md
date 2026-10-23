# Benchmark plan: the camera partition alarm lane

**Question:** at a morning-opening arrival rate, with a heavy camera analytics stream on the same brokers, does
every camera detection, disarm, arm and panic press reach the alarm state service within 10 ms at p99, and every
raised alarm reach every responder within 10 ms at p99, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 150 B | Alarm partition, sequence, kind, camera or door, entry delay, timestamp. |
| Rate | 5K → 20K → 50K → 100K events/s | Detections from every camera in every camera partition, plus access events and arm/disarm commands. |
| Partitions | 32, keyed by alarm partition | A partition's detections, disarms and arms apply in the order they happened. |
| Readers | the alarm state service, plus 3 to 5 responders on the alarms topic | Monitoring agents, notifications, the response workflows, integrations, audit. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A disarm or an alarm cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Morning opening | 5× arrivals for 10 min | Sites opening at the same hour across a time zone. |
| Neighbour | 500 MB/s of camera analytics metadata on a separate topic | A heavy stream must not move the alarm lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the alarm state service and responders receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows false alarms from late disarms and missing alarms next to p99.

**Pass line:** event-to-state p99 under 10 ms and alarm-to-responder p99 under 10 ms at 50K events/s, replicated,
through the morning opening and with the neighbour running; no arrival that disarmed in time gets an alarm, and
no intrusion or panic alarm is missing.

**Published:** p99 and p99.9 vs rate (one line per system), the morning-opening trace, failover recovery, broker
CPU, node count and monthly cost for the lane, and every config and raw output.
