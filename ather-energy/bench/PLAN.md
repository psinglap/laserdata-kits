# Benchmark plan: the rider alert lane

**Question:** at Ather's fleet size, with a reconnect storm and the full sensor stream on the same brokers, does
every fresh charging, parked-movement and crash event reach the alert service, and every alert reach every
notifier, within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B rider event · ~200 B alert | Scooter, sequence, kind, scooter clock, charging, state of charge, parked, position, timestamp. |
| Rate | 500K → 1M scooters, a rider-facing event every 100 s each (5K → 10K events/s) | Ather's annual report: past half a million vehicles on Indian roads. In 2022 Ather wrote that its applications process 3,000 to 5,000 messages per second for customer use cases. |
| Partitions | 32 on rider events, keyed by scooter · 32 on the reconnect backlog · 1–4 on alerts | A scooter's events are applied in order; every notifier reads every alert. |
| Readers | Alert service workers on rider events; ride history on the backlog; 3 → 30 notifiers on alerts | App push, the SMS gateway for crash alerts, the support console, partner integrations. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A crash alert cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | 3× rider events for 10 min | The evening commute and charging hour. |
| Reconnect storm | 5% → 20% of the fleet back online within 10 s, each flushing 10–30 minutes of buffered events | Ather wrote that a network drop and sudden reconnections put significant back-pressure on Kafka, once answered by scaling applications and data nodes by 10×. |
| Neighbour | 500 MB/s of full sensor packets on a separate topic, 3× during the storm, same brokers | The bulk stream must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the service and notifiers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM, same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows fresh alerts at the app on time, alerts pushed twice
and stale events pushed as news next to p99.

**Pass line:** event-to-service p99 and alert-to-notifier p99 each under 10 ms at 1M scooters with 30 notifiers,
replicated, through the peak, the storm and with the neighbour running; no alert pushed twice; no event older
than the fresh window pushed as news; the whole backlog filed to ride history.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the storm trace, failover recovery,
node CPU and memory, node count and monthly cost for the lane, and every config and raw output.
