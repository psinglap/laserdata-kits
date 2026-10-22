# Benchmark plan: the fuel alert lane

**Question:** at the ping rates of BlackBuck's telematics fleet, with low-network history uploads on the same
brokers, does every fuel-sensor ping reach the fuel rules, and every drain alert reach every notifier, within
10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B ping · ~200 B alert | Device, sequence, fuel litres, ignition, speed, position, timestamp. |
| Rate | 50K → 500K devices, a ping every 10 s each (5K → 50K pings/s) | BlackBuck reports 500K+ average monthly active devices in FY26; the fuel-sensor share and the ping interval are set to yours. |
| Partitions | 32 on fuel pings, keyed by device · 1–4 on alerts | A truck's pings are evaluated in order; every notifier reads every alert. |
| Readers | Fuel-rule workers on pings; 3 → 30 notifiers on alerts | Owner app, fleet dashboard, SMS and support tools, each from its own offset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A drain alert is a record owners act on and dispute; it cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | 5× drains for 10 min | Night halts, when most trucks are parked at roadside stops. |
| Neighbour | 500 MB/s of buffered history pings on a separate topic, same brokers | Devices coming back from low network must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the rules and notifiers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows drain alerts delivered while the fuel was still being
drained, and the litres still in the tank, next to p99.

**Pass line:** ping-to-rules p99 and alert-to-notifier p99 each under 10 ms at 500K devices with 30 notifiers,
replicated, through the 5× peak and with the neighbour running; no false drain alerts on your real sensor noise.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU and memory, node count and monthly cost for the lane, and every config and raw output.
