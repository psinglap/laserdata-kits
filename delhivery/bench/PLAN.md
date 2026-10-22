# Benchmark plan: the dock scan lane

**Question:** on a festive-peak shift, with change data capture streaming into the data lake on the same brokers,
does every plan revision and load scan reach the load check, and every misload alert reach every handheld and
screen, within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Amazon MSK?

Rules are fixed before any result exists. If MSK wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B scan or plan · ~200 B alert | Trip, sequence, bag, destination, loader, load count, timestamp; a plan carries its destination list. |
| Rate | 500 → 5,000 dock events/s, 1,000 → 10,000 docks loading at once | Delhivery shipped 6.2 million shipments on one day in September 2025 (NSE filing) and reports 10.4 million shipments a day of rated automated sort capacity (FY26 annual report). |
| Partitions | 32 on dock scans, keyed by trip · 1–4 on alerts | A trip's plan, loads and seal are checked in order; every handheld and screen reads every alert. |
| Readers | Load-check workers on dock scans; 3 → 30 readers on alerts | Loader handhelds, dock supervisors, the yard view and the control tower, each from its own offset. |
| Durability | Iggy `replicated` × 3 · MSK `acks=all`, RF 3 | A load scan is the record of what left on which trailer; it cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region (ap-south-1) and instance family | MSK on its best latency settings. |
| Peak | 3× scan rate for 30 min | A festive-season night at a mega-gateway. |
| Neighbour | 60k msgs/s of CDC changes on a separate topic, same brokers | Delhivery's data lake ingests 60k messages per second through CDC and MSK (AWS Storage Blog, 2024); a re-snapshot must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the load check and the handhelds receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Graviton), same region | Like for like; nodes, memory and ₹ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, MSK by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows misloads flagged before the loader's next scan next to p99.

**Pass line:** scan-to-check p99 and alert-to-handheld p99 each under 10 ms at 5,000 events/s with 30 readers,
replicated, through the 3× peak and with the CDC neighbour running; every misload flagged before the loader's next
scan, with your real scan intervals and plan revisions.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU and memory, node count and monthly cost for the lane, and every config and raw output.
