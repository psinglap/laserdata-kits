# Benchmark plan: the position share lane

**Question:** with whole fleets connected and a 72-hour replay running on the same brokers, does every position and
every shipment change reach the share gate, and every shared position reach the customer-facing readers, within
10 ms at p99 per hop, in order per vehicle, on Apache Iggy and on the same lane in Kafka (Strimzi on GKE)? And what
does the lane cost on each?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B position · ~250 B shipment change · ~200 B shared update | Vehicle, sequence, coordinates and timestamp; an assignment carries customer, pick-up, delivery and window. |
| Rate | 2,000 → 20,000 positions/s, 100,000 → 1,000,000 connected trucks | project44 reports 259,000+ active carriers and 1.5 billion+ shipments a year (press releases, 2026); position rates are not published, so the steps bracket them. |
| Partitions | 32 on positions, keyed by vehicle · 4–16 on shared updates | A truck's shipment changes and positions are applied in order. |
| Readers | Share-gate workers on positions; 3 → 30 readers on shared updates | Customer push, visibility screens, ETA, exception alerts, each from its own offset. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A shared position is a record of what a customer was shown. |
| Kafka tuning | `linger.ms=0`, Snappy as in production, tuned fetch settings, same GCP region and machine family | Kafka on its best latency settings, configured as project44 runs it. |
| Peak | 3× position rate for 30 min | Morning dispatch across North America and Europe. |
| Neighbour | A 72-hour replay at 50 MB/s on a separate topic, same brokers | project44's own Collaborative Visibility stress test rewound offsets by 72 hours (TechBlog, 2023). |
| Failover | Kill a broker at minute 15 | Time until the gate and the readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM (Tau T2A / Axion), same GCP region | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows leaks and misses next to p99.

**Pass line:** position-to-gate p99 and update-to-reader p99 each under 10 ms at 20,000 positions/s with 30 readers,
replicated, through the 3× peak and with the replay running; no position shared after its shipment's stop or
completion, none missed.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery, node CPU,
memory, disk and network, node count and monthly cost for the lane, and every config and raw output.
