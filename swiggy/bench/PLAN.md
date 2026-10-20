# Benchmark plan: the delivery partner ping lane

**Question:** at dinner-rush rates, does every delivery partner ping reach every reader
(tracking screen, last-mile ETA, assignment, distance pay) within 20 ms at p99 on the broker
hop, leaving the rest of a sub-second budget to the mobile network and the services, on Apache
Iggy and on the same lane in Confluent Cloud? And what does the lane cost on each side?

Rules are fixed before any result exists. If Confluent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Ping size | 120 B | partner ID, sequence, lat, lon, order ID, timestamp. |
| Rate | 12K → 50K → 150K → 300K pings/s | 612,000 average monthly transacting partners (Q4 FY26); the share online at the peak and the ping interval are inputs from Swiggy. |
| Partitions | 32, keyed by partner ID | Each partner's pings apply in order. |
| Readers | 4 consumer groups | Tracking screen, last-mile ETA, assignment, distance pay / analytics. |
| Retention | 6 hours | A ping matters for the trip it belongs to. |
| Durability | Iggy `replicated` × 3 · Confluent Cloud `acks=all`, RF 3, min ISR 2 | Like for like. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Confluent on its best latency settings. |
| Rush | 3× partners online for 20 min | Friday dinner, rain, festival evenings. |
| Neighbour | A 100× retry storm on another topic for 4 min | The #BehindTheBug pattern: retries multiplied cluster ingress by 100. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 Iggy nodes, same region as the Confluent cluster | Nodes and $ per month reported next to the Confluent bill for the same throughput. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Confluent by the OpenMessaging Benchmark, with the same workload
shape. The kit runs alongside at each rate, so the report shows the share of ETAs computed on a
replaced ping next to p99.

**Pass line:** broker-hop p99 under 20 ms at 150K pings/s with four readers, replicated, through
the 3× rush and with the retry storm running; ETAs two or more pings behind under 0.1%.

**Published:** p99 and p99.9 vs rate (one line per system), the rush trace, the storm trace,
failover recovery, broker CPU, node count and monthly cost for the lane, and every config and
raw output.
