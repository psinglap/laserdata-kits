# Benchmark plan: the captain location lane

**Question:** at Rapido's peak rates, does every on-duty captain ping reach every reader
(the matching index, the customer's pickup tracking, safety alerts, demand maps) within 20 ms
at p99 on the broker hop, leaving the rest of the budget to the mobile network and the
services, on Apache Iggy and on the same lane in Kafka as Rapido runs it today? And what does
the lane cost on each side?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Ping size | 120 B | captain ID, sequence, lat, lon, free or busy, ride ID, timestamp. |
| Rate | 10K → 50K → 150K → 300K pings/s | 15 lakh+ captains across 400+ cities (May 2026); the share on duty at the peak and the ping interval are inputs from Rapido. |
| Requests | 15,000 ride requests a minute, then 3× | Rapido's published matching-engine figure, and an office-hour peak on top. |
| Partitions | 32, keyed by captain ID | Each captain's pings apply in order. |
| Readers | 4 consumer groups | Matching index, pickup tracking, safety alerts, demand maps / analytics. |
| Retention | 6 hours | A ping matters for the shift and the ride it belongs to. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Like for like. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Peak | 3× ride requests for 20 min | Office and college hours: 4.7 lakh of Hyderabad's 9.1 lakh daily rides. |
| Neighbour | A 20× comms-event burst on another topic for 4 min | A city-wide campaign through the message transporter. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 Iggy nodes on GKE or GCE, same region as the Kafka cluster | Nodes and $ per month reported next to the Kafka bill for the same throughput. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload
shape. The kit runs alongside at each rate, so the report shows the share of offers computed
on a replaced ping next to p99.

**Pass line:** broker-hop p99 under 20 ms at 150K pings/s with four readers, replicated, through
the 3× peak and with the campaign burst running; offers two or more pings behind under 0.1%.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, the burst trace,
failover recovery, broker CPU, node count and monthly cost for the lane, and every config and
raw output.
