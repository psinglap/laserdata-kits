# Benchmark plan: Tubi's creative readiness lane

**Question:** at live-event request rates, with a heavy ad event log on the same brokers, does every first
sighting of a new creative reach the creative registry, and every verdict reach every ad server node, within
10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~110 B sighting · ~140 B verdict | Node, creative, DSP, sequence, timestamp; creative, status, brand, category, timestamp. |
| Sightings | 1K → 10K → 50K/s, with waves of 5,000 new creatives at once | New flights start together at the top of a live event; every node meets them within seconds. |
| Partitions | 32 on sightings, keyed by creative · 1–4 on verdicts | One creative, one partition, one registry reader, one job. Every node reads every verdict. |
| Ad server nodes | 50 → 500 → 2,000 verdict readers | Tubi scaled Kubernetes from ~100 to 2,000 nodes for Super Bowl LIX; the fan-out has to hold as the fleet grows. |
| Verdict table | 100K → 1M creatives | A node added before a live event rebuilds its table from the log before it takes requests. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A lost verdict holds a paying creative back. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 10× sightings for 10 min | Kickoff of a live event, from baseline to millions of viewers in minutes. |
| Neighbour | 50 MB/s of ad event log (impressions, quartiles, errors) on a separate topic | A heavy reporting stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the registry and the ad servers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit
runs alongside at each rate, so the report shows jobs started, sightings absorbed, bids held, pod fill and RPM
next to p99.

**Pass line:** seen-to-registry p99 and verdict-to-ad-server p99 each under 10 ms at 10K sightings/s with 500
ad server nodes, replicated, through the 10× peak and with the neighbour running; exactly one job per new
creative; a node added mid-run holds the full verdict table within one second.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
rebuild time vs table size, broker CPU, node count and monthly cost for the lane, and every config and raw output.
