# Benchmark plan: the boarding-point lane

**Question:** at redBus's festival-night peak, does every GPS packet reach the boarding-point tracker, and every
ENTRY, HALT and EXIT reach its readers (passenger alerts, booking-window extension, the delay feed, the live
tracking page), within 20 ms at p99 on the broker hop, on Apache Iggy and on the same lane as YourBus runs it today
on Kafka? How many hops does the chain need on each side, and what does the lane cost?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Packet size | ~120 B | SVC_DOJ, sequence, lat, lon, timestamp. |
| Rate | 350 → 1K → 3K → 10K packets/s | 20K packets a minute from 15K services (redBus tracking post, Feb 2022); higher steps cover more tracked services and shorter packet intervals. |
| Partitions | 16, keyed by SVC_DOJ | All events for a journey in order, as YourBus partitions today. |
| Readers | 4 | Boarding-point tracker, GPS mapper, booking extension, the live-tracking WebSocket API. |
| Fan-out | 0–10 passenger alerts per ENTRY | One per passenger boarding at that point. |
| Retention | 24 hours | Replay for simulation and debugging, as the post describes. |
| Durability | Iggy `replicated` × 3 · Kafka as configured today | Like for like on acknowledged writes. |
| Peak | 3× journeys tracked for 30 min | Festival-night departures (Diwali, Holi, long weekends). |
| Neighbour | 10× log burst on another topic for 5 min | 100K log messages a minute share the tracking Kafka cluster today. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 Iggy nodes on EC2 in the region YourBus runs in | Nodes and $ per month reported next to the Kafka cluster for the same throughput. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same producer and consumer shape in the same region. The kit runs
alongside at each rate, so the report shows boarding-point events from the wrong packet and passengers not alerted
next to p99.

**Pass line:** broker-hop p99 under 20 ms at 3K packets/s with four readers, replicated, through the 3× peak and
the log burst; zero boarding-point events missed, zero passengers not alerted.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, the burst trace, failover recovery,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
