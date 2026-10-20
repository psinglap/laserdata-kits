# Benchmark plan: the driver ping lane

**Question:** at Porter's peak rates, does every driver-partner ping reach every reader (the
live tracking links, the pickup and drop arrival checks, allocation, the PostGIS writers) within
20 ms at p99 on the broker hop, leaving the rest of the budget to the mobile network and the
services, on Apache Iggy and on the same lane as Porter runs it today on Cloud Pub/Sub? And what
does the lane cost on each side?

Rules are fixed before any result exists. If Pub/Sub wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Ping size | 130 B | driver ID, sequence, lat, lon, vehicle class, order ID, timestamp. |
| Rate | 5K → 20K → 60K → 120K pings/s | 3 lakh driver-partners a month (Inc42, Aug 2026); the share online at the peak and the ping interval are inputs from Porter. |
| Orders | 500,000+ a day nationally, 60% two-wheeler | The Ken, Sep 2025; the city split is an input from Porter. |
| Partitions | 32, keyed by driver ID | Each driver's pings apply in order. |
| Readers | 4 consumer groups | Tracking links, arrival checks, allocation, the PostGIS writer. |
| Retention | 6 hours | A ping matters for the trip it belongs to. |
| Durability | Iggy `replicated` × 3 · Pub/Sub as configured today | Like for like on acknowledged writes. |
| Peak | 3× drivers online for 20 min | A booking surge, as the Google Cloud story describes. |
| Neighbour | A 20× application-log burst on another topic for 4 min | The ~500 TB/month log stream, if it ever shared the broker. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 Iggy nodes on GKE or GCE, in the region Porter runs in | Nodes and $ per month reported next to the Pub/Sub bill for the same throughput and the same number of subscriptions. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Pub/Sub by a publisher and pull subscribers in the same region,
with the same workload shape. The kit runs alongside at each rate, so the report shows arrivals
fired late next to p99.

**Pass line:** broker-hop p99 under 20 ms at 60K pings/s with four readers, replicated, through
the 3× peak and with the log burst running; arrivals two or more pings late under 0.1%.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, the burst trace,
failover recovery, broker CPU, node count and monthly cost for the lane, and every config and
raw output.
