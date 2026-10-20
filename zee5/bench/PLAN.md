# Benchmark plan: the Continue Watching heartbeat lane

**Question:** at ZEE5's evening-peak rates, does every playback heartbeat reach the Continue
Watching service (and the other readers of the heartbeat topic) within 20 ms at p99 on the
broker hop, on Apache Iggy and on the same lane as ZEE5 runs it today on Confluent Cloud? What
does the lane cost on each side at one heartbeat a minute, and at one every 10 or 15 seconds?

Rules are fixed before any result exists. If Confluent Cloud wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Heartbeat size | ~150 B | viewer ID, sequence, event, content ID, position, device, timestamp. ZEE5's own record shape replaces this in week 1. |
| Rate | 17K → 100K → 200K heartbeats/s | ~1M daily peak concurrency (ZEE5 heartbeat talk, Feb 2026 write-up) at one heartbeat per 60 s, 10 s and 5 s. 100B+ a day is ~1.2M/s on average across all heartbeat uses; the share that feeds Continue Watching is an input from ZEE5. |
| Partitions | 32, keyed by viewer ID | Each viewer's heartbeats apply in order. |
| Readers | 4 consumer groups | Continue Watching, concurrent viewership, recommendations, a ScyllaDB connector. |
| Retention | 24 hours | Long enough to rebuild any reader by replay; ScyllaDB keeps the durable state. |
| Durability | Iggy `replicated` × 3 · Confluent Cloud `acks=all` as configured today | Like for like on acknowledged writes. |
| Peak | 3× viewers for 20 min | A big release or the evening peak. |
| Neighbour | A 20× content-metadata burst on another topic for 4 min | The CMS publishing over the same platform when a big title drops with every language and subtitle. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 Iggy nodes in the cloud region ZEE5 runs the heartbeat platform in | Nodes and $ per month reported next to the Confluent Cloud bill for the same throughput, retention and number of readers. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Confluent Cloud by a producer and consumer groups in the same
region, with the same workload shape. The kit runs alongside at each rate, so the report shows
resumes at the stop point and rewatch seconds next to p99.

**Pass line:** broker-hop p99 under 20 ms at 100K heartbeats/s with four readers, replicated,
through the 3× peak and with the metadata burst running; no resume answered from a heartbeat the
lane had not delivered.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, the burst trace,
failover recovery, broker CPU, node count and monthly cost for the lane at each heartbeat
interval, and every config and raw output.
