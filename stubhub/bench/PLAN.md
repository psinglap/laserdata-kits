# Benchmark plan: the StubHub seller listing lane

**Question:** at onsale rates, does every listing change (tickets left and price) reach every reader (event
pages, checkout, search) within 20 ms at p99, leaving most of a 100 ms budget for the services themselves, on
Apache Iggy and on the same lane in the Kafka-compatible platform you shortlist?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 140 B | event:listing, sequence, tickets left, price, kind, timestamp. |
| Rate | 20K → 100K → 200K → 400K changes/s | Hundreds of millions of tickets a year across thousands of events; onsales spike. |
| Partitions | 32, keyed by event:listing | Each listing's changes apply in order; a hot event spreads across partitions. |
| Readers | 3 consumer groups | Event pages, checkout, search. |
| Durability | Iggy `replicated` × 3 · candidate `acks=all`, RF 3, min ISR 2 | A sold listing must not come back. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | The candidate on its best latency settings. |
| Onsale | 10× for 10 min, half the changes on one event | A stadium tour or a final going on sale. |
| Neighbour | 50 MB/s clickstream on a separate topic | A heavy stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, the candidate by the OpenMessaging Benchmark, with the same workload shape. The
kit runs alongside at each rate, so the report shows `sold-before-checkout` next to p99.

**Pass line:** change-to-reader p99 under 20 ms at 200K changes/s with three readers, replicated, through the
10× onsale and with the clickstream neighbour running; `sold-before-checkout` under 0.1% of seat picks.

**Published:** p99 and p99.9 vs rate (one line per system), the onsale trace, failover recovery, broker CPU,
node count and monthly cost for the lane, and every config and raw output.
