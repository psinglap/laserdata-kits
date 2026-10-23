# Benchmark plan: the Copart VB3 live lot lane

**Question:** with every yard's virtual sale lanes selling at once, does every lane change (lot opened, live bid
accepted with the countdown restarted, lot sold) reach every bidder-facing edge node within 10 ms at p99, and every
live bid reach the auction core within 10 ms at p99, so no member bids on a high bid that has already moved and no
bid loses to the countdown in transport, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 180 B | lane, lot, status, high bid, bidder region, estimated retail value, countdown deadline, bid id, sequence, timestamp. |
| Lanes | 400 → 1,000 → 2,500 sale lanes open at once | Every yard selling at once, across Copart's 11 countries (lane counts are assumptions; set them to VB3's). |
| Rate | 2K → 10K → 25K → 50K lane changes/s, the same in live bids | A live bid every few hundred ms on every busy lot, with a burst when many lanes open at the same minute. |
| Partitions | 32 for lot state, keyed by lane; 16 for live bids, keyed by lane | A lane's changes apply in order; busy yards spread across partitions. |
| Readers | 8 edge nodes, each reading every partition | Every edge holds every lane, so a member can follow any auction. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | An accepted bid and a sale must not come back. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Sale start | All lanes open within one minute | The moment a sale day starts. |
| Neighbour | 50 MB/s of member activity on a separate topic | Searches, lot views and watchlist adds must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until edges receive again, the next minute's p99, and bids that missed a countdown. |
| Rebuild | Start a new edge at minute 20 | Time to rebuild every lane from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows stale-screen bids (outbid on arrival, after the hammer) and bid-to-flag
time next to p99.

**Pass line:** state-to-edge and bid-to-core p99 under 10 ms at 25K lane changes/s with eight edges, replicated,
through a sale start and with the member-activity neighbour running; no live bid lost to the countdown in
transport; a new edge rebuilds every lane in under 10 seconds.

**Published:** p99 and p99.9 vs rate (one line per system), the sale-start trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
