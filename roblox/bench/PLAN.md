# Benchmark plan: the Roblox server state lane

**Question:** through a creator update and a 10× thundering herd of joins, does every game server change
(players, capacity, open or shutting down) reach every matchmaker's index within 10 ms at p99, so no join is
sent to a server that is already full or closing, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 150 B | server, sequence, place, edge data center, capacity, players, status, placements seen, timestamp. |
| Rate | 20K → 50K → 100K → 200K server changes/s | Joins, leaves, friend joins, servers starting and closing, and heartbeats from every engine instance. |
| Partitions | 32, keyed by server | Each server's changes apply in order. |
| Readers | 8 matchmakers, each reading every partition | Every matchmaker holds every server. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A shutdown must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Herd | A creator update on the busiest place, then 10× joins into it for 10 min | Roblox's own "quick 10x jump in requests". |
| Neighbour | 50 MB/s of analytics events on a separate topic | A bulk stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until matchmakers receive again, and the next minute's p99. |
| Rebuild | Start a matchmaker for a new cell at minute 20 | Time to rebuild every live server from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows joins sent to full or closing servers next to p99.

**Pass line:** change-to-index p99 under 10 ms at 100K changes/s with eight matchmakers, replicated, through the
herd and with the analytics neighbour running; joins sent to a full or closing server near zero outside the
failover minute; a new matchmaker rebuilds every live server in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the herd trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
