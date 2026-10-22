# Benchmark plan: the MPL player form lane

**Question:** at live-match peak rates, does every finished game (the player's rating and win or loss streak after
it) reach every matchmaker within 10 ms at p99, so no player who taps Play again is matched on a form that misses
their last game, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Result size | 120 B | player, sequence, rating, streak, games played, game, timestamp. |
| Rate | 5K → 10K → 25K → 50K results/s | Every head-to-head and tournament game ends in a result for each player. |
| Partitions | 32, keyed by player | Each player's results apply in order. |
| Readers | 8 matchmakers, each reading every partition | Every matchmaker holds every active player's form. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A result must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Live-match peak | 3× players online for 10 min | MPL's 600–700M events/day at peak against 200M on a normal day. |
| Neighbour | 50 MB/s of app and marketing events on a separate topic | A bulk stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until matchmakers receive again, and the next minute's p99. |
| Rebuild | Start a new matchmaker at minute 20 | Time to rebuild every active player's form from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows matches decided on a stale form and losing streaks missed next to p99.

**Pass line:** result-to-matchmaker p99 under 10 ms at 25K results/s with eight matchmakers, replicated, through
the peak and with the neighbour running; losing streaks missed near zero outside the failover minute; a new
matchmaker rebuilds every active player's form in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
