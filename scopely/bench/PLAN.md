# Benchmark plan: the MONOPOLY GO! race lane

**Question:** from a race start, when players arrive from zero and the game's load triples in about five
minutes, with client analytics on the same brokers, does every dice roll reach the race owner that holds its
race, and every lap, finish and race end reach every game server, within 10 ms at p99 per hop, on Apache Iggy
and on the same lane in Apache Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~110 B roll · ~120 B race update | Race, race number, team, player, server, roll sequence, the player's race points, timestamp. |
| Rate | 20K → 100K → 300K rolls/s | MONOPOLY GO! peaks above a million concurrent players (re:Invent 2025); the race share of that load is yours to set. |
| Ramp | zero to the full rate in 5 minutes, at a set start time | "we triple our baseline in a short period, like five minutes" when the race mini game starts. |
| Partitions | 64 on race-rolls, keyed by race · 4–8 on race-updates | One owner per race, rolls in one order; every game server reads every update. |
| Readers | Race owners on race-rolls; 100 → 2,000 game servers on race-updates | The game runs on 2,000+ hosts at peak; the fan-out back to them is the second hop. |
| Retries | 0.5% of rolls sent twice | The server gives up fast on a throttled write and the client retries. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A finish decides medals and cannot be lost. |
| Neighbour | 200 MB/s of client analytics on a separate topic, same brokers | The 12–15 billion events a day for reporting must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until owners and game servers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region (Graviton included) | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows players who rolled into a decided race next to p99.

**Pass line:** roll-to-race-owner p99 and update-to-game-server p99 each under 10 ms at 100K rolls/s with 2,000
game servers, replicated, through the ramp and with the neighbour running; every place matching the race rules;
retried rolls counted once.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the ramp trace, failover recovery, node
CPU, node count and monthly cost for the lane, and every config and raw output.
