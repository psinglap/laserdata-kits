# Benchmark plan: the match termination lane

**Question:** at an evening queue peak on one shard, with a heavy analytics-telemetry stream on the same brokers,
does every combat action reach the anti-cheat scorer, and every match termination reach the game server host that
runs the match, within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Apache Kafka (Amazon MSK)?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B combat action · ~150 B termination | Host, match, player, weapon, reaction time, snap, hit, headshot, sequence, timestamp. |
| Rate | 100K → 500K → 1M combat actions/s | A slice of the "millions of high-throughput game actions per second" the Anti-Cheat job post describes. |
| Partitions | 64 on combat-actions, keyed by match · 1–4 on match-terminations | A match's shots are judged in order; every host reads the terminations for its matches. |
| Readers | Scorer workers on combat-actions; 100 → 1,000 game server hosts on match-terminations | The second hop has to hold as the shard's fleet grows (108 matches a host in Riot's 128-tick post). |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A termination ends a ranked match and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms` low, same region | Kafka on its best latency settings. |
| Peak | 3× for 10 min | The evening queue on a busy shard, or a patch day. |
| Neighbour | 500 MB/s of analytics telemetry on a separate topic, same brokers | The analytics stream must not move the anti-cheat lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the scorer and hosts receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows aimbot shots landed after a termination next to p99.

**Pass line:** action-to-scorer p99 and termination-to-host p99 each under 10 ms at 500K combat actions/s with
1,000 hosts, replicated, through the 3× peak and with the neighbour running; under 1% of what a lane one second
behind would let land in a terminated match.
