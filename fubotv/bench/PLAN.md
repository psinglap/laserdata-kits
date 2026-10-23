# Benchmark plan: the key-play alerts lane

**Question:** at Fubo's game-night shape, does every key play, clip-ready event and replay review reach Game
Alerts within 10 ms at p99 on the broker hop, through a first-pitch surge of viewing events next door, with
replication on, on Apache Iggy and on the same hop over the path Fubo runs today (Kafka or Pub/Sub)? And does a
restarted Game Alerts rebuild every live game and push nothing twice?

Rules are fixed before any result exists. If the current path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One game moment: game id, per-game sequence, kind (pitch, key, clip, review), play id, inning, resulting score; key plays add team and label, clips add clip id and length (about 150-250 B JSON) | The score so far, not a delta, so a repeat is harmless and a gap is visible; the clip event is what releases the alert. |
| Rate | Every pitch of every live game in real time, a full slate (15 games) and a double slate, up to 1,000 moments/s with replay of a game day | Game Alerts push "the biggest plays" for pro baseball, and a tap opens the play (Fubo, April 2026). The lane is small; what matters is that it is on time and complete. |
| Partitions | 4, 8 and 16, keyed by game id | Each game's pitches, key plays, clips and reviews stay in order. |
| Readers | Game Alerts, plus 2 and 10 further readers on the same lane (in-app key moments, Team Channels, analytics) | Each reader has its own position; adding one must not slow or rebalance Game Alerts. |
| Retention | One game day | Restart rebuilds, replay of a game, and the alerts-sent record. |
| Durability | Iggy `replicated` x 3; the current path as deployed | Like for like. |
| Neighbour | Player viewing events on their own topic, 4x surge when the evening slate starts | Viewing data feeds product analytics, recommendations and personalization (Fubo Data Engineering posting, 2026). |
| Restart | Restart Game Alerts mid-game, at the busiest minute | Alerts recorded twice (must be 0); alerts pushed late; alerts skipped as too old. |
| Failover | Kill the Iggy leader mid-game | Time until Game Alerts receives again, and the next minute's p99. |
| Duration | One game night per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the GCP machine family the current path uses, in the same region | Nodes, vCPUs and $ per hour reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; the current path by its own load tools with the same message size and reader
count. The kit runs alongside at each step, so the report shows `held back by review`, `corrected after push`,
`gaps` and the alerts-sent audit next to p99.

**Pass line:** moment-to-reader p99 under 10 ms and p99.9 under 20 ms, replicated, through the first-pitch surge;
`gaps` 0; no alert recorded twice; a restarted Game Alerts current within 2 s.

**Published:** p99 and p99.9 vs reader count (one line per system), the surge trace, the restart trace, failover
recovery, broker CPU, node count and every config and raw output.
