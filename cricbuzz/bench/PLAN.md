# Benchmark plan: the ball-by-ball commentary lane

**Question:** at Cricbuzz's match-day shape, does every commentary line and every correction reach every
commentary node and match alerts within 10 ms at p99 on the broker hop, through an innings-break surge of
impressions next door, with replication on, on Apache Iggy and on the same hop over the path Cricbuzz runs
today? And can a commentary node added before a big match hold every live match's feed within seconds?

Rules are fixed before any result exists. If the current path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One commentary entry: match id, per-match sequence, kind (ball, edit, over), line id and version, over.ball, event, text, resulting score (about 200 B JSON) | A correction names its line and a higher version, so a reader never puts an older line back; the score so far, not a delta, so a repeat is harmless and a gap is visible. |
| Rate | Real time across every live match on the list, up to 1,000 lines/s | Ball-by-ball commentary for every live match is the core of the product ("super-fast scores and immersive ball-by-ball commentary", App Store listing, October 2026); the lane is small, the fan-out is wide. |
| Partitions | 4, 8 and 16, keyed by match id | Each match's lines and corrections stay in order. |
| Readers | 10, 50 and 200 commentary nodes, each reading every partition, plus match alerts | Every node serves every match; adding nodes must not slow or rebalance the others or the alerts. |
| Audience | 25 million daily active users (IPL 2025, Cricbuzz) | Sets how many nodes read the lane at the peak. |
| Retention | One match day | Rebuilds, replay of a match, and the alerts record. |
| Durability | Iggy `replicated` x 3; the current path as deployed | Like for like. |
| Neighbour | Ad impressions on their own topic, 4x surge at the innings break | 110 billion impressions during IPL 2025 (Cricbuzz). |
| Scale-out | Add 50% more nodes 15 minutes before an India match and at the toss | Time until each new node holds every live feed; p99 of the existing nodes meanwhile. |
| Restart | Restart match alerts mid-innings | Alerts recorded twice (must be 0); alerts sent late; alerts skipped as too old. |
| Failover | Kill the Iggy leader mid-match | Time until nodes receive again, and the next minute's p99. |
| Duration | One match evening per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the instance family the current path uses, in the same region | Nodes, vCPUs and $ per hour reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; the current path by its own load tools with the same message size and reader
count. The kit runs alongside at each step, so the report shows `corrections live`, `old lines held`, `gaps`
and the alerts record next to p99.

**Pass line:** line-to-node p99 under 10 ms and p99.9 under 20 ms with 50 nodes, replicated, through the
innings-break surge; `gaps` and `old lines held` 0; no alert recorded twice; a node added before a big match
holding every live feed within 2 s.

**Published:** p99 and p99.9 vs node count (one line per system), the surge trace, the scale-out trace,
failover recovery, broker CPU, node count and every config and raw output.
