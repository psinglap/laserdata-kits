# Benchmark plan: the live scorecard lane

**Question:** at FanCode's match-day shape, does every score update reach every scorecard node within
10 ms at p99 on the broker hop, through a headline match start and a burst of app events next door, with
replication on, on Apache Iggy and on the same hop over the path FanCode runs today? And can a scorecard
node added before a big match answer scorecardPoll with the current key for every live match within
seconds?

Rules are fixed before any result exists. If the current path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One score update: match id, sport, per-match key, event, resulting score of both sides (about 120 B JSON) | The score so far, not a delta, so a repeat is harmless and a gap is visible. The key is the one scorecardPoll returns. |
| Rate | Real time across every live match and sport, up to 500 updates/s | 15,000+ live events a year (FanCode on the Google Cloud blog, 2024); the lane is small, the fan-out is wide. |
| Partitions | 4, 8 and 16, keyed by match id | Each match's updates stay in order. |
| Readers | 10, 50 and 200 scorecard nodes, each reading every partition | Every node answers polls for every match; adding nodes must not slow or rebalance the others. |
| Viewers | 6 million concurrent, polling every 5 s | 6 million concurrent viewers in one 2022 series (AWS case study); 5 s is the web app's scorecardPoll default. |
| Retention | One match day | Rebuilds and replay of a match. |
| Durability | Iggy `replicated` x 3; the current path as deployed | Like for like. |
| Neighbour | App events on their own topic, 4x surge at a headline match start | The data highway batches app events from every screen. |
| Scale-out | Add 50% more nodes 15 minutes before a headline match and at its start | Time until each new node answers with the current key; p99 of the existing nodes meanwhile. |
| Failover | Kill the Iggy leader mid-match | Time until nodes receive again, and the next minute's p99. |
| Duration | One match evening per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the GKE node pool or instance family the current path uses, in the same Google Cloud region | Nodes, vCPUs and $ per hour reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test` with
the same message size and reader count. The kit runs alongside at each step, so the report shows
`viewer polls answered behind the newest score` and `gaps` next to p99.

**Pass line:** update-to-node p99 under 10 ms and p99.9 under 20 ms with 50 nodes, replicated, through
the match-start surge; `gaps` 0; a node added before a headline match answering with the current key for
every live match within 2 s.

**Published:** p99 and p99.9 vs node count (one line per system), the surge trace, the scale-out trace,
failover recovery, broker CPU, node count and every config and raw output.
