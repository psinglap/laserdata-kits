# Benchmark plan: the DSP appetite lane

**Question:** at Magnite's scale shape, does every change in DSP appetite reach every traffic-shaping node
within 20 ms at p99 (the bar the Demand Tech team holds its routing path to), with replication on, while
the auction log runs at full volume on the same brokers, on Apache Iggy and on the same lane over Kafka as
Magnite runs it today? And can a shaping pod that starts mid-peak have the full table within seconds?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | ~90 B JSON (pair, sequence, bid rate, bidding flag, created time); a binary layout as a second step | The resulting state of one (DSP, supply segment) pair, never a delta. |
| Pairs | 60 DSPs × 5,000 segments = 300K, then × 4 | Set to your real DSP count and the segment keys your shaping model uses. |
| Rate | 20K → 100K → 250K changes/s into the lane, 1% of them flips | Inputs from Magnite: how often bid rates move, and how often a DSP stops or starts bidding on a segment. |
| Partitions | 8, 16, 32, keyed by pair | Each pair's changes stay in order. |
| Readers | 8, 32 and 128 shaping nodes, each reading every partition | Every node needs the whole table; adding nodes must not slow the others. |
| Retention | 6 hours | Rebuilds and replay; a rolling snapshot once a minute keeps a rebuild to about a minute of log. |
| Durability | Iggy `replicated` × 3; Kafka `acks=all`, RF 3 | Like for like. |
| Neighbour | Auction log at production MB/s on the same brokers, 4× burst | Bulk analytics traffic must not move the lane's tail. |
| Restart | Reschedule one shaping pod every 5 minutes | Time until it has every pair, and its first minute's p99. |
| Failover | Kill the Iggy leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 2 hours across a US evening peak per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the instance type the Kafka brokers use, on the same Kubernetes clusters | Nodes, cores and cost per million changes reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test` with the
same message size and fan-out. The kit runs alongside at each rate, so the report shows wasted callouts,
bids held back and `gaps` next to p99.

**Pass line:** exchange-to-node p99 under 20 ms and p99.9 under 40 ms at 100K changes/s with 32 nodes,
replicated, through the auction-log burst; `gaps` 0; a rescheduled pod holding every pair within 5 s.

**Published:** p99 and p99.9 vs rate and vs node count (one line per system), wasted callouts per million,
the burst trace, the restart trace, failover recovery, broker CPU, node count and every config and raw output.
