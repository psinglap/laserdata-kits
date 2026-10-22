# Benchmark plan: the Dailyhunt floor lane

**Question:** at Dailyhunt's scale shape, does every new floor reach every NexVerse.ai bid node within
20 ms at p99, with replication on, through a breaking-news spike on one language and with the ad event log
at full volume on the same brokers, on Apache Iggy and on the same lane over the transport you would
otherwise use (Pub/Sub, or Kafka)? And can a bid node the autoscaler adds mid-spike have every floor within
seconds?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | ~80 B JSON (placement, sequence, floor, step flag, created time); a binary layout as a second step | The resulting floor of one placement, never a delta. |
| Placements | 15 languages × 2,000 ad slots = 30K, then × 4 | Set to the keys your floors really use (language, surface, slot, state, device). |
| Rate | 5K → 20K → 50K floor changes/s, 2% of them steps | Inputs from NexVerse.ai: how often floors move, and how often they jump. |
| Spike | One language at 4× and 8× its floor changes for 5 minutes, steps mostly up | Dailyhunt's "huge and instantaneous spikes … caused by breaking news". |
| Partitions | 8, 16, 32, keyed by placement | Each placement's floors stay in order. |
| Readers | 8, 32 and 128 bid nodes, each reading every partition | Every node needs every floor; adding nodes must not slow the others. |
| Retention | 6 hours | Rebuilds and replay; a rolling snapshot once a minute keeps a rebuild to about a minute of log. |
| Durability | Iggy `replicated` × 3; Kafka `acks=all`, RF 3; Pub/Sub as provided | Like for like. |
| Neighbour | The ad event log at production MB/s on the same brokers, 4× during the spike | Bulk analytics traffic must not move the lane's tail. |
| Scale-out | Add one bid node every 5 minutes, one during each spike | Time until it has every floor, and its first minute's p99. |
| Failover | Kill the Iggy leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 2 hours across an evening news peak per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on Google Cloud, on the machine type your bid nodes' neighbours use | Nodes, cores and cost per million floor changes reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test`, and
Pub/Sub by a publisher and one subscription per bid node, with the same message size and fan-out. The kit
runs alongside at each rate, so the report shows stale-floor auctions, rupees given away and `gaps` next to
p99.

**Pass line:** floor-to-node p99 under 20 ms and p99.9 under 40 ms at 20K changes/s with 32 nodes,
replicated, through an 8× spike and the event-log burst; `gaps` 0; an added node holding every floor within
5 s.

**Published:** p99 and p99.9 vs rate and vs node count (one line per system), stale-floor auctions per
million, the spike trace, the scale-out trace, failover recovery, broker CPU, node count and every config
and raw output.
