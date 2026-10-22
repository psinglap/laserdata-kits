# Benchmark plan: the KBC Play Along answer lane

**Question:** with a full episode's question bursts and heavy engagement traffic on the same brokers, does every
locked answer reach the scorer that owns its player within 20 ms at p99, so it is scored, on time and once, before
the answer is revealed on TV, on Apache Iggy and on the same lane in Amazon Kinesis Data Streams?

Rules are fixed before any result exists. If Kinesis wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B lock | Player ID, question ID, option, lock time, sequence, timestamp. |
| Players | 200K → 500K → 1M → 1.6M locking per question | Sony LIV reported a peak of 1.6 million simultaneous KBC game users (AWS case study). Set the top step to your largest episode. |
| Burst shape | 70% of a question's locks inside the first 10 s, the rest to the timer | A right answer inside 10 s earns 200 bonus points (KBC 2025 Play Along FAQ), so locks bunch up at the start. At 1.6M players that is on the order of 100K locks a second. |
| Partitions / shards | 64 partitions, keyed by player · Kinesis shards sized for the same burst | A player's locks apply in order; scoring is per player. |
| Readers | The scorer (several processes, each owning a set of partitions), an audit reader, an analytics consumer | Three readers on the same lane: Kinesis shared-throughput polling, then enhanced fan-out, both reported. |
| Durability | Iggy `replicated` × 3 · Kinesis (replicated across three AZs by the service) | A lock that decides a prize cannot be lost. |
| Kinesis tuning | KCL polling at the 200 ms floor; enhanced fan-out (`SubscribeToShard`) as a second row | Kinesis on its best documented latency settings. |
| Neighbour | 100 MB/s of playback and engagement events on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader during a question's bonus window | Time until the scorer receives again, and that question's p99 and late-scored count. |
| Rebuild | Start a fresh scorer mid-episode | Time to rebuild every player's episode score from the log, and a digest match with the live scorer. |
| Duration | A full episode's question timeline per step, plus one soak across a week of episodes | The bursts come per question; drift shows over hours. |
| Hardware | 3 nodes, same EC2 instance family as your EKS ingest nodes, ap-south-1, three AZs | Like for like; nodes, cores, disks, cross-AZ bytes and cost next to the Kinesis bill for the same lane (shards, PUT units, EFO consumer-shard hours). |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kinesis by the same workload through the AWS SDK, with the same shape.
The kit runs alongside at each rate, so the report shows `scored after the reveal` next to p99.

**Pass line:** lock-to-score p99 under 20 ms at the 1M-player step, replicated, through the bonus-window bursts
with the neighbour running; no valid lock scored after its reveal; a rebuilt scorer's digests identical to the
live scorer's.

**Where it runs:** in your AWS account, on your instances, with a copy of Play Along locks only. Nothing is in the
scoring path during the evaluation.

**Published:** p99 and p99.9 vs players (one line per system and read mode), the per-question burst trace,
failover recovery, rebuild time, broker CPU, node count, cross-AZ bytes and cost for the lane, and every config
and raw output.
