# Benchmark plan: the live watch signals lane

**Question:** with heavy client-signal traffic on the same brokers, does every start, switch and stop of a live
event reach the live personalization features within 20 ms at p99, so the profile's next Live rail is built
from what they are actually watching, on Apache Iggy and on the same lane in Amazon Kinesis Data Streams?

Rules are fixed before any result exists. If Kinesis wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B signal | Profile ID, sequence, action, now-watching and just-left event IDs, timestamp. |
| Rate | 20K → 50K → 100K → 200K signals/s | Disney+ ingests "billions of events per hour" into Kinesis (AWS); watch signals are a slice. Set the top step to your largest live window. |
| Rail requests | half of the signal rate | A rail request 0.3–2 s after a signal; set the share and gap to what your app logs show. |
| Partitions / shards | 32 partitions, keyed by profile · Kinesis shards sized for the same rate | A profile's signals apply in order; the features read per profile. |
| Readers | The feature service, a monitoring reader, an analytics consumer | Three readers on the same lane: Kinesis shared-throughput polling, then enhanced fan-out, both reported. |
| Durability | Iggy `replicated` × 3 · Kinesis (replicated across three AZs by the service) | A signal the rail relies on cannot be lost. |
| Kinesis tuning | KCL polling at the 200 ms floor; enhanced fan-out (`SubscribeToShard`) as a second row | Kinesis on its best documented latency settings. |
| Surge | 2× for 5 min | A slate of games tipping off, when everyone switches at once. |
| Neighbour | 100 MB/s of playback beacons and analytics on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the features receive again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full live window | Drift shows over hours, and a live slate has its own shape. |
| Hardware | 3 nodes, same EC2 instance family as your Flink workers, one region, three AZs | Like for like; nodes, cores, disks, cross-AZ bytes and cost next to the Kinesis bill for the same lane (shards, PUT units, EFO consumer-shard hours). |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kinesis by the same workload through the AWS SDK, with the same shape.
The kit runs alongside at each rate, so the report shows `built without the last signal` next to p99.

**Pass line:** signal-to-feature p99 under 20 ms at 50K signals/s, replicated, through the 2× surge with the
neighbour running; Live rail requests built without the profile's last signal under 0.01%.

**Where it runs:** in your AWS account, on your instances, with a copy of watch signals only. Nothing is in the
rail path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system and read mode), the surge trace, failover recovery,
broker CPU, node count, cross-AZ bytes and cost for the lane, and every config and raw output.
