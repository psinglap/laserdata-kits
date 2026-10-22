# Benchmark plan: the Autoplay lane

**Question:** with listening analytics on the same brokers, does every listening action (play, episode
complete, unlock, skip, show complete) reach the Autoplay queue within 20 ms at p99, so the next show
Autoplay starts at the end of content reflects what the listener just did, on Apache Iggy and on the same
lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B action | User ID, session, sequence, action type, show ID, unlock method, timestamp. |
| Rate | 20K → 50K → 100K → 200K actions/s | Pocket FM has 250M+ listeners in 20+ countries (TechCrunch, Sep 2026) but does not publish its event rate; set the top step to your busiest hour. |
| End-of-content requests | half of the action rate | A request 0.3–2 s after an action; set the share and gap to what your Autoplay API logs show. |
| Partitions | 32, keyed by user | A listener's actions apply in order; the queue is per listener. |
| Readers | The Autoplay queue service, a monitoring reader, an analytics consumer | Three readers on the same lane, Iggy and Kafka alike. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | An action the queue relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`, one consumer group per reader | Kafka on its best low-latency settings, on the same instance family. |
| Surge | 2× for 5 min | When a hit's daily episodes drop and its listeners come back at once (illustrative; use your own peak). |
| Neighbour | 100 MB/s of listening analytics (playtime pings, screen events) on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the queue receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full US evening | Drift shows over hours; the US is ~70% of revenue. |
| Hardware | 3 nodes, the same EC2 instance family as your Kafka brokers, one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost next to your Kafka cluster for the same lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same workload through the Java client, with the same shape.
The kit runs alongside at each rate, so the report shows `picked without the last action` next to p99.

**Pass line:** action-to-queue p99 under 20 ms at 50K actions/s, replicated, through the 2× surge with the
neighbour running; Autoplay picks made without the listener's last action under 0.01%.

**Where it runs:** in your AWS account, on your EKS nodes or EC2, with a copy of listening actions only.
Nothing is in the Autoplay serving path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU,
node count, cross-zone bytes and cost for the lane, and every config and raw output.
