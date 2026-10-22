# Benchmark plan: the decision outcome lane

**Question:** with the full event-history stream on the same brokers, does every delivered decision and
every goal event (add to cart, checkout, uninstall, subscription cancellation) reach the online learner
within 20 ms at p99, so the user's next decision is computed with it, on Apache Iggy and on the same lane
in your current transport?

Rules are fixed before any result exists. If the incumbent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B record | Identity, sequence, kind, action (campaign, channel, offer), timestamp. |
| Rate | 20K → 100K → 500K → 1M records/s | CleverTap counts 50B+ user actions a day (about 580K/s on average) across all brands; goal events and delivered decisions are a slice of that. Set the top step to your busiest brand's IPL hour. |
| Decision requests | half of the record rate | A request 0.3–2 s after a record; set the share and gap to what your decision logs show. |
| Partitions | 32, keyed by user | A user's records apply in order; the context is per user, as TesseractDB shards by user. |
| Readers | The online learner, a monitoring reader, a history writer | Three readers on the same lane, on both systems. |
| Durability | Iggy `replicated` × 3 · incumbent at its production setting (Kafka: `acks=all`, RF 3, `min.insync.replicas=2`) | A reward the learner relies on cannot be lost. |
| Incumbent tuning | Its best low-latency settings (Kafka: `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`) | The incumbent on its best day, on the same instance family. |
| Surge | 5× for 10 min | IPL 2023: one customer's ingest went from 45M to 309M events a minute (CleverTap tech blog, Apr 2023). |
| Neighbour | 100 MB/s of raw user actions on a separate topic, same brokers | Heavy, latency-tolerant history traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the learner receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full IPL match | Drift shows over hours. |
| Hardware | 3 nodes, the same EC2 instance family your ingestion runs on, one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost next to your current transport for the same lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; the incumbent by the same workload through its Java client, with the same
shape. The kit runs alongside at each rate, so the report shows `decided without the last outcome` next to p99.

**Pass line:** outcome-to-learner p99 under 20 ms at 100K records/s, replicated, through the 5× surge with
the neighbour running; decisions made without the user's last outcome under 0.01%.

**Where it runs:** in your AWS account, in the region of the brand you pick, with a copy of delivered
decisions and goal events only. Nothing is in the decision path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU,
node count, cross-zone bytes and cost for the lane, and every config and raw output.
