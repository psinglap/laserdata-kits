# Benchmark plan: the payer velocity lane

**Question:** with a heavy analytics stream on the same brokers, does every completed payment's
resulting velocity counts reach the aggregate store within 20 ms at p99, so the risk check on the
payer's next payment always counts it, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B velocity update | Payer, sequence, counts and amounts over 60 s and 10 min, distinct payees, transaction ID, timestamp. |
| Rate | 4K → 15K → 40K → 100K velocity updates/s | About 360 million payments a day on average (Aerospike Bangalore Summit talk, 2025), with headroom for peaks. |
| Partitions | 32, keyed by payer | A payer's counts apply in order; the risk check reads per payer. |
| Readers | Aggregate store for the risk check, one monitoring reader, one analytics reader | Each with its own consumer group on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A count that a check relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, the direct (unspooled) producer path | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 15 min | PhonePe does not publish per-second peaks; set it to your evening or festival peak. |
| Bursts | 2% of payments in runs of 2–7 payments, 1–3 s apart | The checks where a late count changes the decision. |
| Neighbour | 50 MB/s of analytics events on a separate topic, same brokers | The spooled analytics traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the store receives again, and the next minute's p99. |
| Duration | 30 min per step + one full-day soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same bare-metal or VM class in one of your data centers | Like for like; nodes, cores, disks and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `missing the previous payment` next to p99.

**Pass line:** payment-to-store p99 under 20 ms at 15K velocity updates/s, replicated, through the
3× peak with the neighbour running; risk checks missing the payer's previous payment under 0.01%.

**Where it runs:** entirely inside your data centers, on your machines, with copies of payment
events only. Nothing is in the authorization path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery,
broker CPU, node count and cost for the lane, and every config and raw output.
