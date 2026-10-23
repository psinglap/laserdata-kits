# Benchmark plan: the session sequence lane

**Question:** with the event firehose on the same brokers, does every product view, item page visit and cart
add reach the shopper's session sequence within 20 ms at p99, so the next ads carousel (retailer home page,
items to add next, pre-checkout) is retrieved from the cart the shopper has now, on Apache Iggy and on the same
lane in your Confluent Cloud cluster?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~220 B action | User ID, session, sequence, action kind, product token (Semantic ID), retailer type, aisle, timestamp. |
| Rate | 10K → 25K → 50K → 100K actions/s | Instacart reported 90.3 million orders in Q2 2026 (about a million a day) and an average order of $115; views, item page visits and cart adds behind those orders are a slice of the "over two trillion events a year" on the streaming platform. Set the top step to your busiest evening. |
| Carousel requests | half of the action rate | A request 0.2–1.5 s after an action; set the share and gap to what your ads serving logs show. |
| Partitions | 32, keyed by user | A shopper's actions apply in order; the sequence is per shopper. |
| Readers | The sequence service, a monitoring reader, a Flink-side consumer | Three readers on the same lane, Iggy and Kafka alike. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | An action the prompt relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`, one consumer group per reader | Kafka on its best low-latency settings. |
| Surge | 2× for 5 min | The evening before a holiday, when everyone is filling a cart at once. |
| Neighbour | 100 MB/s of firehose events (impressions, page loads) on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the sequence receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full day | Drift shows over hours, and a shopping day has its own shape. |
| Hardware | 3 nodes on the same AWS instance family as your Flink and serving fleet, one region, three AZs | Like for like; nodes, cores, disks, cross-AZ bytes and cost next to the same lane on Confluent Cloud. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same workload through the Java client, with the same shape.
The kit runs alongside at each rate, so the report shows `built without the last action` next to p99.

**Pass line:** action-to-sequence p99 under 20 ms at 25K actions/s, replicated, through the 2× surge with the
neighbour running; carousels built without the shopper's last action under 0.01%.

**Where it runs:** in your AWS account, on your Kubernetes nodes or EC2, with a copy of session events only.
Nothing is in the ads serving path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU,
node count, cross-AZ bytes and cost for the lane, and every config and raw output.
