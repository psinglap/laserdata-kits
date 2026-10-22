# Benchmark plan: the session context lane

**Question:** with the clickstream on the same brokers, does every shopper action (search, product view,
wishlist, bag add, filter) reach the session context within 20 ms at p99, so the shopper's next type-ahead
request is boosted from what they want now, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B action | User ID, session, sequence, action type, three weighted annotations (category, brand, gender), timestamp. |
| Rate | 20K → 50K → 100K → 200K actions/s | Myntra's data platform takes "millions of clickstream events each minute" (Aerospike, Mar 2026) and peaked at 18.8 billion app and web events on BFF 2022's first day (Myntra Engineering); context-building actions are a slice. Set the top step to your largest sale hour. |
| Type-ahead requests | half of the action rate | A request 0.3–2 s after an action; set the share and gap to what your search logs show. |
| Partitions | 32, keyed by user | A shopper's actions apply in order; the context is per shopper. |
| Readers | The context service, a monitoring reader, an analytics consumer | Three readers on the same lane, Iggy and Kafka alike. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | An action the context relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`, one consumer group per reader | Kafka on its best low-latency settings, on the same VM family. |
| Surge | 2× for 5 min | The first hour of EORS or BFF, when everyone is browsing at once. |
| Neighbour | 100 MB/s of clickstream (page loads, impressions) on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the context receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full sale day | Drift shows over hours, and a sale day has its own shape. |
| Hardware | 3 nodes, the same Azure VM family as your Kafka brokers, one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost next to your Kafka cluster for the same lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same workload through the Java client, with the same shape.
The kit runs alongside at each rate, so the report shows `boosted without the last action` next to p99.

**Pass line:** action-to-context p99 under 20 ms at 50K actions/s, replicated, through the 2× surge with the
neighbour running; type-ahead requests boosted without the shopper's last action under 0.01%.

**Where it runs:** in your Azure subscription, on your AKS nodes or VMs, with a copy of shopper actions only.
Nothing is in the type-ahead path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU,
node count, cross-zone bytes and cost for the lane, and every config and raw output.
