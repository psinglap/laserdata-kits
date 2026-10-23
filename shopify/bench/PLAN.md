# Benchmark plan: the buyer signal lane for Shopify Inbox

**Question:** with storefront analytics traffic on the same brokers, does every cart change, checkout and
completed order of a chatting buyer reach that buyer's conversation within 20 ms at p99, so the next reply,
from the Inbox agent or the merchant's staff, sees what the buyer just did, on Apache Iggy and on the same
lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B signal | Buyer ID, sequence, event kind, product, and the resulting status, item quantity, subtotal and order number. |
| Rate | 10K → 25K → 50K → 100K signals/s | The Inbox post says the cart and checkout sources carry "tens of thousands of events per second" even off-peak, before filtering to chatting buyers. Set the top step to your BFCM peak for that filtered slice. |
| Replies | 40% of the signal rate | A reply 0.3–3 s after a signal; set the share and gap to what Inbox's own response times show. |
| Partitions | 32, keyed by buyer | Cart and checkout events reach Kafka from different sources; on the lane a buyer's signals apply in one order. |
| Readers | The conversation view, a monitoring reader, an analytics consumer | Three readers on the same lane, Iggy and Kafka alike. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | A completed order the conversation relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`, one consumer group per reader | Kafka on its best low-latency settings, on the same machine type. |
| Surge | 2× for 5 min | Black Friday opening; Shopify's observability ingest doubles from steady state to BFCM peak (ClickHouse, 2026). |
| Neighbour | 100 MB/s of storefront analytics events on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the conversation view receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak through a full sale day | Drift shows over hours, and BFCM has its own shape. |
| Hardware | 3 nodes, the same GCE machine family as your Kafka brokers on GKE, one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost next to your Kafka cluster for the same lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same workload through the Java client, with the same shape.
The kit runs alongside at each rate, so the report shows `written without the last signal` and
`order placed but not shown` next to p99.

**Pass line:** signal-to-conversation p99 under 20 ms at 25K signals/s, replicated, through the 2× surge with
the neighbour running; replies written without the buyer's last signal under 0.01%.

**Where it runs:** in your Google Cloud project, on your GKE nodes or VMs, with a copy of cart, checkout and
order events only. Nothing is in the Inbox path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU,
node count, cross-zone bytes and cost for the lane, and every config and raw output.
