# Benchmark plan: the last-searched lane

**Question:** with the clickstream on the same brokers, does every hotel search and hotel view, from B2C and B2B,
reach the traveller's last-searched list within 20 ms at p99, so the next tap on the search bar shows the hotel
they just looked at, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B event | User ID, segment, sequence, event type, hotel, city, check-in date, timestamp. |
| Rate | 10K → 25K → 50K → 100K events/s | MakeMyTrip serves "millions of daily users" (Databricks, Apr 2026); hotel searches and views are a slice of the clickstream. Set the top step to your busiest sale hour. |
| Search-bar taps | half of the event rate | A tap 0.3–3 s after a search; set the share and gap to what your app logs show. |
| Partitions | 64, keyed by user | As on the Kafka stream behind "last-searched" today; a traveller's searches apply in order. |
| Segments | B2C and B2B on one lane | Both written by their own producers; the service reads one lane, with no stream union. |
| Readers | The last-searched service, a monitoring reader, an analytics consumer | Three readers on the same lane, Iggy and Kafka alike. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | A search the list relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.min.bytes=1`, `fetch.max.wait.ms=10`, one consumer group per reader | Kafka on its best low-latency settings, on the same VM family. |
| Surge | 2× for 5 min | A sale opening, when everyone is searching at once. |
| Neighbour | 100 MB/s of clickstream (page loads, impressions) on a separate topic, same brokers | Heavy, latency-tolerant traffic must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the service receives again, and the next minute's p99. |
| Duration | 30 min per step + one soak across a full sale day | Drift shows over hours, and a sale day has its own shape. |
| Hardware | 3 nodes, the same instance family as your Kafka brokers, one region, three zones | Like for like; nodes, cores, disks, cross-zone bytes and cost next to your Kafka cluster for the same lane. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by the same workload through the Java client, with the same shape.
The kit runs alongside at each rate, so the report shows `served without the last search` next to p99.

**Pass line:** search-to-list p99 under 20 ms at 25K events/s, replicated, through the 2× surge with the neighbour
running; taps served without the last search under 0.01%.

**Where it runs:** in your cloud account, beside your Kafka, with a copy of hotel search events only. Nothing is
in the app's path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the surge trace, failover recovery, broker CPU, node
count, cross-zone bytes and cost for the lane, and every config and raw output.
