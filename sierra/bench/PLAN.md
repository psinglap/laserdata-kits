# Benchmark plan: the customer memory lane

**Question:** at disruption-peak turn rates, with a heavy transcript log on the same brokers, does every turn reach
the memory builder, and every memory update reach the memory store, within 10 ms at p99 per hop, on Apache Iggy and
on the same lane in your current eventing stack (Kinesis or Kafka, whichever carries your conversation events)?

Rules are fixed before any result exists. If the incumbent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~600 B turn record · ~400 B memory record | Customer, channel, runtime, turn sequence, turn text, fact; memory carries the latest facts and the sequence it is current to. |
| Rate | 5K → 20K → 50K turns/s | Hundreds of millions of calls a year plus chat and SMS average a few thousand turns a second; peaks and growth set the upper steps. |
| Partitions | 32 on turns, keyed by customer · 8 on memory, keyed by customer | A customer's turns are applied in order on every channel; the store reads every memory partition. |
| Readers | Memory builder workers on turns; 1 → 8 store writers on memory | The store side scales with regions and tenants. |
| Durability | Iggy `replicated` × 3 · the incumbent's equivalent (Kinesis default, or Kafka `acks=all`) | Memory is a customer record and cannot be lost. |
| Incumbent tuning | Kinesis with enhanced fan-out, or Kafka with low linger, same region | The incumbent on its best latency settings. |
| Peak | 3× for 10 min | A disruption surge: many customers calling and switching at once. |
| Neighbour | 200 MB/s of transcripts on a separate topic, same brokers | The lakehouse feed must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the builder and the store receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region (or the managed service's equivalent capacity) | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, Kinesis by the same workload
written with the AWS SDK. The kit runs alongside at each rate, so the report shows channel switches that found the
store behind next to p99.

**Pass line:** turn-to-memory p99 and memory-to-store p99 each under 10 ms at 20K turns/s, replicated, through the
3× peak and with the neighbour running; under 0.1% of channel switches finding the store behind the customer's
latest fact, with your real switch gaps.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery, node
CPU, node count and monthly cost for the lane, and every config and raw output.
