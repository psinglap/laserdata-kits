# Benchmark plan: the GPU node health lane

**Question:** at fleet scale, with a heavy GPU telemetry stream on the same brokers, does every passive health
sample reach the health checker, and every hold reach every placement scheduler, within 10 ms at p99 per hop, on
Apache Iggy and on the same lane in Kafka (or NATS JetStream, if that is the bus Together picks)?

Rules are fixed before any result exists. If the other side wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~110 B healthy sample · ~170 B sample with a signal · ~150 B hold | Node, sequence, active signals by Together's names (with GPU index and Xid), timestamp. |
| Rate | 10K → 30K → 100K samples/s | One sample a second per node; Together's postings describe "tens of thousands of GPUs", so 10K–100K nodes covers today and the announced build-out at 8 GPUs a node, plus faster sampling. |
| Partitions | 16 → 64 on node-signals, keyed by node · 1 on node-health | One checker decision per node, in order; every scheduler reads every hold. |
| Readers | Checker on node-signals; 8 → 64 placement schedulers and repair workers on node-health | The fan-out to the control plane is the second hop and has to hold as it grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | A hold that is lost puts work on a bad node. |
| Kafka tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, same region and instance class | Kafka on its best latency settings. |
| Faults | One node fault every 10 s (Xid, uncorrectable ECC, GPU off the bus, IB link flapping, thermal throttle), each lasting 20 s | Detection time, hold fan-out time and placements on the faulted node, per fault. |
| Burst | A rack loses power: 64 nodes report faults in the same second | Holds for a whole rack reach every scheduler before the next placement round. |
| Neighbour | 200 MB/s of DCGM metrics and kernel logs on a separate topic, same brokers | Fleet telemetry must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the checker and the schedulers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows placements after a hold next to p99.

**Pass line:** sample-to-checker p99 and hold-to-scheduler p99 each under 10 ms at 30K samples/s with 64
schedulers, replicated, through the rack burst and with the neighbour running; under 1% of the placements a 15 s
polling path would let through land on a node after its hold.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), per-fault detection and fan-out times,
the rack-burst trace, failover recovery, node CPU, node count and monthly cost for the lane, and every config and
raw output.
