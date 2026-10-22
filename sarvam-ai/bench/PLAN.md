# Benchmark plan: the concurrency lane

**Question:** at a campaign launch, with a heavy post-call payload stream on the same brokers, does every
conversation start and end reach the plan-ceiling ledger, and every headroom change reach every voice-agent
worker, within 10 ms at p99 per hop, on Apache Iggy, and how does the same lane compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B slot event · ~110 B headroom decision | Worker, organization, sequence, resulting live count, source, event, timestamp. |
| Rate | 100 → 500 → 2K → 10K conversations started/s (two slot events each) | Millions of voice calls a day today; the step above it is headroom for growth and launches. |
| Live conversations | 10K → 50K → 200K | Rate × talk time; 90 s average talk time, as in Sarvam's own sizing example. |
| Partitions | 16 on slot-events, keyed by organization · 1–4 on headroom | One ledger owns an organization's count, in order; every worker reads every decision. |
| Readers | The ledger on slot-events; 20 → 200 workers on headroom | The fan-out back to the fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A lost end leaks a slot until the lease releases it; a lost start lets a ceiling be passed. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 10 min | A large campaign launched at the start of calling hours, compressed. |
| Neighbour | 50 MB/s of post-call payloads (outcome, transcript, variables) on a separate topic, same brokers | A heavy, latency-tolerant stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until the ledger and the workers receive again, and the next minute's p99. |
| Worker loss | Kill 10% of workers at minute 20 | Time until their slots are released by lease, and conversations refused meanwhile. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region (e.g. ap-south-1 or Sarvam's own GPU-cloud region) | Like for like; nodes and ₹ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows conversations started past a ceiling next to p99.

**Pass line:** slot-event-to-ledger p99 and decision-to-worker p99 each under 10 ms at 10K conversations
started/s with 200 workers, replicated, through the 3× peak and with the neighbour running; outbound started
inside the inbound reserve under 0.5% of outbound starts; no slot held by a killed worker after the lease.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover and
worker-loss recovery, broker CPU, node count and monthly cost for the lane, and every config and raw output.
