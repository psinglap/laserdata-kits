# Benchmark plan: the billable-events lane for BCAP

**Question:** at Big Billion Days rates, with the rest of the UI telemetry firehose on the same brokers, does
every billable view and click reach BCAP, and every budget stop reach every ad server, within 10 ms at p99 per
hop, on Apache Iggy and on the same lane in your Pub/Sub or Kafka setup?

Rules are fixed before any result exists. If the incumbent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B billable event · ~100 B stop | Campaign, impression ID, event time, kind, cost in ₹, budget day, ad server, timestamp. |
| Rate | 50K → 100K → 250K → 500K billable events/s | Billable views and clicks are a share of the ~1M events/s your ads pipeline ingests; the real share is yours to set. |
| Partitions | 32 on billable events, keyed by campaign · 1–4 on stops | A campaign's events and its repeats reach one BCAP reader, in order; every ad server reads every stop. |
| Readers | BCAP on billable events; 50 → 500 ad servers on stops | The fan-out back to ad serving is the second hop and has to hold as the fleet grows. |
| Dirty traffic | 0.5% repeats, 2% minutes late, 0.1% over an hour late | Your firehose as your post describes it: retries, jitter, apps batching in the background. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 · Pub/Sub as configured | Billable events feed charges and cannot be lost. |
| Incumbent tuning | Kafka `linger.ms=0`, `fetch.max.wait.ms=0` · Pub/Sub streaming pull, same region | Each on its best latency settings. |
| Peak | 7× for 10 min | Big Billion Days ran 7× BAU transactional data (Google Cloud, 2023). |
| Neighbour | 50 MB/s of non-billable UI telemetry on a separate topic, same brokers | The rest of the firehose must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until BCAP and the ad servers receive again, BCAP's watermark lag, whether it went fail-safe. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family, in your GCP region | Like for like; nodes and ₹ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows over-burn in ₹ and repeats dropped next to p99.

**Pass line:** billable-to-BCAP p99 and stop-to-ad-server p99 each under 10 ms at 250K billable events/s with 500
ad servers, replicated, through the 7× peak and with the neighbour running; over-burn under 0.1% of spend; no
event charged twice.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery and
watermark lag, broker CPU, node count and monthly cost for the lane, and every config and raw output.
