# Benchmark plan: the Kill Bill usage lane

**Question:** at peak payment rates, does every payment's usage reach the subscription's live day total within
10 ms at p99 on Apache Iggy, counted once with retries and replays, and does a Kill Bill outage leave the live
meter untouched while the usage waits in the log and drains afterwards with nothing lost? How does the same lane
compare on Kafka?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~330 B usage message · ~110 B day total | The Kill Bill plugin's JSON (subscriptionExternalKey, trackingId, tenantId, txnCount and txnVolume records) plus sequence, day totals and timestamp. |
| Rate | 5K → 12K → 25K → 50K usage messages/s | One message per successful payment; 12K is the screening peak Cashfree's RiskShield post gives; the real rate is yours to set. |
| Partitions | 16–32 on usagemetrics, keyed by subscriptionExternalKey · 1–4 on usage-daily | A subscription's usage applies in order to one reader. |
| Retries | 0.5% of messages sent twice, plus one meter restart per step | Counted-once has to hold under at-least-once delivery. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | Usage is revenue and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× payment rate for 10 min | A sale-day or salary-day surge, compressed. |
| Kill Bill outage | Kill Bill unavailable for 10 min at minute 10 | The backlog it leaves, the time to drain it, and the live meter's p99 meanwhile. |
| Failover | Kill the leader at minute 20 | Time until the meter receives again, the next minute's p99, and reconciliation after. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours, and a soak crosses an IST day boundary. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows subscriptions reconciled, retries absorbed and Kill Bill drain time
next to p99.

**Pass line:** payment-to-usage p99 under 10 ms at 25K usage messages/s, replicated, through the 3× peak and the
Kill Bill outage; every subscription reconciled to the paisa in the meter and in Kill Bill; zero usage counted
twice; the backlog drained within minutes of Kill Bill coming back.

**Published:** p99 and p99.9 vs rate (one line per system), the peak and outage traces, failover recovery,
reconciliation counts, broker CPU, node count and monthly cost for the lane, and every config and raw output.
