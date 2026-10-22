# Benchmark plan: the account spend lane

**Question:** with a heavy event-catalog stream on the same brokers, does every approval and reversal's
resulting account spend reach the transaction feature store within 20 ms at p99, so the decision engine
checks the next authorization's category limit against current spend, on Apache Iggy and on the same
lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~300 B spend update | Account, sequence, card, category, resulting spend per category (six MCC groups), reference, timestamp. |
| Rate | 250 → 1K → 5K → 20K spend updates/s | Zeta reported about 2 million transactions a day in 2024 (about 25 a second on average); the steps leave room for tenant growth and peaks. |
| Partitions | 16, keyed by account | An account's spend applies in order across its primary and add-on cards. |
| Readers | The feature store, one monitoring reader, one data-lake reader | Each reading the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A spend update a limit relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 4× for 15 min | Zeta does not publish per-second peaks; set it to your salary-day or festival peak. |
| Add-on runs | 2% of authorizations in runs of 2–7 on one add-on card, 1–3 s apart, crossing the category limit | The checks where a late update lets an authorization through over the limit. |
| Reversals | 3% of approvals, 2–20 s later | A reversal not yet applied declines a card that had room. |
| Neighbour | 50 MB/s of catalog events (notifications, data-lake feed) on a separate topic, same brokers | The event catalog must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the store receives again, and the next minute's p99. |
| Duration | 30 min per step + one full-day soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same EC2 instance class, in your AWS account and region | Like for like; nodes, cores, disks and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `missing the last change` and
`over-limit approvals` next to p99.

**Pass line:** authorization-to-store p99 under 20 ms at 5K spend updates/s, replicated, through the
4× peak with the neighbour running; zero over-limit approvals and checks missing the last change under 0.01%.

**Where it runs:** inside your AWS account, on copies of one tenant's authorization outcomes only.
Nothing is in the authorization path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery,
broker CPU, node count and cost for the lane, and every config and raw output.
