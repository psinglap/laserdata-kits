# Benchmark plan: the graph link lane

**Question:** with a heavy tracking stream on the same brokers, does every account-to-asset link from a
login or profile change reach the real-time graph's writer within 20 ms at p99, so the two-hop check on
the account's next payment always holds it, on Apache Iggy and on the same lane in your Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B link | Account, asset (device ID, IP, VID or address), kind, edge state, sequence, timestamp. |
| Rate | 10K → 50K → 100K → 200K links/s | About two links per login; PayPal's 2021 post puts payments at "several thousand" a second, and logins outnumber payments. Set to your numbers. |
| Partitions | 32, keyed by asset | An asset's links apply in order; the graph writer reads per partition. |
| Readers | Graph writer, one investigation-tool reader, one analytics reader | Each with its own position on the same lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A link a risk decision relies on cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Peak | 3× for 15 min | Retail Friday and Cyber Monday shape; PayPal does not publish per-second login peaks. |
| Repeat fraudsters | 0.5% of logins re-add a known-bad asset and pay 0.2–2 s later | The checks where a late link changes the decision. |
| Neighbour | 50 MB/s of tracking events on a separate topic, same brokers | First-party tracking must not move the lane's tail. |
| Failover | Kill the leader at minute 10 | Time until the writer receives again, and the next minute's p99. |
| Duration | 30 min per step + one full-day soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same machine class, one data center or cloud region | Like for like; nodes, cores, disks and cost reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape.
The kit runs alongside at each rate, so the report shows `without the newest links` next to p99,
and the same figure for your current write path's measured freshness (`graph.py --slow-path-s`).

**Pass line:** link-to-graph p99 under 20 ms at 100K links/s, replicated, through the 3× peak with
the neighbour running; payment checks without the links from their login under 0.01%; every repeat
fraudster linked to the restricted accounts.

**Where it runs:** on your machines, in your data center or cloud account, with copies of login and
profile events only. Nothing is in the payment path during the evaluation.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery,
broker CPU, node count and cost for the lane, and every config and raw output.
