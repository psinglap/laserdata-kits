# Benchmark plan: the Robinhood card state lane

**Question:** at market-open rates, does every card account change (spendable balance and card status) reach
every stand-in authorizer replica within 10 ms at p99, so a stand-in decision made a moment later matches the
primary's, on Apache Iggy and on the same lane on Kafka as you run it today?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 120 B | account, sequence, spendable balance, card status, kind, timestamp. |
| Rate | 5K → 20K → 50K → 100K changes/s | Card spend all day, plus cash moving into and out of stocks at the open. |
| Partitions | 16, keyed by account | Each account's changes apply in order. |
| Readers | 3 stand-in replicas, each reading every partition | Every replica holds every account. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A lock or a debit must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Market open | 5× for 10 min | Cash moving into stocks in the first minutes of trading. |
| Neighbour | 50 MB/s settlement backfill on a separate topic | A bulk load must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until replicas receive again, and the next minute's p99. |
| Rebuild | Restart one replica at minute 20 | Time to rebuild every account from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows stand-in decisions that disagree with the primary next to p99.

**Pass line:** change-to-replica p99 under 10 ms at 50K changes/s with three replicas, replicated, through the 5×
market open and with the settlement neighbour running; no stand-in decision made on stale state outside the
failover minute; a replica rebuilds every account in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the market-open trace, failover recovery, rebuild
time, broker CPU, node count and monthly cost for the lane, and every config and raw output.
