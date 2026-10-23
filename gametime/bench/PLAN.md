# Benchmark plan: the Gametime Zone Deals pool lane

**Question:** through an event's final hour, does every listing change (tickets left, price, seller floor) reach
every Zone Deals seat picker within 10 ms at p99, so no pick lands on a listing that already sold elsewhere or was
repriced above the Zone Deal price, on Apache Iggy and on the same lane on Confluent Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 160 B | listing, event:zone, sequence, tickets left, price, floor, automatic pricing flag, Zone Deals tickets processed, timestamp. |
| Rate | 5K → 20K → 50K → 100K listing changes/s | Seller API updates, automatic repricing and sales on other channels, across every event on sale. |
| Partitions | 32, keyed by event:zone | A zone's listings apply in order; a hot event's zones spread across partitions. |
| Readers | 4 seat pickers, each reading every partition | Every picker holds every zone. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A sold listing must not come back. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Final hour | One event: every opted-in listing repriced at once, then 10× its listing changes and Zone Deals orders for 10 min | The last 90 minutes, where Gametime says the best deals come. |
| Neighbour | 50 MB/s of app events on a separate topic | A bulk stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until pickers receive again, and the next minute's p99. |
| Rebuild | Start a new picker at minute 20 | Time to rebuild every zone's pool from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows re-picks (sold out, over the deal price) next to p99.

**Pass line:** change-to-pool p99 under 10 ms at 50K changes/s with four pickers, replicated, through the final
hour and with the app-events neighbour running; re-picks during the final hour within their pre-final-hour range;
a new picker rebuilds every zone in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the final-hour trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
