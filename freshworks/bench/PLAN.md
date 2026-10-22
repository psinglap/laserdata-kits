# Benchmark plan: the Freshworks agent state lane

**Question:** through a shift change in a ticket spike, does every agent change (status, call, chat and ticket
load against capacity) reach every Omniroute routing node within 10 ms at p99, so no ticket or chat is handed to
an agent who is away, on a call or at capacity, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 180 B | agent, sequence, group, status, tickets and capacity, chats and capacity, call flag, assignments received, product, timestamp. |
| Rate | 10K → 25K → 50K → 100K agent changes/s | Assignments, resolutions, chats closing, calls, breaks, manual assignments and periodic re-sends of every agent. |
| Partitions | 32, keyed by agent | Each agent's changes apply in order, whichever product sent them. |
| Readers | 8 routing nodes, each reading every partition | Every node holds every agent. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A status change must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Shift change | Half of the busiest groups turn over while their queues spike 4× for 10 min | A shift ending into an incident at a large customer. |
| Neighbour | 50 MB/s of product events on a separate topic | The data-platform stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until routing nodes receive again, and the next minute's p99. |
| Rebuild | Start a new routing node at minute 20 | Time to rebuild every agent from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region, self-hosted | Like for like with Central's own clusters; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows work sent to agents who are away, on a call or at capacity next to p99.

**Pass line:** change-to-view p99 under 10 ms at 50K changes/s with eight routing nodes, replicated, through the
shift change and with the product-events neighbour running; misrouted tickets and chats near zero outside the
failover minute; a new routing node rebuilds every agent in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the shift-change trace, failover recovery, rebuild
time, broker CPU, node count and monthly cost for the lane, and every config and raw output.
