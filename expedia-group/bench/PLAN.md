# Benchmark plan: the Expedia Group bandit decision lane

**Question:** through a spike in traffic, does every bandit decision (session, bandit, arm shown) reach every
Sampler pod's consistency cache within 10 ms at p99, so no traveler sees a different hero image, layout or module
order for the same bandit on their next page, on Apache Iggy and on the same lane on Kafka?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Decision size | 120 B | session, bandit, arm, Sampler, timestamp. |
| Rate | 20K → 50K → 100K → 200K decisions/s | New decisions only: every bandit on every page a session has not seen yet. |
| Partitions | 32, keyed by session | A session's decisions apply in order. |
| Readers | 16 Sampler pods, each reading every partition | The load balancer is not sticky: any pod can serve a session's next page. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A decision must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Spike | 3× new sessions for 10 min | Illustrative; replace with Expedia Group's own peak shape. |
| Neighbour | 50 MB/s of clickstream events on a separate topic | A bulk stream must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until Samplers receive again, and the next minute's p99. |
| Rebuild | Scale out by four Sampler pods at minute 20 | Time to rebuild every live session from the log before the first page. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows flipped images on revisits next to p99.

**Pass line:** decision-to-cache p99 under 10 ms at 100K decisions/s with sixteen Sampler pods, replicated,
through the spike and with the clickstream neighbour running; flipped images on revisits near zero outside the
failover minute; a new Sampler pod rebuilds every live session in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the spike trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
