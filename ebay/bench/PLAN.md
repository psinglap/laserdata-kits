# Benchmark plan: the broadcast push lane

**Question:** with unicast notifications flowing and a broadcast to 20,000 subscriptions arriving, does every
window task reach the push worker that owns it, and every unicast notification reach its push worker, within
10 ms at p99, and is every broadcast handed to the HTTP senders for all 20,000 subscriptions within 250 ms, on
Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B unicast notification · ~140 B window task · ~200 B retry record | Topic, subscription or (event, window), sequence, timestamp. |
| Rate | 3K → 10K → 30K unicast/s, with a broadcast every 5 s → every 1 s | eBay has not published ESP volumes; the steps bracket a busy notification platform. |
| Broadcast | 20,000 subscriptions in windows of 200 → 100,000 in windows of 200 | eBay's post: 20,000 subscribers, window size 200. The upper step asks what happens as the topic grows. |
| Partitions | 16 unicast (keyed by subscription) · 16 windows (window % 16) · 8 retry | Task affinity as eBay's post describes it; per-subscription order. |
| Readers | 4 → 32 push workers, direct partition reads | Each worker owns its windows and keeps them cached. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | A broadcast must reach every subscription. |
| Kafka tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, same hardware | Kafka on its best latency settings. |
| Endpoint outages | 1% of endpoints not acknowledging for 6 s of every 20 s | Retries on their own topic; the lane under test must not move. |
| Failover | Kill a broker at minute 15 | Time until push workers receive again; the next minute's completion times. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows broadcast completion next to p99.

**Pass line:** event-to-push p99 under 10 ms and broadcast completion p99 under 250 ms at 10K unicast/s with a
broadcast every second, replicated, with the endpoint outages running; unicast p99 in seconds with broadcast
windows within 10% of seconds without.

**Published:** p99 and p99.9 vs rate (one line per system), broadcast completion distribution, unicast p99 with
and without broadcasts, retry hold times, failover recovery, node CPU, node count and monthly cost for the lane,
and every config and raw output.
