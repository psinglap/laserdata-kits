# Benchmark plan: the Contextual Journeys trip lane

**Question:** with tracked events keyed by user and every partition owned by exactly one journey
worker, does every event reach its trip within 50 ms at p99 at WebEngage's event rates, with a
one-time campaign send on the same cluster; does every exit event reach its trip before that trip's
reminder goes out, through a worker restart and a leader failover; and what does the lane cost per
billion events, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B tracked event | User ID, event name, context value, per-user sequence, timestamp. |
| Event rate | 50K → 100K → 200K events/s | 1.6T+ events tracked a year (webengage.com) is about 50K/s on average; sale and renewal days run well above it. |
| Users and trips | 20M → 50M active users, Zipf-skewed, 30% of new trips for a user already in one | "User + Context = Unique Journey Trip": one user holds several trips of one journey (WebEngage, Dec 2025). |
| Exit race | 35% of trips exit 0.1–0.8 s before their reminder is due | The exit trigger must win, or the user who just renewed gets "your renewal is due". |
| Partitions and workers | 64 → 128 → 256 partitions, 4 → 16 → 32 workers, static ownership, direct partition reads | One owner per user by construction, so per-user state needs no lock. |
| Campaign send next door | 25K → 50K campaign messages/s, 400 B | 60B+ messages a month (webengage.com) is about 23K/s on average. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, `min.insync.replicas=2` | A trip event cannot be lost. |
| Kafka tuning | `linger.ms` 0 and 5, cooperative-sticky assignor, static membership | Kafka on its best latency settings. |
| Worker restart | Kill one worker at minute 10, move its partitions to another, stop-then-start | Reminders sent after exit, trips reminded twice, and time until its users are advanced again. |
| Failover | Kill the leader at minute 15 | Time until workers receive again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same AWS instance family and region | Like for like. |
| Cost | Nodes, CPU and monthly cost for the lane | Reported as $ per billion events. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's workers and send audit, Kafka by its own perf tool with
the same workload shape. The kit runs alongside at each step, so the report shows reminders sent after
exit, trips reminded twice and order violations next to p99.

**Pass line:** p99 under 50 ms event-to-trip at 100K events/s, replicated, with the campaign send next
door; at most 0.1% of reminders sent after the trip's exit and zero trips reminded twice, including
through the restart and the failover; and a lower $ per billion events than the same lane on Kafka.

**Published:** p99 and p99.9 vs rate and vs partition count, reminders sent after exit per run,
restart and failover traces, CPU, node count, monthly cost and $ per billion events, and every config
and raw output.
