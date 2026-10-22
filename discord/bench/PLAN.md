# Benchmark plan: the Osprey action lane

**Question:** with Osprey's asynchronous actions keyed by user and every partition owned by exactly
one rules worker, does every action reach its worker within 50 ms at p99 at raid rates, with a search
reindex backfill on the same cluster; is a spammer label always in place before that user's next
action, through a worker restart and a leader failover; and what does the lane cost per billion
actions, on Apache Iggy, on Pub/Sub and on Kafka?

Rules are fixed before any result exists. If Pub/Sub or Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Action size | 200–400 B action | User ID, action name, the fields rules read (account age, link flag), per-user sequence, timestamp. |
| Action rate | 10K → 30K → 60K actions/s | "~400 million actions per day" is about 4,600/s on average (Discord Engineering, Feb 2026); peaks are not published, so we test 2×, 6× and 13× the average. |
| Users | 5M → 20M active, Zipf-skewed; 3% → 10% of sessions spam runs of 8–20 link messages 80–250 ms apart | Spam runs are where the next action arrives before the label could land. |
| Partitions and workers | 64 → 128 → 256 partitions, 4 → 16 → 32 workers, static ownership, direct partition reads | One owner per user by construction; more partitions is how ownership scales out. |
| Rules cost | The kit's two rules, then a worker that spends 2–20 ms per action (stands in for ~500 rules and UDF RPCs) | Head-of-line blocking within a partition shows only when evaluation is not free. |
| Neighbour | 40K → 80K reindex documents/s, 800 B | Discord reindexes its largest guilds by backfilling history (search post, Apr 2025). |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 · Pub/Sub regional, with and without ordering keys | A dropped action is a missed verdict. |
| Worker restart | Kill one worker at minute 10, move its partitions to another, stop-then-start | Let-throughs and the time until its users are evaluated again. |
| Failover | Kill the leader at minute 15 | Time until workers receive again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same GCP region and machine family; Pub/Sub as the managed service | Like for like where it applies; Pub/Sub cost from its per-message pricing. |
| Cost | Nodes, CPU and monthly cost for the lane | Reported as $ per billion actions. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's workers and effect audit; Kafka by its own perf tool and
Pub/Sub by a load generator with the same workload shape. The kit runs alongside at each step, so the
report shows messages let through after the label, users labelled by two workers, and order
violations next to p99.

**Pass line:** p99 under 50 ms action-to-verdict at 30K actions/s, replicated, with the backfill next
door; zero messages let through after the label and zero users labelled by two workers, including
through the restart and the failover; and a lower $ per billion actions than the same lane on Pub/Sub
and on Kafka.

**Published:** p99 and p99.9 vs rate and vs partition count (one line per system), let-throughs per
run, restart and failover traces, CPU, node count, monthly cost and $ per billion actions, and every
config and raw output.
