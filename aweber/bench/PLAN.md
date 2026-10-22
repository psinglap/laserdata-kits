# Benchmark plan: the Imbi notifications lane

**Question:** on the Iggy server Imbi already runs, with its analytics streams busy next door, does every
deployment and mention notification reach slackbot and email within 50 ms at p99, posted once, and is a reader
that stops moving flagged within seconds?

Rules are fixed before any result exists. Every row is published, including the ones that look bad.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B notification · ~2 KB webhook row next door | Project or user, environment, entry type, version, sequence, timestamp. Webhook rows carry headers and payload. |
| Rate | Imbi's real notification rate, then 10× and 100× | ADR 0019 names deployments and mentions; Imbi's volume is not published, so the first step measures it. |
| Partitions | 4 per topic, keyed by project (deployment) and by user (mention) | One project's deploy and rollback stay in order; readers can spread out later. |
| Readers | slackbot and email, direct partition reads, own position · 1 watchdog | The two consumers ADR 0019 names. |
| Delivery | At-least-once, 1% of notifications published twice | ADR 0019: "Delivery is at-least-once." Slack and email must still post once. |
| Neighbour | Imbi's analytics streams at their real rate, then a backfill burst | The ClickHouse sink path shares the server and must not move the lane's tail. |
| Faults | Injected reader stalls; reader kill and restart; server restart mid-stream | The sequence in apache/iggy #4273: time to flag, time to move again, posted twice, missing. |
| Durability | Single node, as Imbi runs today · then replicated × 3 | Same engine either way; replication shows the cost of the extra safety. |
| Duration | 30 min per step + 24 h soak | Drift and false flags show over hours. |
| Hardware | Imbi's own Kubernetes node class, or the same instance type on LaserData Cloud | Like for like; CPU, memory and disk reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy's own tail is measured with `iggy-bench` at the same message size and rate; the kit runs alongside at each
step, so the report shows posted-once and stall-flag numbers next to p99.

**Pass line:** publish-to-reader p99 under 50 ms for both readers at 100× Imbi's rate with the analytics
neighbour running; zero missing and zero posted twice outside a reader crash; every injected stall flagged within
5 s, and no false flag in the 24 h soak.

**Published:** p99 and p99.9 vs rate per reader, duplicates skipped, time to flag per injected fault, restart
recovery, broker CPU, memory and disk, and every config and raw output.
