# Benchmark plan: the subscriber decision lane

**Question:** with shopping events keyed by subscriber and every partition owned by exactly one
decision worker, does every event reach its worker within 50 ms at p99 at Black Friday rates, with
a 25,000 texts/s campaign blast on the same cluster; does every shopping burst still queue at most
one text per subscriber, through a worker restart and a leader failover; and what does the lane cost
per billion events, on Apache Iggy and on the same lane in Pulsar?

Rules are fixed before any result exists. If Pulsar wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | 250 B shopping event | Subscriber ID, brand, event name, per-subscriber sequence, timestamp. |
| Event rate | 30K → 60K → 120K events/s | "Over 10 billion events" across a BFCM weekend is about 29K/s on average; flash sales "roughly doubled" traffic (Attentive blog). |
| Subscribers | 20M → 50M active, Zipf-skewed, sessions of 5 events 50–600 ms apart, 40% with 2+ triggers | Bursts are where two consumers would each send a text (Attentive Tech Blog, Apr 2025). |
| Partitions and workers | 64 → 128 → 256 partitions, 4 → 16 → 32 workers, static ownership, direct partition reads | One owner per subscriber by construction; more partitions is how ownership scales up for BFCM. |
| Campaign blast next door | 25K → 50K campaign texts/s, 400 B | "A minimum rate of around 25,000 messages per second" on the send path (Attentive engineering post). |
| Durability | Iggy `replicated` × 3 · Pulsar `ackQuorum` 2 of 3, KEY_SHARED and FAILOVER both measured | A decided text cannot be lost. |
| Pulsar tuning | Batching on and off, key-based batching for KEY_SHARED, receiver queue tuned | Pulsar on its best latency settings for each subscription type. |
| Worker restart | Kill one worker at minute 10, move its partitions to another, stop-then-start | Duplicate texts and the time until its subscribers are decided again. |
| Failover | Kill the leader at minute 15 | Time until workers receive again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side (plus bookies for Pulsar), same AWS instance family and region | Like for like; Pulsar's BookKeeper nodes are counted in its cost. |
| Cost | Nodes, CPU and monthly cost for the lane | Reported as $ per billion events. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's workers and send audit, Pulsar by its own perf tool
with the same workload shape. The kit runs alongside at each step, so the report shows duplicate
texts, subscribers decided by two or more workers, and order violations next to p99.

**Pass line:** p99 under 50 ms event-to-decision at 60K events/s, replicated, with the campaign
blast next door; zero duplicate texts and zero subscribers decided by two workers, including through
the restart and the failover; and a lower $ per billion events than the same lane on Pulsar.

**Published:** p99 and p99.9 vs rate and vs partition count (one line per system and subscription
type), duplicate texts per run, restart and failover traces, CPU, node count, monthly cost and $ per
billion events, and every config and raw output.
