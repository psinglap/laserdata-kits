# Benchmark plan: the safety verdict lane

**Question:** at evening-peak turn rates, with a heavy full-prompt log on the same brokers, does every chat
turn reach the trust and safety classifiers, and every verdict reach every chat server, within 10 ms at
p99 per hop, on Apache Iggy and on the same lane in WarpStream?

Rules are fixed before any result exists. If WarpStream wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~700 B turn record · ~120 B verdict | Conversation, chat server, turn sequence, turn text (user message and reply), timestamp. |
| Rate | 20K → 50K → 100K turns/s | Character.AI's inference post cites about 20,000 queries a second (2024); peaks and growth set the upper steps. |
| Partitions | 32 on turns, keyed by conversation · 1–4 on verdicts | A conversation's turns are classified in order; every chat server reads every verdict. |
| Readers | Classifier workers on turns; 50 → 500 chat servers on verdicts | The fan-out back to the serving fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · WarpStream defaults (object storage) | Verdicts are a safety record and cannot be lost. |
| WarpStream tuning | Its documented low-latency settings (shorter batch timeout, lower linger), same region | WarpStream on its best latency settings, on the same cloud. |
| Peak | 3× for 10 min | An evening surge in active conversations. |
| Neighbour | 500 MB/s of full prompts on a separate topic, same brokers | The prompt-capture pipeline must not move the lane's tail. |
| Failover | Kill a broker (Iggy) or an Agent (WarpStream) at minute 15 | Time until classifiers and chat servers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, WarpStream by the OpenMessaging Benchmark Kafka driver, with the same
workload shape. The kit runs alongside at each rate, so the report shows next turns served before their
verdict next to p99.

**Pass line:** turn-to-classifier p99 and verdict-to-chat-server p99 each under 10 ms at 50K turns/s with
500 chat servers, replicated, through the 3× peak and with the neighbour running; under 0.1% of next turns
built before their verdict, with your real turn gaps.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
