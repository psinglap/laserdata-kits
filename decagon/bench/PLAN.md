# Benchmark plan: the task progress lane

**Question:** with every live voice call running tasks behind the conversation, and a heavy conversation log on
the same brokers, does every task step reach the voice host serving that call within 10 ms at p99, on Apache
Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~200 B step record | Call, task, step and step count, running or done, what the caller can be told, sequence, timestamp. |
| Rate | 5K → 20K → 60K steps/s | 20,000 live calls with a task every 4–12 s of conversation and a step every 0.4–2.5 s is about 6,000 steps/s. The upper steps cover more concurrent calls and longer AOPs with more tool calls. |
| Partitions | 16 → 64, keyed by call | One reader per voice host, each reading only its own calls' partitions; a call's steps stay in order. |
| Readers | 8 → 64 voice hosts | The lane has to hold as the voice fleet grows; each host reads a slice, nobody reads the whole lane. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | A lost step means the caller hears the wrong thing. |
| Kafka tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, assigned partitions (no group rebalances), same region and instance class | Kafka on its best latency settings. |
| Spoken updates | One every 1–3 s per call while a task runs | Out-of-date updates counted against the same moments for both systems. |
| Peak | 3× task rate for 10 min | An outage or a storm: many callers asking about the same thing at once. |
| Neighbour | 200 MB/s of transcripts, tool traces and summaries on a separate topic, same brokers | The conversation-data pipeline must not move the lane's tail. |
| Failover | Kill a broker at minute 15; restart one voice host at minute 20 | Time until hosts receive again, the next minute's p99, and the restarted host's rebuild time from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows out-of-date spoken updates next to p99.

**Pass line:** step-to-voice-host p99 under 10 ms at 20K steps/s with 32 voice hosts, replicated, through the
3× peak and with the neighbour running; under 1% of the updates a 1 s lane would get wrong out of date.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover and host-rebuild recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
