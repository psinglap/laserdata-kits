# Benchmark plan: the real-time tracking lane

**Question:** at LinkedIn's peak rates for the latency-sensitive tracking category, does every signal reach
real-time processing within 10 ms at p99 on Apache Iggy, durably logged and replayable, while a bursty media
category runs on the same brokers, and what does the same lane cost on Kafka with its best latency settings?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Signal size | 200 B | Pseudonymized member ID, sequence, event type, platform, app version, timestamp. |
| Rate | 50K → 200K → 500K → 1M signals/s | A slice of "millions of events per second at peak"; LinkedIn has not published the latency-sensitive share, so the evaluation finds where the lane bends. |
| Partitions | 64, keyed by pseudonymized member ID | Each member's signals apply in order. |
| Readers | 8 real-time processing instances, direct partition reads | One owner per partition, offsets stored once a second. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A signal is acknowledged only when the log holds it. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Peak | 3× for 10 min | The busiest hour of the week. |
| Media neighbour | 100 MB/s of render, play, pause and exit events on their own topic | A bursty category must not move the latency-sensitive tail. |
| Restart | Stop one reader for 30 s at minute 10 | Signals held, time to catch up, lost (must be 0). |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows lost and held next to p99.

**Pass line:** signal-to-processing p99 under 10 ms at 500K signals/s with eight readers, replicated, through the
3× peak and with the media neighbour running; zero signals lost through a reader restart and a leader failover.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, restart and failover recovery, broker
CPU, node count and monthly cost for the lane as $ per billion signals, and every config and raw output.
