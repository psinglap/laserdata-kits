# Benchmark plan: the Crons check-in lane

**Question:** with every check-in and clock pulse on the lane, the burst at the top of each minute, and an
error storm on the same brokers, does every partition's clock reach the monitor consumer within 10 ms at
p99, on Apache Iggy and on the same lane in Kafka, so that the clock ticks on the minute and no job that
checked in is marked missed?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B per check-in · ~100 B per clock pulse · ~4 KB per error event next door | Slug, status, check_in_id, monitor_config, timestamp. |
| Rate | 7M → 100M check-ins/day, with 80% of a minute's `in_progress` arriving in its first 5 s | "Over 7 million check-ins per day" in 2023 (Sentry blog); cron jobs start on the minute. Today's volume is yours to share. |
| Partitions | 128, keyed by monitor | Sentry's check-in topic has 128 partitions (#55821). The clock is the slowest of them. |
| Readers | 1 → 8 monitor consumers, partitions split between them | Each reader owns its partitions; a shared clock store as today, or each partition's clock read from the log. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A lost check-in is a false missed alert. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, LogAppendTime, same instance family | Kafka on its best latency settings, on its own small cluster. |
| Stuck partition | One partition unread for 5 min | The #55821 trade-off: the clock must hold, then backfill. |
| Neighbour | 300 MB/s of error events on a separate topic, same brokers | A customer's error storm must not move the clock. |
| Restart | Kill the monitor consumer at minute 15 | Time to rebuild partition clocks from the log; no clock jump (the #124511 failure). |
| Failover | Kill a broker at minute 20 | Time until the clock ticks again, and the next ticks' lag. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload
shape. The kit runs alongside at each rate, so the report shows missed and timeout marks (right, false,
undetected) next to p99 and tick lag.

**Pass line:** check-in p99 under 10 ms and tick lag p99 under 50 ms at 100M check-ins/day, replicated, through
the minute bursts, the error storm and the restart; no false missed or false timeout mark.

**Published:** p99 and p99.9 vs rate (one line per system), tick lag per minute, the stuck-partition and restart
traces, failover recovery, node CPU and memory, node count and monthly cost for the lane, and every config and
raw output.
