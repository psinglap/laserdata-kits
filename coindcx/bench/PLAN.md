# Benchmark plan: the CoinDCX mark price lane

**Question:** when the mark batch for every futures instrument lands on the whole second, does every mark price
and every position change reach every liquidation worker within 10 ms at p99, so the whole batch is checked
against every open position long before the next one, on Apache Iggy and on the same lane as you run it today?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Record size | ~150 B | instrument or account, sequence, mark price or position state, batch, timestamp. |
| Mark batches | 505 → 1,000 → 2,000 instruments, once a second | Every active instrument, all at once, as the partner's marks arrive. |
| Position changes | 2K → 10K → 30K changes/s | Fills, adds, reduces and closes, plus the closes liquidations cause. |
| Partitions | 16 for marks keyed by instrument, 16 for positions keyed by account | Each instrument's marks and each account's positions apply in order. |
| Readers | 4 liquidation workers, each reading every mark partition and a quarter of the positions | Cross margin needs every mark in every worker. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A position change must not come back undone. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Crash | Market falls 8% over 2 min, position changes 5× | Liquidations and the closes they cause arrive together. |
| Neighbour | 30 MB/s trade export on a separate topic | A bulk export must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until workers receive again, and the next minute's p99. |
| Rebuild | Restart one worker at minute 20 | Time to rebuild every mark and its positions from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and AWS region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate, so the report shows the USDT filled past liquidation and bankruptcy prices next to p99.

**Pass line:** lane p99 under 10 ms with four workers, replicated, through the crash and with the export running;
every mark batch checked against every position within 50 ms of arriving; a worker rebuilds marks and positions
in under a minute.

**Published:** p99 and p99.9 vs rate (one line per system), the crash trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
