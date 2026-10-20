# Benchmark plan: the order-update lane into Meecom

**Question:** on sale day, with a 170-million-recipient campaign being queued next door, does
every order update reach Meecom's sender within 50 ms at p99, and what does the lane cost per
billion messages, on Apache Iggy and on the same lane in Confluent Cloud?

Rules are fixed before any result exists. If Confluent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | 300 B order update · 600 B campaign send | Order ID, state, channel, sequence, timestamp; a personalised template. |
| Order-update rate | 6K → 30K → 100K updates/s | 75,000 orders a minute at the 2026 sale peak, about five states per order, plus retries and headroom. |
| Campaign next door | 283K sends/s for 10 min | 170M recipients personalised in under 10 minutes, on its own topic. |
| Partitions | 32 order updates (keyed by order ID) · 64 campaign | Each order's updates stay in order. |
| Readers | Meecom's order sender, the funnel tracker, one analytics reader | Sent → Delivered → Clicked → Read reporting reads the same lane. |
| Durability | Iggy `replicated` × 3 · Confluent `acks=all`, RF 3 | An order update cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Confluent on its best latency settings. |
| Sale peak | 3× for 30 min, 5× for 10 min | 3× on regular sale days, up to 5× after festive sales (Confluent case study). |
| Failover | Kill the leader at minute 15 | Time until the sender receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Cost | Iggy: nodes in your GCP project, $ per month · Confluent Cloud: your bill for the same topics | Reported as $ per billion messages written, all readers included. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Confluent by the OpenMessaging Benchmark, with the same workload
shape. The kit runs alongside at each rate, so the report shows `out of date when sent` next to p99.

**Pass line:** order-update p99 under 50 ms at 30K updates/s with the campaign running, replicated,
through the 5× peak; `out of date when sent` under 0.01%; and a lower $ per billion messages than
the same topics on Confluent Cloud.

**Published:** p99 and p99.9 vs rate (one line per system), the sale-peak trace with the campaign,
failover recovery, broker CPU, node count, monthly cost and $ per billion messages, and every
config and raw output.
