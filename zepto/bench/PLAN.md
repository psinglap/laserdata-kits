# Benchmark plan: the P0 hub stock lane

**Question:** at festive-peak rates, does every inventory and price change reach every P0
reader (re-validation, cart, picker) within 20 ms at p99, leaving most of a 100 ms budget for
the services themselves, on Apache Iggy and on the same lane in MSK?

Rules are fixed before any result exists. If MSK wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | 120 B | hub:sku, sequence, resulting level or price, timestamp. |
| Rate | 20K → 100K → 250K → 500K changes/s | Hundreds of hubs × thousands of SKUs, spiking on festive days. |
| Partitions | 32, keyed by hub:sku | Each SKU's changes in a hub apply in order. |
| Readers | 4 consumer groups | Re-validation, cart, picker/dispatch, and one analytics reader. |
| Durability | Iggy `replicated` × 3 · MSK `acks=all`, RF 3, min ISR 2 | Stock and price cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | MSK on its best latency settings. |
| Festive peak | 5× for 10 min | Diwali and New Year evenings. |
| P1 neighbour | 50 MB/s on a separate topic | A heavy domain must not move the P0 tail. |
| Failover | Kill the leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, MSK by the OpenMessaging Benchmark, with the same workload
shape. The kit runs alongside at each rate, so the report shows `shown-but-gone` next to p99.

**Pass line:** event-to-reader p99 under 20 ms at 250K changes/s with four readers, replicated,
through the 5× peak and with the P1 neighbour running; `shown-but-gone` under 0.1%.

**Published:** p99 and p99.9 vs rate (one line per system), the festive-peak trace, failover
recovery, broker CPU, node count and monthly cost for the lane, and every config and raw output.
