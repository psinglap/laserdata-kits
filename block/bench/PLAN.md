# Benchmark plan: the Afterpay payment token lane

**Question:** through a scheduled card updater run, with a heavy warehouse feed on the same brokers, does every
payment token change reach every subscribing service's cache within 10 ms at p99, signed and free of card numbers,
so an instalment charged a moment later uses the current token, on Apache Iggy and on the same lane on Kafka as you
run it today?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Change size | ~260 B | Customer, sequence, kind, token ID, last four, expiry, status, timestamp, HMAC-SHA256 signature. Never a card number. |
| Rate | 1K → 5K → 20K → 50K changes/s | Card updater results arrive in batches from scheduled jobs; the upper steps are a whole updater file landing at once. |
| Partitions | 16 → 64 on payment-token-changes, keyed by customer | One customer's changes stay in order. |
| Subscribers | 2 → 10 → 50, each reading every partition | Collections, refunds and every other opted-in service; adding one must not slow the others. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A token change cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms` low, same region | Kafka on its best latency settings, with your signing and masking client libraries on both sides. |
| Peak | 10× for 10 min | A scheduled updater run. |
| Neighbour | 100 MB/s of payment events for the warehouse on a separate topic, same brokers | Afterpay's Kafka pipelines carry more than 9 TB a day, about 100 MB/s on average. |
| Failover | Kill a broker at minute 15 | Time until subscribers receive again, and the next minute's p99. |
| Rebuild | Restart a subscriber at minute 20 | Time to rebuild every customer's token from the log. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same AWS region, inside the same VPC as the payments platform | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows instalments charged on a superseded token, records that
fail the signature check and card numbers seen by subscribers next to p99.

**Pass line:** change-to-cache p99 under 10 ms at 20K changes/s with 10 subscribers, replicated, through the 10×
updater run and with the neighbour running; no record failing the signature check; no card number seen by any
subscriber; instalments on a superseded token under 1% of what a subscriber 5 s behind would send.

**Published:** p99 and p99.9 vs rate (one line per system), the updater-run trace, failover recovery, rebuild time,
broker CPU, node count and monthly cost for the lane, and every config and raw output.
