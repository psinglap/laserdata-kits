# Benchmark plan: the Soundbox payment alert lane

**Question:** at Paytm's device-merchant scale, with a bank recovering from an outage and the app events stream on
the same brokers, does every fresh payment confirmation reach the announcement service, and every announcement
reach every reader (the device push, the business app, merchant support), within 10 ms at p99 per hop, on Apache
Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~250 B status event · ~250 B announcement | Device, sequence, status, amount, time paid, transaction ID, today's total and count, language, timestamp. |
| Rate | 1M → 1.57 crore devices, a payment every 200 s each in steady state (5K → 80K confirmations/s) | Paytm's Q1 FY27 results: 1.57 crore merchants pay for its payment devices. Per-device payment rates are not published; the rate is illustrative and set with Paytm. |
| Partitions | 32 → 128 on confirmations, keyed by device · 32 on late confirmations · 1–4 on announcements | A device's payments are announced in order and its total never goes backwards. |
| Readers | Announcer workers on confirmations; the passbook on late confirmations; 3 → 30 readers on announcements | The device push to AWS IoT, the business app, merchant support, risk monitoring. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A confirmed payment cannot be lost. |
| Kafka tuning | `linger.ms=0`, tuned fetch settings, same region and instance family | Kafka on its best latency settings. |
| Peak | 3× confirmations for 10 min | A festival evening. |
| Bank recovery | 5% → 20% of devices' pending payments settled within 10 s, 15–30 minutes of them each | Late confirmations must not queue ahead of the payment at the counter now, and must not be announced. |
| Neighbour | 500 MB/s of app events on a separate topic, 3× for 10 min, same brokers | Paytm wrote of more than 5 billion events a day for behavioural analytics; the bulk stream must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the announcer and the readers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, x86 and ARM, same AWS region as the IoT path | Like for like; nodes, memory and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows fresh payments announced on time, announced twice,
stale payments announced and totals that went backwards next to p99.

**Pass line:** confirmation-to-announcer p99 and announcement-to-reader p99 each under 10 ms at 1M devices with
30 readers, replicated, through the peak, the bank recovery and with the neighbour running; no payment announced
twice; no payment older than the fresh window announced; no total going backwards; every late confirmation filed
to the passbook.

**Not measured here:** the hop from the device push to the Soundbox over AWS IoT and the mobile network. It is the
same on both sides of the comparison.
