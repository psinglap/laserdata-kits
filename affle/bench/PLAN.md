# Benchmark plan: the click-to-install lane

**Question:** at peak click volumes, with heavy bid and win logs on the same brokers, does every click and
install postback reach the click-to-install scorer, and every publisher block reach every attribution node,
within 10 ms at p99 per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B click or install · ~120 B block | Device ID, publisher, redirect time, node, sequence, timestamp. |
| Rate | 10K → 50K → 200K events/s | Affle reports 119.7 million CPCU conversions in Q3 FY2026, about 15 a second on average; clicks and unbilled installs are many times that, and Affle has not published them. The steps cover several times any plausible peak. |
| Partitions | 16 → 64 on click-install, keyed by device · 1 on blocks | A device's click is always ahead of its install; every node reads every block. |
| Readers | Scorer on click-install; 8 → 100 attribution nodes and bidders on blocks | The fan-out back to every node that bills or buys is the second hop and has to hold as it grows. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | Clicks decide who is paid for a conversion; blocks decide what is billed. Neither can be lost. |
| Kafka tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, same region and instance class as Affle's cluster | Kafka on its best latency settings. |
| Injection | A new publisher firing clicks on 60% of beginning installs, 0.5–4 s before the postback, for 8 s, every 10 s | Detection time, block fan-out time and conversions billed after each block, per incident. |
| Peak | 5× for 10 min | A festive-season or launch-day peak. |
| Neighbour | 200 MB/s of bid request, response and win logs on a separate topic, same brokers | The reporting pipeline must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the scorer and the nodes receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows conversions billed after a block next to p99.

**Pass line:** event-to-scorer p99 and block-to-node p99 each under 10 ms at 50K events/s with 100 nodes,
replicated, through the 5× peak and with the neighbour running; no install scored before its click; under 1% of
the conversions a 1 s lane would bill to a blocked publisher billed on this lane.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), per-incident detection and fan-out
times, the peak trace, failover recovery, node CPU, node count and monthly cost for the lane, and every config
and raw output.
