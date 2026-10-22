# Benchmark plan: the chart candle lane

**Question:** at Angel One's tick rates, across the real on-premise to AWS link, does every price
tick reach the candle builder within 25 ms at p99 (against ~1,000 ms metered today), through the
09:15 open, with replication on, on Apache Iggy and on today's Kafka plus MirrorMaker path? And do
the candles come out identical?

Rules are fixed before any result exists. If the Kafka path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Tick | 28 B: scrip code, timestamp, price, total volume, per-scrip sequence | The tick as "Charting at Angel One" defines it. Your real tick format runs as a second step through `kafka_bridge.py`. |
| Rate | Angel One's own peak ticks/s at the open, then 2× and 5× | Not published; an input from Angel One. The kit defaults (40,000 scrips, 20,000 ticks/s) are placeholders. |
| Partitions | 4, 8 and 16, keyed by scrip code | Each scrip's ticks stay in order, so a candle closes as soon as every partition has passed its end. |
| Path | Capture on-prem → Iggy in AWS Mumbai over the existing link; the same ticks over Kafka → MirrorMaker → Kafka on AWS | Like for like: one link, two transports. Delay metered the way Angel One meters it today: tick timestamp against the builder's clock. |
| Builders | 1, 2 and 4 candle builders, each reading every partition | A second builder (DR, a new timeframe, a watchlist tagger) must not slow the first. |
| Store | Upserts to an Aurora MySQL copy every 100 ms per changed candle | So the measured delay includes the store, as the chart sees it. |
| Durability | Iggy `replicated` × 3; Kafka with today's `acks=-1`, `max.in.flight=1` | The settings Angel One's post gives for the cross-geo hop. |
| Burst | The 09:15 open, replayed from a recorded session at 1× and 3× | The open is where the delay matters most. |
| Neighbour | Order events at 10× on another topic | A busy stream on the same nodes must not move the candle lane's tail. |
| Restart and DR | Kill a builder every 10 minutes; start a second builder in Hyderabad reading the same lane | Time until it serves the open candles again; candle digests must match the first builder's. |
| Failover | Kill the Iggy leader at minute 15 | Time until builders receive again, and the next minute's p99. |
| Duration | One full session (09:15 to 15:30) per step | Drift shows over hours. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` for the broker alone and by this kit end to end. The report shows
`late`, `gaps` and the candle digests next to p99.

**Pass line:** tick-to-candle p99 under 25 ms and p99.9 under 50 ms across the link at Angel One's
peak with 2 builders, replicated, through the open and the order burst; `late` and `gaps` 0; every
1-minute candle identical between the two builders and the current Kafka path; a restarted builder
serving every open candle within 5 s.

**Published:** p99 and p99.9 vs rate, vs partitions and vs builders (one line per system), the open
trace, restart and DR traces, failover recovery, broker CPU, node count and every config and raw output.
