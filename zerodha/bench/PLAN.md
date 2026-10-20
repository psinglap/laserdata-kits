# Benchmark plan: the tick lane

**Question:** at Zerodha's rates, does every exchange tick reach every ticker node within 5 ms at
p99 on the broker hop, through a 10× burst, with replication on, on Apache Iggy and on the same
hop over NATS as Zerodha runs it today? And can a ticker node that restarts mid-session serve every
quote again within seconds?

Rules are fixed before any result exists. If NATS wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Tick size | 56 B | Kite's 44-byte `quote` packet, a per-instrument sequence and a created timestamp. A `full` packet (184 B, 5-level depth) is run as a second step. |
| Rate | 20K → 100K → 250K → 500K ticks/s into the lane | The rate before fan-out is an input from Zerodha; "tens of millions" of packets a second leave the ticker after fan-out (Zerodha Tech Blog, 2021). |
| Partitions | One or more per exchange segment, keyed by instrument token | Each instrument's ticks stay in order; the segment is in the token's low byte. |
| Readers | 4, 8 and 16 ticker nodes, each reading every partition | Every node needs every tick; adding nodes must not slow the others. |
| Retention | One trading day | Rebuilds and replay; a rolling snapshot of every quote once a minute keeps a rebuild to about a minute of log. |
| Durability | Iggy `replicated` × 3; NATS as deployed today (core or JetStream) | Like for like with what the ticker hop runs on now. |
| Burst | 10× for 10 s, at the open and mid-session | "A 10x burst in traffic over a period of 10 seconds is quite common" (Zerodha Tech Blog, 2020). |
| Neighbour | Order postbacks at 10× on another topic | A busy stream on the same nodes must not move the tick lane's tail. |
| Restart | Kill one ticker node every 5 minutes | Time until it serves every quote again, and its first minute's p99. |
| Failover | Kill the Iggy leader at minute 15 | Time until readers receive again, and the next minute's p99. |
| Duration | One full session (09:15 to 15:30) per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on the machines the NATS cluster uses, or the same instance family in Zerodha's DC | Nodes, cores and power per million ticks reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; NATS by `nats bench` with the same message size and fan-out. The
kit runs alongside at each rate, so the report shows `shown after a newer tick existed` and
`gaps` next to p99.

**Pass line:** feed-to-node p99 under 5 ms and p99.9 under 10 ms at 250K ticks/s with 8 nodes,
replicated, through the 10× burst and with the postback burst running; `gaps` 0; a restarted node
serving every quote within 5 s.

**Published:** p99 and p99.9 vs rate and vs node count (one line per system), the burst trace, the
restart trace, failover recovery, broker CPU, node count and every config and raw output.
