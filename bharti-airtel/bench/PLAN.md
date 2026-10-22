# Benchmark plan: the Airtel domain verdict lane

**Question:** at the scale shape of Airtel's Fraud Detection Solution, does every new verdict on a domain
(block, or a block lifted) reach every enforcement point (each DNS resolver node and the SMS link scanner)
within 25 ms at p99, with replication on, through a smishing campaign and with call and SMS records at full
volume on the same brokers, on Apache Iggy and on the same lane over the transport you would otherwise use?
And can a resolver node added mid-campaign hold every verdict within seconds?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | ~110 B JSON (domain, sequence, verdict, score, source, created time); a binary layout as a second step | The resulting verdict of one domain, never a delta. |
| Domains | 500K, then 2M and 5M | The database holds 187K+ blocked domains (GSMA, Oct 2025) plus every domain scored clean. |
| Rate | 3K → 10K → 30K verdicts/s, 5 to 50 flips/s | Inputs from your team: how many unknown domains partner APIs score per second, and how often a verdict flips. |
| Campaign | 10× and 50× the flips to block for 5 minutes, all from the SMS link scan | One SMS blast carrying a fresh set of links. |
| Partitions | 8, 16, 32, keyed by domain | Each domain's verdicts stay in order. |
| Readers | 8, 32 and 128 enforcement points, each reading every partition | Every resolver needs every verdict; adding one must not slow the others. |
| Retention | 6 hours, then 7 days | Rebuilds, replay and audit of which verdict was in force when; a rolling snapshot keeps a rebuild to about a minute of log. |
| Durability | Iggy `replicated` × 3; Kafka `acks=all`, RF 3 | Like for like. |
| Neighbour | Call and SMS records at production MB/s on the same brokers, 4× during the campaign | Bulk analytics traffic must not move the lane's tail. |
| Scale-out | Add one resolver every 5 minutes, one during each campaign | Time until it holds every verdict, and its first minute's p99. |
| Failover | Kill the Iggy leader at minute 15 | Time until resolvers receive again, and the next minute's p99. |
| Duration | 2 hours across an evening peak per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on Airtel Cloud or your own data centre, on the machine type your resolvers' neighbours use | Nodes, cores and cost per million verdicts reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test` with
one consumer per resolver, with the same message size and fan-out. The kit runs alongside at each rate, so the
report shows clicks let through, legit lookups still blocked and `gaps` next to p99.

**Pass line:** verdict-to-resolver p99 under 25 ms and p99.9 under 50 ms at 10K verdicts/s with 32 resolvers,
replicated, through a 50× campaign and the record burst; `gaps` 0; an added resolver holding every verdict
within 5 s.

**Published:** p99 and p99.9 vs rate and vs resolver count (one line per system), clicks let through per
million lookups, the campaign trace, the scale-out trace, failover recovery, broker CPU, node count and every
config and raw output.
