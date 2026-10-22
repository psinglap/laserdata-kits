# Benchmark plan: the REV2 safety signal lane

**Question:** with the preliminary content stream and every ML signal topic keyed by content ID on
Apache Iggy, and each signals joiner owning a fixed set of partition numbers, does fully enriched content
reach REV2 within 50 ms at p99 of its last signal being written, at peak content rates, with a
Time-Travel replay on the same cluster; what join-rate does that give at each wait limit; and what does the
lane cost per billion events, on Apache Iggy and on Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Content | 1–20 KB preliminary enriched JSON per post or comment, then 50 KB | Reddit's enriched events are "fairly large JSON blobs", up to 20 MB uncompressed for deeply nested ones (Reddit Engineering, Jun 2024). |
| Signals | 3 → 8 signal topics, 100–300 B Protobuf-sized messages | Signals-Joiner joins "various signal streams" by content ID (Reddit Engineering, Aug 2024). |
| Content rate | 600 → 2,000 → 6,000 posts and comments/s | The enrichment service processes "around 600 messages per second under normal traffic" (Jun 2024); peaks are not published, so we test about 3× and 10×. |
| Inference delay | Lognormal, medians 120 ms to 900 ms; 1% of signals 6–15 s late; 0.2% never sent | ML inferencing "typically carries a much higher latency" than a rule (Aug 2024); late and missing signals are what the wait limit is for. |
| Partitions and joiners | 32 → 64 → 128 partitions per topic, 4 → 8 → 16 joiners, static ownership, direct partition reads | Content and its signals share a partition number by construction; more partitions is how joiners scale out. |
| Wait limit | 2 s, 5 s, 10 s | The trade between join-rate and how long REV2 waits for a signal that may not come. |
| Neighbour | Time-Travel replay of the content stream at 100K → 300K msgs/s | REV2's Time-Travel works through a large backlog of historical events (Reddit Engineering, Sep 2023). |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3 | A dropped signal is a missed join. |
| Joiner restart | Kill one joiner at minute 10, start it again | Join-rate and latency through the restart. |
| Failover | Kill the leader at minute 15 | Time until joiners receive again, and the next minute's p99. |
| Duration | 1 h per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same AWS region and instance family | Like for like. |
| Cost | Nodes, CPU and monthly cost for the lane | Reported as $ per billion events. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's joiners and REV2 reader; Kafka by its own perf tool with the
same workload shape. The kit runs alongside at each step, so the report shows join-rate, content past the
wait limit and late signals next to p99.

**Pass line:** p99 under 50 ms from the last signal written to the fully enriched content in REV2's hands,
at 6,000 posts and comments/s, replicated, with the replay next door; join-rate equal to the share of
signals that arrived inside the wait limit (nothing lost by the lane); and a lower $ per billion events
than the same lane on Kafka.

**Published:** p99 and p99.9 vs rate and vs partition count (one line per system), join-rate per wait
limit, restart and failover traces, CPU, node count, monthly cost and $ per billion events, and every
config and raw output.
