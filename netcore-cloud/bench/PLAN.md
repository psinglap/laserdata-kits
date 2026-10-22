# Benchmark plan: the delivery status lane, one topic per status type

**Question:** with Sent, Delivered and Read webhooks on their own topics, does every Delivered
status reach its campaign reporting writer within 100 ms at p99, and land in the campaign report
within 1 s, while a Read surge several times the normal rate is held at the Read writer's cap;
how far does that hold as webhook traffic grows; and what does the lane cost per billion
webhooks, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Webhook size | 200 B status record | Message ID, campaign, status, receive timestamp. |
| Outbound rate | 4,000 messages/s | The delivery engine's rate in Netcore's 435M-message post. |
| Status webhooks | 10K → 20K → 40K webhooks/s, Sent : Delivered : Read about 4 : 3.8 : 1.6 | 10K+ TPS inbound in the same post, 20K+ in its k6 tests; 40K for headroom. |
| Provider retries | 2% of webhooks sent twice | Each status must be counted once per message. |
| Topics | status-sent 4, status-delivered 8, status-read 8 partitions, keyed by message ID | One stream per status type, as the post describes. |
| Readers | One writer per status, direct partition reads; Delivered uncapped, Read capped | Dedicated capacity for Delivered, throttled Read. |
| Read surge | Read at 5× normal for 10 min, every 30 min | A large broadcast being opened. |
| Report writes | One row per changed campaign every 250 ms, against an 8,000 writes/s ceiling | The post verified 8,000+ database writes/s. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A Delivered status cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Failover | Kill the leader at minute 15 | Time until the Delivered writer receives again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family and region | Like for like. |
| Cost | Nodes, broker CPU and monthly cost for the lane | Reported as $ per billion webhooks. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit's reporters, Kafka by the OpenMessaging Benchmark with
the same workload shape. The kit runs alongside at each step, so the report shows the share of
Delivered statuses in the report within 1 s and the Read backlog next to p99.

**Pass line:** Delivered p99 under 100 ms and 99.9% of Delivered statuses in the report within 1 s
at 20K webhooks/s, replicated, through the Read surge; the Read backlog drains at its cap; report
writes stay under the database's ceiling; and a lower $ per billion webhooks than the same lane on
Kafka.

**Published:** p99 and p99.9 vs webhook rate (one line per system), the Read surge trace, failover
recovery, broker CPU, node count, monthly cost and $ per billion webhooks, and every config and raw
output.
