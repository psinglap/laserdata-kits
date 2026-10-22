# Benchmark plan: the MCM item status lane

**Question:** at a large retailer's scale shape, does every stock-out, restock and kill-switch block reach
every Moloco Commerce Media Decision API server within 20 ms at p99, with replication on, through a flash sale
on one category and with shopper user events at full volume on the same brokers, on Apache Iggy and on the
same lane over the transport you would otherwise use (Pub/Sub, or Kafka)? And can a Decision API server the
GKE autoscaler adds mid-sale know every item's status within seconds?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | ~90 B JSON (item, sequence, stock count, blocked reason, step flag, created time); a binary layout as a second step | The resulting status of one item, never a delta; the per-item sequence plays the role of `updated_time`: the newest wins. |
| Items | 200K items in live campaigns, then 2M and 10M | MCM takes catalogs up to 100M items; set to the share of one retailer's catalog that is in live campaigns. |
| Rate | 5K → 20K → 50K status changes/s; 60 → 600 stock-outs/s; kill-switch bulk calls of 100–1,000 items | Inputs from MCM: how often stock and blocks change on a large retailer, and how big a bulk block gets. |
| Flash sale | One category at 5× and 10× its stock updates and stock-outs for 10 minutes | A deal drop sends shoppers to the same items at once. |
| Partitions | 8, 16, 32, keyed by item (or item and seller, MSPI) | Each item's changes stay in order. |
| Readers | 8, 32 and 128 Decision API servers, each reading every partition | Every server needs the whole table; adding servers must not slow the others. |
| Retention | 12 hours | A kill-switch block lasts 12 hours; rebuilds and replay; a rolling snapshot once a minute keeps a rebuild to about a minute of log. |
| Durability | Iggy `replicated` × 3; Kafka `acks=all`, RF 3; Pub/Sub as provided | Like for like. |
| Neighbour | Shopper user events (HOME, SEARCH, ITEM_PAGE_VIEW, ADD_TO_CART, PURCHASE) at production MB/s on the same brokers, 4× during the sale | Bulk event traffic must not move the lane's tail. |
| Scale-out | Add one Decision API server every 5 minutes, one during each sale | Time until it knows every item, and its first minute's p99. |
| Failover | Kill the Iggy leader at minute 15 | Time until servers receive again, and the next minute's p99. |
| Duration | 2 hours across a retailer's evening peak per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes on GKE or Compute Engine, in the region that serves the retailer | Nodes, cores and cost per million status changes reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Kafka by `kafka-producer-perf-test` and `kafka-consumer-perf-test`, and Pub/Sub
by a publisher and one subscription per Decision API server, with the same message size and fan-out. The kit
runs alongside at each rate, so the report shows ads for unavailable items per million decisions and `gaps`
next to p99.

**Pass line:** status-to-server p99 under 20 ms and p99.9 under 40 ms at 20K changes/s with 32 servers,
replicated, through a 10× sale and the user-event burst; `gaps` 0; an added server knowing every item's status
within 5 s.

**Published:** p99 and p99.9 vs rate and vs server count (one line per system), ads for unavailable items per
million decisions, the sale trace, the scale-out trace, failover recovery, broker CPU, node count and every
config and raw output.
