# Real-time item availability for Moloco Commerce Media, on LaserData

When an advertised item sells out or a retailer blocks it with the Item Kill Switch, every Decision API
server knows in milliseconds, in order per item with the newest change winning, so no decision returns an
item a shopper cannot buy. A server the GKE autoscaler adds mid-sale rebuilds every item's status from the
log. It runs beside Pub/Sub, Kafka and the catalog feeds, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Moloco_ItemStatus.pdf`](LaserData_Moloco_ItemStatus.pdf)

## Why this use case

From Moloco's own sources:

- The Item Kill Switch "is designed to immediately stop specific items from being advertised … particularly
  useful when items are out of stock or need to be hidden due to fraudulent product reports"; a blocked item
  "is immediately pulled from ad serving, ensuring the Decision API does not return these items"
  ([MCM docs: Item Kill Switch](https://mcm-docs.moloco.com/docs/item-kill-switch)). A block lasts 12 hours;
  a newer in-stock catalog row with a later `updated_time` lifts it; `UpdateItemStatusBulk` is limited to 5
  QPS per platform per user ([Quotas & Limits](https://mcm-docs.moloco.com/docs/limits)).
- Catalog `availability` set to out_of_stock "prevents the item from being advertised"; the full feed is
  "typically imported daily" (or every 4 hours under 10 GB), with an optional incremental feed, for example
  hourly ([Catalog feed](https://mcm-docs.moloco.com/docs/catalog-feed),
  [SSPI specification](https://mcm-docs.moloco.com/docs/sspi-catalog-feed-specification)).
- Decision API processing runs real-time ML inference at "p95 latency of around 100ms on our server side",
  and MCM is "entirely hosted on Google Cloud"
  ([Ad serving end to end latency](https://mcm-docs.moloco.com/docs/ad-serving-e2e-latency)); GKE and its
  autoscaler "increase and decrease the infrastructure capacity in minutes"
  ([Google Cloud customer story](https://cloud.google.com/customers/molocokr)).
- The data stack: "BigQuery, Bigtable, Redis, GCS, Kafka, Pub/Sub", for "reliable and low-latency delivery
  of data across the ads ecosystem" ([Applied Data job post](https://job-boards.greenhouse.io/moloco/jobs/7519526003), 2026).
- Costco selected MCM "to power onsite advertising across Costco.com" (Moloco blog, March 2026).

When a deal drop sends shoppers to the same items at once, stock runs out item by item, in seconds. Until a
Decision API server knows an item is gone, it can still return it in a sponsored slot; the click lands on a
sold-out page and the advertiser pays for it. This kit carries one signal, the status of each item in a live
campaign, from the status service to every Decision API server on Apache Iggy, and prints its p99 every
second next to the cost of being late: ads returned for items that were no longer available. The log adds
rebuilds: a server added during the sale reads the last minute of the log and knows every item, including
ones that have not changed for hours.

We don't know how item status reaches your Decision API servers today, or how fast; "immediately" is your
word and the kit's "1 s behind" figure is a comparison, not a claim about your system.

## Run it

Needs Docker and Python 3.10+. Defaults: 16 categories × 12,500 items (200,000 items in live campaigns),
5,000 stock updates a second, 60 stock-outs and 50 restocks a second, an Item Kill Switch bulk call blocking
100 items every 10 s, a flash sale on electronics from second 60 for 30 s (5× its stock updates and
stock-outs), and Decision API servers that each answer 2,000 decisions a second with 10 ad items each, at 1%
CTR and $0.60 CPC. All illustrative: set them to yours.

```
docker compose up -d                        # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt             # apache-iggy 0.9.0
python status.py &                          # the item status service: one writer per partition
python decision_node.py --name dapi-1 &     # a Decision API server
python decision_node.py --name dapi-2       # and another: each reads every change
```

Each prints a line a second:

```
[dapi-1]  5,000 status changes/s  status-to-server p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)
          stock-outs 60  blocks 0  restocks 50  ads for unavailable items …/s (… per 1M decisions),
          clicks …, $… charged  missed …  1 s behind …/s, $…  gaps 0  items 200,000
[status]  5,110 status changes/s  stock-outs 60  restocks 50  kill-switch blocks 0  … writes
          write ack p50 …ms  p99 …ms  flash sale: electronics x5
```

- **ads for unavailable items:** ad items this server returned after the item sold out or was blocked,
  before it had the change (latency × this server's ad rate on one item), per second and per million
  decisions; **clicks** and **$ charged:** those ads at `--ctr` and `--cpc`. **missed:** ad slots a restocked
  item could have had before the server knew it was back. **1 s behind:** the same changes if the server
  learned of every one a full second late. The estimate spreads ads evenly over items; items that sell out
  in a sale are usually the popular ones, so it understates both sides.
- **blocks:** items a kill-switch bulk call made ineligible this second.
- **gaps:** a missing sequence number for an item; must stay 0.

**What the numbers mean.** The kit times each change from the moment the status service made it (including
its wait for the next write) to the moment `decision_node.py` has applied it, through Python clients, on
whatever machine you run it on. In CI, on a shared 2-vCPU GitHub runner with the broker, the status writers,
three Decision API servers and the user-event stream all on one box, at about 2,080 status changes/s over
80,000 items (16 categories × 5,000), with 40 stock-outs a second and a 50-item kill-switch call every 5 s,
three green CI runs held a median per-second p50 of 3.3 to 4.0 ms, p99 5.7 to 7.8 ms and p99.9 6.3 to 8.9 ms
on each server; 367 of 368 steady-state server-seconds passed the 20 ms budget, and the one slow second
(p99 27 ms) came as the added server replayed about 115,000 changes on the same two CPUs. The flash sale
(status changes up from about 2,070 to 2,580 a second) with a 4× user-event burst next door (to 4 MB/s)
moved the median p99 by −0.1 to +0.4 ms. At 2,000 decisions a second per server, a median of 0.036 to 0.044
ads a second went to items already gone (about 18 to 22 per million decisions, 3 to 4 per server over a
69-second run); had every change arrived a full second later, about 10 a second, some 880 per run. The
server added as the sale opened rebuilt all 80,000 items from the last 20 s of the log (about 112,000 to
115,000 changes) in 0.62 to 1.32 s; 10 s of live changes alone would have covered 22% of them. No sequence
gaps. That is the end-to-end lane through this demo code, not the broker alone; the broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `status.py` | The item status service: per item, stock count and block reason, written on every change with a per-item sequence; stock-outs (OUT), restocks (BACK), Item Kill Switch bulk calls, a flash sale on one category, and a rolling snapshot of every item once a minute. One writer per partition, each writing every 2 ms. |
| `decision_node.py` | A Decision API server: reads every partition with its own offsets, keeps the live item table, counts ads for unavailable items, clicks and dollars at its decision rate, checks sequence gaps. Rebuilds from the last `--replay-s` seconds of the log on start. |
| `user_events.py` | Shopper user events in the MCM Event service shape (HOME, SEARCH, ITEM_PAGE_VIEW, ADD_TO_CART, PURCHASE; ~315 B records) on their own topic, with a burst, for the isolation test. |
| `status_bridge.py` | Reads your item status records from Cloud Pub/Sub (its own subscription, `iggy-shadow`) or Kafka (its own consumer group, `iggy-shadow`), keeps the newest-`updated_time`-wins rule, and writes changes into Iggy, for a shadow run on real traffic. |
| `ci.sh` | The CI run: two servers, kill-switch calls, a flash sale with a 4× user-event burst next door, a third server added as it opens; pass only on 95% PASS seconds per server, zero gaps, stock-outs and blocks counted, and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The big sale:** `python status.py --rate 20000 --outs 240 --kill-items 1000 --sale-x 10`.
   Pass: status-to-server p99 stays under 20 ms through the sale and `gaps` stays 0.
2. **More Decision API servers:** start a third and a fourth `decision_node.py`.
   Pass: the first server's p99 moves by less than 10%; every server reports the same change count.
3. **The autoscaler adds a pod mid-sale:** start `decision_node.py --name dapi-9` while the `flash sale`
   line shows. Pass: `rebuilt from the last 75 s of the log … status for 200,000 of 200,000 items` within
   seconds, and the line after it is PASS. Compare with the `without the log` line.
4. **Shopper events next door:** `python user_events.py --mbps 20 --x 4` while the rest runs.
   Pass: the lane's p99 moves by less than 10%.

## Shadow your own item status

```
pip install google-cloud-pubsub            # or: pip install confluent-kafka
gcloud pubsub subscriptions create iggy-shadow --topic=<item status topic> --message-retention-duration=1h
python status_bridge.py --project <gcp project> --item-field item_id --availability-field availability \
  --blocked-field blocked --time-field updated_time
# or, from Kafka: python status_bridge.py --kafka <bootstrap> --topic <item status topic>
python decision_node.py --name shadow --replay-s 0
```

Point the bridge at wherever item status already flows: incremental catalog rows after import, kill-switch
requests, or a retailer's stock feed. It reads through its own subscription or consumer group, so the
services that publish and every existing reader are untouched. For an MSPI catalog, add
`--seller-field seller_id` so status is kept per item and seller. Map other fields in `to_record()`. In a
shadow run the latency `decision_node.py` prints starts at the bridge.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, new in September 2026. Three nodes tolerate one
  failure. It is young; the evaluation's failover row is there to test it.
- **Delivery:** at-least-once. The per-item sequence makes repeats harmless and gaps visible.
- **Replay:** polling from an offset or a timestamp, which is how a server rebuilds; per-topic expiry (12
  hours here, the length of a kill-switch block). There is no log compaction, which is why the status
  service writes a rolling snapshot.
- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9. The lane does not need it; the
  bridge reads Pub/Sub or Kafka as one more subscriber.
- **Managed service on Google Cloud:** none from Google; you run the broker (one Rust binary or a container,
  on GKE or Compute Engine), with LaserData's support.
- **Clients:** official SDKs for Rust, Go, Java, Python, C# and Node.js.
- **Footprint:** a single Rust binary, Apache-licensed: thread-per-core, no garbage collector.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one retailer's mirrored item status (one category
is enough), alongside your current path. No advantage, no migration.

kranti@laserdata.com · laserdata.com
