# Live Fulfilled by Faire stock at checkout, on LaserData

Every FBF receipt, pick, cycle count and reservation reaches your multi-brand checkout in milliseconds, even at a
Faire Market peak. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Faire_FBFStock.pdf`](LaserData_Faire_FBFStock.pdf)

## Why this use case

At Faire's November 2025 Toronto Tech Talk, the Fulfilled by Faire pod described the service as "storing
participating brands' inventory in a Faire-operated warehouse and fulfilling orders on their behalf", so retailers
get "a single minimum and consistent delivery". The team "built the integrations needed to sync products, orders,
shipments, and inventory" with a 3PL. The pilot "processed more than 15,000 orders for 41 brands" in six months and
"has now grown into two full engineering pods focused on scaling the service and improving the multi-brand checkout
experience" (The Craft, "Building beyond code", December 2025).

A multi-brand checkout is only as good as the stock view behind it. If that view is refreshed by a periodic sync,
a receipt, a pick, a cycle count or another retailer's reservation stays invisible until the next sync, and a cart
built on it can accept case packs the warehouse no longer has. One line of a single-shipment order is then short,
after the retailer has checked out. This kit runs the one lane that removes the wait: every FBF stock change, keyed
by brand:SKU, with the resulting levels, pushed into the checkout's view as it happens. It prints the lane's p99
every second, and the lines a periodic sync would have accepted that the warehouse can't fill, judged against the
same live state.

Faire's data platform is building "the CDC and streaming ingestion layer from CockroachDB changefeeds and MySQL
binlogs through Kafka into Iceberg tables on S3" (Staff Data Infrastructure Engineer posting, 2026). The bridge in
this kit reads that kind of changefeed, read-only, and the CDC flow to Iceberg runs next to the lane as its heavy
neighbour.

## Run it

Needs Docker and Python 3.10+. Defaults: 500 brands × 150 SKUs in case packs, 200 stock changes a second, and 20
multi-brand checkouts a second (illustrative; Faire has not published FBF SKU counts, change rates or order rates
since the 41-brand pilot).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python warehouse.py &                # FBF stock per brand:SKU, and the order service's answer to each checkout
python checkout.py                   # the live stock view + retailers checking out multi-brand carts on it
```

Both print a line a second:

```
[checkout] … changes/s  change-to-checkout p50 …ms  p99 …ms  PASS (<100ms)  carts …  orders …  lines …
           a 300s inventory sync would have accepted … lines the warehouse can't fill (…% of carts)
[warehouse] stock changes …  checkout orders …  lines reserved …  short at the warehouse … (…%)
```

**What the numbers mean.** The kit times each change from the moment `warehouse.py` creates it to the moment
`checkout.py` has applied it, through Python clients, on whatever machine runs it. On a shared 2-vCPU GitHub runner
with the broker and all three scripts on one box (150 brands × 120 SKUs, about 1,140 stock changes/s, a Faire Market
burst to about 2,500/s for 15 s, 60 multi-brand checkouts/s, 2 MB/s of lakehouse CDC next door), with the checkout
reading every partition directly, it held p50 4.1–8.5 ms and p99 15.5–20.7 ms over three green runs (p99 15.3–20.8 ms
in the Market seconds, worst second 16.4–41.8 ms), with no second over the 100 ms budget in 171. The warehouse marked
0.01–0.02% of reserved lines short, the units another order took in the milliseconds between checkout and
reservation.

A 60 s periodic sync would have accepted at least one line the warehouse can't fill in about 60–63% of carts in CI.
That figure comes from CI compressing churn: each SKU changes about every 16 s there, far more often than in a real
warehouse. The share grows with the sync interval and with how often a SKU changes, so read it as the mechanism, and
run the kit on your own change rate and sync interval for your number. That is the end-to-end lane through this demo
code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on
dedicated machines.

| File | What it is |
|---|---|
| `warehouse.py` | On hand, reserved, available and case pack per `brand:SKU`, published to `fbf-stock` with a per-SKU sequence and the resulting levels. Reserves each checkout line or marks it short against the true levels. `--market-x` multiplies other retailers' orders for a Faire Market. |
| `checkout.py` | The live stock view from the lane, retailers checking out multi-brand carts in whole case packs on it, and a periodic sync of the same view for comparison. |
| `lake_cdc.py` | A heavy, latency-tolerant stream (CockroachDB and MySQL rows on their way to Iceberg) on its own topic, for the isolation test. |
| `changefeed_bridge.py` | Copies the CockroachDB changefeed of your FBF inventory table from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A Faire Market:** `python warehouse.py --market-x 5 --market-at 60 --market-for 120`.
   Pass: change-to-checkout p99 under 100 ms, and short lines during the burst within their range before it.
2. **Lakehouse CDC at full volume:** `python lake_cdc.py --mbps 50` while both run.
   Pass: the FBF stock lane's p99 moves by less than 10%.
3. **Rebuild the stock view:** stop `checkout.py` and start it again; it replays the stock topic.
   Pass: it catches up within seconds and shows the right available cases for every SKU.

## Shadow your own traffic

```
pip install confluent-kafka
python changefeed_bridge.py --bootstrap <brokers> --kafka-topic <fbf-inventory-changefeed-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=… -X sasl.password=…
```

The bridge reads as its own consumer group, `iggy-shadow`; the changefeed, the services that write the table, and
every existing consumer are untouched. It expects the default wrapped JSON envelope with the `updated` option, and
uses each row's commit timestamp as the per-SKU sequence, so the duplicates a changefeed can redeliver are dropped.
Map your column names in `to_change()`. `--ns-from-commit` times latency from the row's commit in CockroachDB. The
bridge starts from the latest record, so the view fills as SKUs change; seed it from a snapshot if you need every
SKU at once.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so this is one lane beside your Kafka, not a replacement for it,
and the CDC path to Iceberg stays on Kafka. Delivery is at-least-once; the per-SKU sequence makes replays and retries
safe. Replicated clusters shipped in September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored FBF inventory changefeed, beside Kafka, read
by one checkout reader in shadow. No advantage, no migration.

kranti@laserdata.com · laserdata.com
