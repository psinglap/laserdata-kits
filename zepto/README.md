# Real-time hub stock and prices for Zepto, on LaserData

Every stock and price change from every dark store reaches re-validation, cart and the picker
app in milliseconds, even at Diwali peak. It runs beside MSK, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/Cr4ZXH3fpKwyiU4bxGD2ew
- **One slide:** [`LaserData_Zepto_HubStock.pdf`](LaserData_Zepto_HubStock.pdf)

## Why this use case

From Zepto's own AWS Big Data Blog post (Aug 2026): search events flow through MSK topics per
domain (Catalog, Inventory, Pricing, Offers), and the OpenSearch index refreshes every 10 seconds.
That works because every result is re-validated against live per-hub stock after retrieval.

So the number that decides how many shoppers tap an item the hub just sold is the latency of the
inventory and pricing stream that re-validation reads. This kit runs that one lane on Apache Iggy
and prints its p99 every second.

## Run it

Needs Docker and Python 3.10+. Defaults: 300 hubs × 2,000 SKUs, 5,000 changes/s, a search index
refreshed every 10 s, and 200 searches/s.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python hubs.py &                     # stock, price and order acceptance for every hub
python revalidate.py                 # 10 s index + re-validation + shoppers
```

Both print a line a second:

```
[revalidate]  5,000 changes/s  event-to-live-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)
              index results already gone, caught by re-validation …%
[hubs] orders accepted …  rejected as sold out …  shown-but-gone …%
```

| File | What it is |
|---|---|
| `hubs.py` | Stock and price per `hub:sku`, published to the `inventory` and `pricing` topics with a per-key sequence and the resulting value. Accepts or rejects orders against true stock. |
| `revalidate.py` | A search index refreshed every `--refresh` s, re-validated against a live view kept from the two topics. Orders one surviving result per search. |
| `p1_flood.py` | A heavy P1 domain (tag and embedding updates) on its own topic, for the isolation test. |
| `msk_bridge.py` | Copies an existing MSK topic into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Festive peak:** `python hubs.py --festive 5 --festive-at 60 --festive-for 120`.
   Pass: event-to-live-view p99 under 100 ms, and `shown-but-gone` under 0.1%.
2. **A P1 domain at full volume:** `python p1_flood.py --mbps 50` while both run.
   Pass: the P0 lane's p99 moves by less than 10%.
3. **Rebuild a reader:** stop `revalidate.py` and start it again; it replays both topics from the start.
   Pass: it catches up within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-brokers> --kafka-topic <inventory-topic> --iggy-topic inventory \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; producers, Flink jobs and existing consumers are untouched. Map your record
shape in `to_msg()`: the kit needs `k` ("hub:sku"), `seq`, and `level` or `price`.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored inventory topic, alongside
MSK. No advantage, no migration.

kranti@laserdata.com · laserdata.com
