# Real-time Zone Deals inventory for Gametime, on LaserData

Every listing change reaches every Zone Deals seat picker in milliseconds, even in an event's final hour, so the
seats Gametime picks are still there at the price the fan paid. It runs beside Confluent Kafka, so nothing else
has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.gametime-kit
- **One slide:** [`LaserData_Gametime_ZoneDeals.pdf`](LaserData_Gametime_ZoneDeals.pdf)

## Why this use case

From Gametime's own blog and careers page:

- Zone Deals: fans "buy tickets in a general area of a venue rather than picking out a specific row", and with
  Zone Deals 2.0 "let Gametime select your exact seats in a designated Zone". "You pick the Zone. We pick the
  seat. You save big." (Gametime blog, September 2023 and March 2025)
- Sellers: "individual fan sellers and high-volume professional sellers", with "Direct API connectivity for
  real-time pricing and availability". Sellers can opt into automatic pricing, which "updates ticket prices based
  on how the market is moving", the time left and comparable seats, never under the seller's "minimum price".
  (Gametime blog, December 2025 and September 2023)
- The last hour: "some of our best deals come in the last 90 minutes before the start time", and LastCall sells
  "up to 90 minutes after an event begins". (Gametime blog, September 2023)
- Engineering: "from real-time pricing engines to sub-second checkout"; the data platform runs "Kafka
  (Confluent)" on AWS. (Careers page, 2026; Senior Data Engineer job post)

A Zone Deal is a promise made before the seats are chosen: the fan pays the zone's price, then Gametime picks the
listing. That pick is right only if the picker's view of the zone is current, and in the final hour the zone moves
fastest: sellers sell the same tickets on other channels, automatic pricing reprices the whole event, sellers pull
or add tickets. This kit runs that one feed as a lane on Apache Iggy: every listing's state (tickets left, price,
seller floor), partitioned by event and zone, read by every seat picker directly, kept for six hours so a new or
restarted picker rebuilds its pool from the log. It prints the lane's p99 every second, re-picks (seats picked on a
listing that was already sold out or above the deal price), and what a pool synced every 3 seconds would have done
with the same orders, judged against the same listing state.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000 events × 8 zones × 30 seller listings (the hottest event a venue
four times that size), 60% of listings on automatic pricing, 2,000 listing changes a second and 80 Zone Deals
orders a second.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python listings.py &                 # sellers, automatic pricing, fans, and checkout's check
python zone_picker.py --name zp-1 --shard 0 --shards 2 &   # a seat picker: half the zones' orders
python zone_picker.py --name zp-2 --shard 1 --shards 2     # the other half; each holds every zone
```

They print a line a second:

```
[zp-1]  2,100 changes/s  change-to-pool p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  zone orders picked …  no seat …
[listings] 2,100 changes/s  zone orders … picked … (steady)  re-picks … (sold out … over the deal price …) no seat …
        a pool refreshed every 3s would re-pick … (sold out … over the deal price …) no seat …
```

**What the numbers mean.** The kit times each change from the moment `listings.py` creates it (including its wait
for the next write) to the moment a `zone_picker.py` has applied it to its pool, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box
(18,450 listings in 720 zones, 600 listing changes a second, 60 Zone Deals orders a second, one event's final hour
for 15 s with 300 extra changes and 180 extra orders a second on it, 2 MB/s of app events next door, two pickers,
one restarted), the medians of the steady-state seconds over three green runs on the final code were p50
4.8–8.8 ms, p99 12.2–19.9 ms and p99.9 16.6–24.3 ms on each picker, at about 650 listing changes a second. Every
steady-state second passed the 50 ms budget (65 of 65 and 53 of 53 per picker in all three runs); the worst second
reached 34.8–41.8 ms. The restarted picker rebuilt its pool from the log (18,450 listings, about 37,200 changes) in
0.18–0.37 s before it took an order. Of 4,233–4,300 picks checked against the listings after warm-up, 25–32
(0.58–0.75%) were re-picks, all on listings that sold out within milliseconds of the pick, nearly all in the final
hour; the same pickers' pool copies synced every 3 s, judged against the same listing state, would have re-picked
97–148 (2.3–3.5%) and found no seat for 30.6–30.8% of orders (4.2–5.7% for the live pool). In the final hour the
synced pool's bigger miss is the price drop itself: it found no seat for 62.0–63.8% of orders, against 9.6–12.9%
for the live pool, because the listings automatic pricing had just moved under the deal price were not in it yet.
Order-to-pick was p50 7.0–9.8 ms and p99 12.6–17.1 ms. 43–49 orders a run went unpicked: they were sent to the
restarted picker's zones while it was down, and are counted apart from latency. That is the end-to-end lane
through this demo code at reduced scale, not the broker alone, and the comparison is the kit's model, not a
Gametime figure. One Python reader tops out near 4,000 changes/s on that runner; past it, the reader falls behind,
not the broker. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated
machines.

| File | What it is |
|---|---|
| `listings.py` | Sellers, automatic pricing and fans. Publishes each listing's state to `zone-listings` with a per-listing sequence and the resulting state (tickets left, price, floor, Zone Deals tickets processed), partitioned by event:zone; sends Zone Deals orders keyed by event:zone, only while the zone shows a deal; checks every pick against the listing at that moment. `--final-hour` runs one event's last-minute window. |
| `zone_picker.py` | A seat picker: reads every partition of the lane directly into its pool, rebuilds from the log on start, takes the orders of its share of zones (`--shard` of `--shards`), picks the cheapest listing with enough tickets at or under the deal price, and keeps a copy of its pool refreshed every 3 s for comparison. |
| `app_events_flood.py` | A heavy, latency-tolerant stream (event views, searches, zone views, price alert follows) on its own topic, for the isolation test. |
| `confluent_bridge.py` | Copies a Confluent topic that carries your listing changes into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `ci.sh` | The CI run: 18,450 listings, one event's final hour, an app-events neighbour, two pickers and a restart. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The market is a model, not Gametime data: listing counts, prices, seller floors, the share on automatic pricing,
sales on other channels, Zone Deal prices (a cheap third of each zone), the cheapest-first pick and the 3-second
comparison pool are the kit's assumptions. How Zone Deals picks seats today is not public; set `--refresh-s` on
`zone_picker.py` to what your inventory sync does.

## Three tests

1. **An event's final hour:** `python listings.py --final-hour 4 --final-at 60 --final-for 30`.
   Pass: change-to-pool p99 under 50 ms on every picker, and re-picks near their pre-final-hour rate through it.
2. **App events at full volume next door:** `python app_events_flood.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **A picker restarts mid-evening:** stop one `zone_picker.py` and start it again with the same `--shard`; it
   rebuilds its pool from the log before it takes an order. Pass: every listing in its pool within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python confluent_bridge.py --bootstrap <brokers> --topic <listing changes> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; seller integrations,
automatic pricing, checkout and every other consumer are untouched. Map your record shape in `to_change()`: the
kit needs `k` (listing), `z` (event:zone), `seq`, `qty` and `price`. The public sources name Confluent Kafka for
the data platform only; if listing changes don't reach a Kafka topic today, the evaluation starts with a tap where
seller API updates and price changes are written.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; pickers use the Apache
Iggy SDK. Delivery is at-least-once; the per-listing sequence makes replays and retries safe. Replicated clusters
shipped in September 2026, which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored listing feed, beside Confluent Kafka and
your Zone Deals service. No advantage, no migration.

kranti@laserdata.com · laserdata.com
