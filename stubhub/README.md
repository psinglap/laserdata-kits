# Real-time seat availability and prices for StubHub, on LaserData

Every seller listing change reaches the event pages and checkout in milliseconds, even in a big onsale. It runs
beside Event Hubs today, and beside whichever broker you choose next, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.stubhub-kit
- **One slide:** [`LaserData_StubHub_ListingLane.pdf`](LaserData_StubHub_ListingLane.pdf)

## Why this use case

From StubHub's own 2026 job postings: the streaming and storage platforms process 10–30 billion events a day for
roughly 100 service teams on Azure Event Hubs and Service Bus, and a program is starting to move streaming to an
AWS-native, Kafka-compatible platform, with vendor selection and benchmarking. The Supply Platform keeps seller
ticket inventory moving "blazing fast" and meets its SLAs "no matter the scale of the seller inventory load".
StubHub's December 2025 release on its ChatGPT app promises "live availability and pricing, not cached webpage
summaries".

Every surface that makes that promise is only as current as the stream of listing changes behind it. This kit
runs that one lane on Apache Iggy and prints its p99 every second, with the seat picks checkout rejects because
the tickets were already gone.

## Run it

Needs Docker and Python 3.10+. Defaults: 2,000 events × 250 seller listings, 2,000 listing changes a second, and
300 seat picks a second.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python listings.py &                 # every seller listing, and checkout's accept or reject
python event_pages.py                # live view of every event + buyers picking seats
```

Both print a line a second:

```
[pages]   2,000 changes/s  change-to-page p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  seat picks …
          a page 2s behind would list … already-sold listings (…%)
[listings] seat picks confirmed …  rejected as already sold …  sold-before-checkout …%  (this second)
```

**What the numbers mean.** The kit times each change from the moment `listings.py` creates it to the moment
`event_pages.py` has applied it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU
GitHub runner with the broker and all three scripts on one box (1,500 changes/s, a 2× onsale to about 3,000/s,
2 MB/s of clickstream next door), it held p50 11–12 ms and p99 22–24 ms over two green runs (21.5 ms in the onsale seconds), with no second over
the 100 ms budget. Checkout rejected 0.8–0.9% of seat picks as already sold; a page fed 2 s behind would have
listed about 2–5% of its listings after they had sold out.
That is the end-to-end lane through this demo code, not the broker alone. One Python reader tops out near
5,000 changes/s on that runner; past it, latency climbs because the reader falls behind, not the broker. The
broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `listings.py` | Tickets left and price per `event:listing`, published to `listing-changes` with a per-listing sequence and the resulting state. Confirms or rejects seat picks against the true state. `--onsale` multiplies the rate and puts part of it on one event. |
| `event_pages.py` | A live view of every event from the lane, buyers picking seats from it, and the same view held 2 s behind for comparison. |
| `clickstream_flood.py` | A heavy, latency-tolerant stream (page views, searches) on its own topic, for the isolation test. |
| `shadow_bridge.py` | Copies an existing Event Hub (or, after the move, a Kafka-compatible topic) into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A big onsale:** `python listings.py --onsale 3 --onsale-at 60 --onsale-for 120`.
   Pass: change-to-page p99 under 100 ms, and sold-before-checkout during the onsale within its pre-onsale range.
2. **Clickstream at full volume:** `python clickstream_flood.py --mbps 50` while both run.
   Pass: the listing lane's p99 moves by less than 10%.
3. **Rebuild a page server:** stop `event_pages.py` and start it again; it replays the listing topic.
   Pass: it catches up within seconds.

## Shadow your own traffic

```
pip install azure-eventhub
python shadow_bridge.py --connection-string "<Event Hubs namespace, Listen>" --event-hub <listing-changes-hub>
```

The bridge reads as its own consumer group, `iggy-shadow` (create it on the hub first), and writes no
checkpoints; producers and existing consumers are untouched. After the move to a Kafka-compatible broker:
`python shadow_bridge.py --source kafka --bootstrap <brokers> --topic <listing-topic> -X key=value …`.
Map your record shape in `to_change()`: the kit needs `k` ("event:listing"), `seq`, `qty` and `price`.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress). Your platform program asks for Kafka compatibility, so this is
a proposal for one lane beside that platform, not a replacement for it. Delivery is at-least-once; the
per-listing sequence makes replays and retries safe. Replicated clusters shipped in September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored listing stream, beside Event Hubs and inside
your broker benchmark. No advantage, no migration.

kranti@laserdata.com · laserdata.com
