# Live price floors for the Roku Exchange, on LaserData

Every auction outcome reaches the floor service, and every new floor reaches every exchange node, in
milliseconds, even in a live game's ad break. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.roku-kit
- **One slide:** [`LaserData_Roku_LiveFloors.pdf`](LaserData_Roku_LiveFloors.pdf)

## Why this use case

From Roku's own job posts (October 2026): the exchange "operates efficiently across a real-time bidding
environment powered by more than 100 million household devices", and its product team designs "algorithms for
dynamic price floor optimization using real-time signals such as demand density, device and content context,
seasonality, and competing bid landscape", measured on "fill rate, CPM, latency, revenue" and watched through
"QPS, timeout rates, win rate, revenue per mille, and bid density" (Senior Product Manager, Advertising –
Exchange Optimization). In ad serving, "Every millisecond and every decision carries direct, measurable revenue
impact" (Staff Software Engineer, Demand Bidder). The data stack lists Kafka and Flink (Senior Data Engineer,
Data Governance Lead). In Q2 2026, third-party DSPs bought "nearly three-quarters of in-stream video ad spend"
(shareholder letter, Aug 2026), so most video impressions are priced in the exchange's auctions.

A floor built on demand density is only as current as the demand signal it was computed from. The loop has two
hops: each auction's outcome has to reach the floor service, and the new floor has to reach every exchange node
before its next auction. This kit runs that one loop on Apache Iggy, prints both hops' latency every second, and
prints revenue per thousand auctions (RPM) and fill for live floors next to the same floors refreshed on a
slower cadence, judged on the same bids.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 exchange nodes, 400 inventory segments (content category × device
type × break slot), 10,000 auctions a second at $8–$30 CPM, a live game's ad break every 90 s for 30 s that
doubles the sports bidders, and a 5-minute floor refresh to compare against. All illustrative; set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python floors.py &                   # the floor service, fed by the outcome lane
python exchange.py --join-at 60      # 8 exchange nodes: outcomes out, floors in; a 9th joins at 60 s
```

They print a line a second:

```
[floors]    10,000 outcomes/s  outcome-to-floor-service p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  floors moved …  segments priced 400  sports avg floor $…
[exchange]  10,000 auctions/s (sports break)  nodes 8  floors in x-00's table 400  floor-to-exchange-nodes n … p50 …ms p99 …ms
[yield] live floors: RPM $… fill …%  | floors refreshed every 300 s: RPM $… fill …%  | live vs refreshed +…% (sports +…%)  run total +…%
[join] late node rebuilt 400 segment floors from … floor updates in the log in … ms, now taking auctions
```

**What the numbers mean.** The kit times each outcome from the moment `exchange.py` creates it to the moment
`floors.py` has applied it, and each floor from `floors.py` to every exchange node, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at
reduced scale (3,000 auctions/s, 400 segments, 4 exchange nodes, a live break every 40 s for 12 s, a 30 s
refresh to compare against), over three green runs it held outcome-to-floor-service p50 3.7–5.0 ms and
p99 6.9–11.3 ms (every steady-state second inside 50 ms), and floor-to-exchange-nodes p50 3.5–5.0 ms and p99
6.0–8.7 ms. With 2 MB/s of impression log on the same broker, the lane's p99 moved by 4% or less. A fifth node
started 45 s in rebuilt all 400 floors from the log in 38–52 ms. That is the end-to-end loop through this demo
code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on
dedicated machines.

The yield numbers belong to the demo's demand model and its simple floor rule, not to Roku: live floors earned
1.0–1.3% more per thousand auctions than the same floors refreshed every 30 s, and filled 84.8–84.9% of auctions
against 82.6–83.7%. Live floors rise with a break's bidders and come back down after it; a refreshed floor set
during a break stays high and leaves auctions unfilled. Your bids, your floor model and your refresh cadence set
the real number.

| File | What it is |
|---|---|
| `exchange.py` | A fleet of exchange nodes. Each auction clears second-price against the node's floor for its segment and publishes the outcome (bids back, top and second bid, with a per-(node, segment) sequence) to `auction-outcomes`, keyed by segment. Each node reads `floor-updates` on its own connection into its own floor table, and every auction is also cleared against the last periodic refresh, from the same bids, for the yield comparison. `--join-at` adds a node that rebuilds its table from the log. |
| `floors.py` | The floor service. Keeps each segment's top bids for the last 5 s, sets the floor at their 5th percentile every 250 ms (a stand-in for your floor model), and publishes floors that moved. Prints the lane's p99. |
| `impression_log.py` | A heavy impression and bid log on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your auction log from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A live game's ad break:** `python exchange.py --surge 3 --surge-every 120 --surge-for 45`.
   Pass: outcome-to-floor-service p99 under 50 ms and floor-to-exchange-nodes p99 under 50 ms through the break,
   and the sports floors follow the break up and back down (the `sports avg floor` column).
2. **A heavy neighbour:** `python impression_log.py --mbps 50` while both run.
   Pass: the outcome lane's p99 moves by less than 10%.
3. **Add a node mid-game, restart the floor service:** `--join-at 60` adds a node that reads the floor log
   from the start; stop `floors.py` and start it again and it replays the outcome log, where outcomes older
   than its 5 s window fall out, and prints "catching up" until it is live.
   Pass: the new node takes auctions with a full floor table within a second, and floors resume within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker> --kafka-topic <auction-log-topic> \
  --node-field <host-field> --segment-fields <category-field>,<device-field>,<slot-field> \
  --count-field <bids-back-field> --top-field <top-bid-cpm-field> --second-field <second-bid-cpm-field> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; exchange nodes, producers and existing consumers are untouched. Map your record in
`to_outcome()`, keyed by whatever fields your floors use. Then compare the floors `floors.py` computes, and how
fast they move, with yours. The exchange services are on the JVM; Apache Iggy has a Java SDK for when they write
to the lane directly.

## What Apache Iggy 0.9 does not do yet

The Kafka API gateway is in progress, so Kafka clients and connectors cannot write to Iggy unchanged yet; the
bridge above is how a trial reads your traffic today. Delivery is at-least-once, which is why every record carries
a sequence and readers drop repeats. Clustering with replication is new (September 2026); run the trial on a
single node or on LaserData Cloud.

LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure,
run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored auction-log topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
