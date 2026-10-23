# Real-time VB3 lot state and live bids for Copart, on LaserData

Every accepted bid, countdown restart and sale reaches every bidder's screen in milliseconds, in every yard selling
at once, so members bid on the current price. It runs beside VB3, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.copart-kit
- **One slide:** [`LaserData_Copart_LiveLotLane.pdf`](LaserData_Copart_LiveLotLane.pdf)

## Why this use case

From Copart's own filings:

- "We sell virtually all our vehicles on our virtual marketplace platform VB3." After preliminary bidding, the
  second step is "an internet-only virtual auction": "bidders enter bids via the internet in real time while BID4U
  submits bids" for the preliminary high bidder. "When bidding stops, a countdown is initiated. If no bids are
  received during the countdown or any extensions, the vehicle sells to the highest bidder." (Form 10-K, fiscal 2025)
- About 1 million members in over 185 countries; over 250 locations in 11 countries; more than 4 million units sold
  in the last year. (Fourth-quarter fiscal 2026 results, September 2026) 69.8% of US units went to members registered
  outside the vehicle's state. (Form 10-K, fiscal 2025)
- Infrastructure: "multiple co-located data centers around the world and multiple cloud platforms", a hybrid
  infrastructure "designed to run continuously". (Form 10-K, fiscal 2025)
- Streaming: Copart's Data & AI team, building an "AI ready data platform", asks for hands-on real-time pipelines
  with "Pub/Sub, Kinesis, Kafka". (Sr. Data Engineer job post, 2026)

In a live VB3 auction, each sale lane has a small state that changes with every bid: the lot on the block, the high
bid, the high bidder's flag, and the countdown that every accepted bid restarts. Members follow many lanes at once
from everywhere in the world, and decide their next bid on what their screen shows. A screen that is behind sends a
bid the core has to reject (the high bid already moved) or shows a lot as open after it sold. This kit runs that one
feed as a lane on Apache Iggy: every lane change as the resulting state, partitioned by lane, read directly by every
bidder-facing edge node, kept for six hours so a new or restarted edge rebuilds every lane from the log; and members'
live bids back to the auction core on a second topic. It prints the lane's p99 every second, bid-to-flag time (a
member's bid until their flag shows as high bidder), and live bids placed on a stale screen next to what a screen
refreshed every second would have lost with the same bids, judged against the same lane state.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 yards × 2 virtual sale lanes (400 lots on the block at once), a lot's
high preliminary bid as its opening price, increments of $25, $50 and $100, up to 4 live bids a second on a lot while
its price is low, and a 3 s countdown after every accepted bid.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python auction_block.py &            # the auction core: lanes, lots, countdowns, judges every live bid
python bid_edge.py --name edge-1 --edges 2 &   # an edge node and the members on it
python bid_edge.py --name edge-2 --edges 2     # a second edge; each holds every lane
```

They print a line a second:

```
[edge-1]  1,300 state changes/s  state-to-edge p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  bids sent …
        bid-to-flag p50 …ms p99 …ms  lots open …
[auction]  1,300 lane changes/s  live bids … accepted …  stale-screen bids … (outbid … after the hammer …)
        a screen refreshed every 1s would lose … (outbid … after the hammer … missed lot …)  lots sold … total $…
```

**What the numbers mean.** The kit times each lane change from the moment `auction_block.py` creates it (including
its wait for the next write) to the moment a `bid_edge.py` has applied it, through Python clients, on whatever
machine you run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box (60 yards ×
2 lanes, 120 lots on the block at once and 331–343 lots sold a run, up to 6 live bids a second on each lot from two
edge nodes, 2 MB/s of member activity next door, one edge restarted), the medians of the steady-state seconds over
three green runs on the final code were p50 5.9–8.8 ms, p99 10.7–15.9 ms and p99.9 11.5–16.8 ms on each edge, at
about 340 lane changes a second. Every steady-state second passed the 50 ms budget (65 of 65 and 53 of 53 per edge
in all three runs); the worst second reached 42.8–47.4 ms. Bid-to-core was p50 5.2–6.7 ms and p99 9.5–12.0 ms;
bid-to-flag, a round trip through the core, p50 11.1–15.6 ms and p99 16.8–24.9 ms. The restarted edge rebuilt all
120 lanes from the log (about 9,700 changes) in 0.05–0.09 s before it took a bid. Of 21,408–22,143 live bids judged
after warm-up, 3.7–4.5% were outbid on arrival: another member's bid reached the core first while theirs was in
flight; none landed after the hammer. The same bids placed from a screen refreshed
every second, judged against the same state, would have lost 73.5–73.6%: about 69% outbid on arrival, and 3.7–4.2%
missed lots (the screen still showed the previous lot as sold while the next one was open). That is the end-to-end
lane through this demo code at reduced scale, not the broker alone, and the comparison is the kit's model, not a
Copart figure. One Python reader tops out near 4,000 changes/s on that runner; past it, the reader falls behind, not
the broker. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `auction_block.py` | A stand-in for the VB3 auction core. Runs every sale lane, publishes each lane change to `lot-state` with a per-lane sequence and the resulting state (lot, open or sold, high bid, bidder region, countdown deadline), partitioned by lane; reads `live-bids` from every partition directly and judges each bid against the lane at that moment, the live bid and its 1 s screen twin against the same state. |
| `bid_edge.py` | A bidder-facing edge node: reads every partition of the lane directly into its view of every lane, rebuilds from the log on start, plays its members bidding the next increment over what their screen shows, sends bids from a separate task every 5 ms, and keeps a copy refreshed every second for comparison. |
| `member_activity_flood.py` | A heavy, latency-tolerant stream (lot searches, lot views, watchlist adds, saved searches) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies a Kafka topic that carries your lot state into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `ci.sh` | The CI run: 120 lanes, two edge nodes and a restart, a member-activity neighbour. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The auction is a model, not Copart data: lane counts, lot values, opening bids, increments, how fast members bid,
the 3 s countdown and the 1 s comparison screen are the kit's assumptions. Set `--countdown-s` on
`auction_block.py` and `--bid-rate` on `bid_edge.py` to VB3's own numbers.

## Three tests

1. **Sale start across every yard:** `python auction_block.py --yards 400 --lanes-per-yard 2` with
   `bid_edge.py --bid-rate 8`; every lane opens in the same second.
   Pass: state-to-edge p99 under 50 ms on every edge through the first minute.
2. **Member activity at full volume next door:** `python member_activity_flood.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **An edge node restarts mid-sale:** stop one `bid_edge.py` and start it again; it rebuilds every lane from the log
   before its members bid. Pass: every lane in its view within seconds, and no bid from it lands after the hammer.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <lot state> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; VB3's services and every
other consumer are untouched. Map your record shape in `to_state()`: the kit needs `k` (sale lane), `lot`, `st`,
`high`, `dl` and `seq`. Copart's job posts name Pub/Sub, Kinesis and Kafka for the data platform; which one, if any,
carries VB3's live bid events is not public. If it is Pub/Sub or Kinesis, swap the consumer in the bridge for that
client; the rest stays the same.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; edge nodes use the Apache
Iggy SDK (Java, C#, Go, Rust, Python, Node.js). The kit's bridge reads Kafka; for Pub/Sub or Kinesis, swap its
consumer. Delivery is
at-least-once; the per-lane sequence makes replays and retries safe. Replicated clusters shipped in September 2026,
which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored lot-state feed, beside VB3. No advantage, no
migration.

kranti@laserdata.com · laserdata.com
