# Real-time account links for PayPal's risk graph, on LaserData

Every login's device and IP, and every profile asset an account adds, reach the real-time graph in
milliseconds, so the two-hop check on that account's next payment already sees the link. It runs
beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_PayPal_GraphLinks.pdf`](LaserData_PayPal_GraphLinks.pdf)

## Why this use case

From PayPal's own posts:

- "Today, almost all PayPal risk models are using graph features." Two-hop queries run on Gremlin over
  Aerospike and "should be finished in 10 milliseconds on average"; graph compute for risk decisioning
  has a "100 milliseconds SLA" (PayPal Technology Blog, Dec 2021, "How PayPal Uses Real-time Graph
  Database and Graph Analysis to Fight Fraud").
- The write side asks for "Seconds level data freshness". Login, transaction and other events are
  consumed to create "new account vertices, and new edge links", so the graph "can be updated with
  the latest data in a second-level gap" (same post).
- The post's repeat-fraudster case: seven restricted accounts share one profile asset, and a new
  account adds the same asset. Assets are "IP, address, VID, device ID, etc.", and in a takeover
  they "might change frequently within a short period of time" (PayPal Developer Community Blog,
  Sep 2023, "Graph Usage in Combating ATO Fraud Risk").

The query is fast; the question is whether the edge is there yet. A link helps only if it lands
before the account's next payment is checked, and a scripted fraudster can pay within a second of
adding an asset. This kit runs the account-to-asset links on their own lane, keyed by asset, timed
from the login to the graph, and checks every payment that follows a login against the graph.

## Run it

Needs Docker and Python 3.10+. Defaults: 50 million accounts, 5,000 logins a second (about 10,000
links a second), 60% of logins followed by a payment 0.2 to 8 seconds later, 0.5% repeat fraudsters
who pay within 2 seconds, and a 3× peak for the first minute. PayPal does not publish login rates or
login-to-payment gaps, so set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python graph.py &                    # the graph's write path, and the two-hop check
python activity.py &                 # logins, profile changes, repeat fraudsters, payments
python tracking_flood.py --at 20     # a heavy tracking stream next door
```

Each prints a line a second:

```
[activity]  15,000 logins/s (peak)  links …  repeat fraudsters …  payment checks …
[graph]  30,700 links/s  link-to-graph p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
[check] payment checks … (repeat fraudsters …)  without the newest links …%
        repeat fraudsters linked to restricted accounts … of …  at a 1 s write path: without the newest links …%
```

**The business number.** For every payment that follows a login, `activity.py` sends a check with
the sequence of each link from that login. `graph.py` asks whether its graph held all of them when the
payment started, and only then runs the two-hop query (account → asset → restricted accounts).
On a real Apache Iggy 0.9.0 broker in CI, three runs checked about 18,300 to 18,500 payments each, 293 to 338 of them
by repeat fraudsters paying 0.2 to 2 seconds after re-adding a known-bad asset. None ran without its
links, and every repeat fraudster was linked to the restricted accounts, including through the peak.

The same line prints a comparison, judged on the same checks: a write path with one second of
freshness (the kind end of "seconds level") would have checked 10.3 to 11.0% of payments, and 41 to 47% of
repeat fraudsters, before their links landed. That share comes from the kit's assumed login-to-payment
gaps, not from PayPal data. Run the bridge on your events and set `--slow-path-s` to the freshness you
measure to get yours.

**What the latency means.** The kit times each link from the login in `activity.py` to the moment
`graph.py` has applied it, through Python clients, on whatever machine you run it on. On a shared
2-vCPU CI runner, with the broker and three Python processes on one box and the graph reading the 16
partitions directly, at 400 logins a second (about 830 links a second) with a 2× peak for 15 seconds,
three green runs held p50 4.0 to 6.0 ms, p99 7.3 to 10.6 ms and p99.9 8.4 to 12.8 ms (median per second over 61 to 62 steady-state seconds each), with no second
over the 50 ms budget. Through the peak (about 1,650 links a second) the median per-second p99 was
8.3 to 13.2 ms, worst 16.3 ms, with a 1 MB/s tracking stream next door. That is the end-to-end lane through this demo code,
not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`,
on dedicated machines.

| File | What it is |
|---|---|
| `activity.py` | Logins and profile changes as account-to-asset links on `asset-links`, keyed by asset, with a sequence and the edge's state. A start-up snapshot of restricted accounts on known-bad assets. Payments that follow logins, as checks on `payment-checks`. |
| `graph.py` | The graph's write path (account → assets, asset → accounts), applied by sequence, and the two-hop check on every payment. Prints link-to-graph p99 and the business numbers. |
| `tracking_flood.py` | A heavy first-party tracking stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Reads your login and profile-change topics as its own consumer group and publishes their links to the lane, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The peak:** `python activity.py --logins-per-s 5000 --peak 3 --peak-for 60`.
   Pass: link-to-graph p99 under 50 ms through the peak, under 0.01% of payment checks without the
   links from their login, and every repeat fraudster linked to the restricted accounts.
2. **Tracking on the same brokers:** `python tracking_flood.py --mbps 20 --at 20` while both run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart the graph's writer:** stop `graph.py` and start it again; it replays the lane, snapshot
   included. Pass: no edge applied twice or out of order, and back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker>:9092 --kafka-topic <login-events> --kafka-topic <profile-changes> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, as its own consumer group (`iggy-shadow`); producers, the graph data process
service and every existing consumer are untouched. Map your record shape in `to_links()`: the kit needs
the account and its assets (device ID, IP, VID, address). Hash or tokenize assets as your data rules
require; the lane needs only a stable ID per asset.

## What Apache Iggy 0.9 doesn't have yet

At-least-once delivery, not exactly-once (the per-edge sequence makes that safe here). Replication
clusters are new (September 2026). A Kafka-compatible gateway is in progress, so this lane uses an Iggy
SDK and the trial uses the bridge; the Kafka fleet stays as it is.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored login-events topic, self-hosted
beside your Kafka, in shadow. No advantage, no migration.

kranti@laserdata.com · laserdata.com
