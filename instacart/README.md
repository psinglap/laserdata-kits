# Real-time session sequence for Instacart ads retrieval, on LaserData

Every product view and cart add reaches the shopper's session sequence in milliseconds, so the next ads
carousel sees the cart they have now. It runs beside Confluent and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Instacart_SessionSequence.pdf`](LaserData_Instacart_SessionSequence.pdf)

## Why this use case

From Instacart's own sources:

- Contextual Recommendations powers retrieval "for both ads and organic recommendations across all major
  browse surfaces". "The model leverages the user's real-time session, which includes product views, item
  page visits, and cart additions, as a sequence of these product tokens." The 2026 rebuild builds a prompt
  of retailer type, user history and cart, where "Cart SIDs capture the real-time intent of the current
  session ... adapting as new items are added", and "The user's intent shifts mid-cart". On the retailer
  home page and pre-checkout it lifted click-through 5% and add-to-carts 34% (Instacart Tech, "From Scoring
  to Spelling: Rebuilding Ads Retrieval at Instacart", Jun 2026).
- The Ads Quality team is hiring to "Build real-time feature processing infrastructure—low-latency pipelines
  that compute, join, and serve fresh features to models on the critical path of every ad request", with
  "online sequence feature stores, streaming update paths" for "a real-time sequential foundation model for
  ads" (Senior Software Engineer II, Ads Quality, Oct 2026).
- Real-time ML at Instacart runs Kafka as the central event store, Flink to compute features, and a key-value
  feature store, at "terabytes of event data per day"; real-time features include "live shopping sessions";
  feature generation is "in the order of seconds", and Kafka adds latency "usually within a few 100ms"
  (Instacart Tech, "Lessons Learned: The Journey to Real-Time Machine Learning at Instacart", Sep 2022).
- The Flink platform handles "over two trillion events a year" (Instacart Tech, Apr 2023), on Kafka and
  Confluent on AWS, where "architectural decisions have real technical and financial consequences" (Staff
  Software Engineer, Data Governance & Foundations, Aug 2026). Ads revenue was $297 million in Q2 2026, up 16%.

Confluent Cloud, Flink and the feature store are the right home for two trillion events a year, and this
kit does not touch them. The session behind an ads carousel is a narrower job with a tighter clock: a
shopper with milk, eggs and cereal in the cart adds ground beef and opens pre-checkout less than a second
later. This kit runs session actions on their own lane, keyed by user, read by the sequence service
directly, and counts the outcome: carousels built without the shopper's last action.

Instacart does not publish how the session reaches ads retrieval today, its freshness, the gap from a cart
add to the next carousel, or its event schema. The lane is a proposal; the kit's flags let you set those to
yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 400,000 shoppers in session, 10,000 actions a second (45% product
views, 25% item page visits, 30% cart adds), a 20% chance a shopper moves to another aisle, half of the
actions followed by an ads carousel request 0.2 to 1.5 s later, and a 2× busy-evening surge for the first
minute. These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python retrieval.py &                  # the session sequence service, checking every carousel request
python app.py &                        # shoppers browsing and filling carts
python firehose.py --at 20             # a heavy event firehose next door
```

Each prints a line a second:

```
[app]  20,000 shopper actions/s (surge)  views …  item pages …  cart adds …  aisle changes …  carousel requests …
[sequence]  20,000 actions/s  action-to-sequence p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  shoppers in session …
[carousels] requests …  right after a cart add …  built without the last action …%  without the last cart add …  mismatched …
```

`built without the last action` is the business number. Each action carries its kind, the product's token
(in the four-codeword Semantic ID shape; the codes here are made up), the retailer type, the shopper's
session and a sequence number that only grows per user. The sequence service appends every action to the
session tokens and every cart add to the cart tokens, keeping the last 20 of each (`common.fold()`). Each
carousel request carries the number, kind and product of the shopper's last action. `retrieval.py` asks
whether it had applied that action when the request started. If not, the carousel was built from the session
as it was a moment ago; if the missing action was a cart add, the prompt's cart segment was missing the
product the shopper had just added (`without the last cart add`).

What-if: `python retrieval.py --hold-ms 1000` applies every action only once it is 1 s old, as a feature path
measured in seconds would, so you can see how many carousels such a path builds from a stale sequence.

**What the numbers mean.** The kit times each action from the moment `app.py` sends it to the moment `retrieval.py` has applied it to the session sequence, through Python clients, on whatever machine runs it. On a shared GitHub runner (2 vCPU), with the broker and four Python processes on one box, at 1,500 shopper actions a second over 200,000 shoppers with a 2x surge for 15 seconds, three green runs held p50 4.9 to 7.7 ms, p99 8.6 to 14.1 ms and p99.9 9.6 to 15.7 ms (median per second over 61 to 62 steady-state seconds each), with no second over the 50 ms budget. Through the surge (3,000 actions a second) the median per-second p99 was 9.8 to 17.9 ms, worst 10.2 to 19.5 ms, with a 1 MB/s event firehose next door. The what-if with every action applied 1 s late built 61.3 to 61.8% of carousels (median per second) from a stale sequence, about 4,500 to 4,600 per run missing the cart add just made. Those are the demo's Python clients on a shared box at reduced scale, not the broker. The broker's own tail latency is measured separately with `iggy-bench` on dedicated machines (`bench/PLAN.md`). Of about 57,000 to 57,700 carousel requests per run, none was built without the shopper's last action.

| File | What it is |
|---|---|
| `app.py` | The Instacart storefront: session actions on `session-actions` (kind, product token, retailer type, session, sequence that only grows per user), and the shopper's next ads carousel request on `carousel-requests`. `--gap-min/--gap-max`, `--switch-share` and `--surge` shape the load. |
| `retrieval.py` | The session sequence service: applies actions by sequence, times action-to-sequence, and checks every carousel request against the shopper's last action. `--hold-ms` is the what-if. |
| `common.py` | Topics, partitioning by user, and the sequence (cart tokens and session tokens, last 20 each). |
| `firehose.py` | A bulk, latency-tolerant event stream (impressions, page loads) on its own topic on the same broker. |
| `confluent_bridge.py` | Reads your session events from Confluent Cloud as consumer group `iggy-shadow`, read-only, and publishes them to the lane, keyed by user, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The night before a holiday:** `python app.py --actions-per-s 10000 --surge 2 --surge-for 60`.
   Pass: action-to-sequence p99 under 50 ms through the surge, and no carousel built without the last action.
2. **The firehose on the same broker:** `python firehose.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the sequence service:** stop `retrieval.py` and start it again. It rebuilds every shopper's
   sequence from the lane, prints "catching up", then times live actions again; carousel requests sent while
   it was away are not counted as live.
   Pass: no action applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python confluent_bridge.py --bootstrap <bootstrap> --kafka-topic <your-session-events> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api key> -X sasl.password=<api secret>
```

The bridge only reads, as its own consumer group (`iggy-shadow`); the apps, event ingestion, every producer,
every Flink job and every existing consumer are untouched. Map your fields in `to_action()`: the defaults look
for `user_id`, `event`, `product_id` or `sid`, `retailer_type` and `session_id`. In shadow mode the clock
starts when the bridge reads the record. Everything runs in your AWS account.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge and the sequence
service reads the lane with an Iggy SDK (Go, Java, Python, Rust, C#, Node.js). Delivery is at-least-once; the
per-shopper sequence number drops replays. Replicated clusters shipped in September 2026 and are still new.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored session-events stream, beside
Confluent Cloud. No advantage, no migration.

kranti@laserdata.com · laserdata.com
