# Real-time last-searched hotels for MakeMyTrip, on LaserData

Every hotel search and hotel view, from the B2C app and from corporate travel, reaches the traveller's
last-searched list in milliseconds, before their next tap on the search bar. It runs beside your Kafka and
Spark, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_MakeMyTrip_LastSearched.pdf`](LaserData_MakeMyTrip_LastSearched.pdf)

## Why this use case

From MakeMyTrip's own sources:

- "Last-searched" hotels is the personalised list of a traveller's recent interests that appears as they tap
  the search bar. MakeMyTrip moved it to Spark Real-Time Mode on Databricks: p50 latency went from about
  1.23 s to 44 ms, p99 from over a minute to about 500 ms, and click-through rate rose 7% (Databricks blog,
  "How MakeMyTrip achieved millisecond personalization at scale", Apr 2026, co-written by Aditya Kumar,
  Associate Director of Engineering).
- The path, as published: B2C and B2B clickstream Kafka topics merged into one stream (about a month of
  joint work), 64 Kafka partitions, each user's "Last N" hotels looked up in Aerospike, results pushed to a
  Redis UI cache so the app answers in under 50 ms. Spark micro-batch "consistently delivered one to two
  seconds of latency" despite tuning.
- The team evaluated Apache Flink on about 10 pipelines; it met the latency, but they chose one engine:
  "We wanted one source of truth -- one Spark-based pipeline -- rather than two engines to maintain."
- Earlier, MakeMyTrip's data platform ran a real-time serving store on Spark Streaming, Kafka, RocksDB and
  Akka HTTP for ranking, dynamic pricing, persuasions and cross-sell (Piyush Kumar, The Fifth Elephant 2019).

Your Kafka, Spark RTM, Aerospike and Redis stay exactly as they are, and Spark keeps every pipeline that
needs its logic. This kit looks at one narrow piece: the event lane in front of the last-searched list, whose
p99 (about 500 ms published) sets how often a traveller who searched a hotel a moment ago taps the search
bar and does not see it. The kit runs hotel searches on their own lane, keyed by user, B2C and B2B on one
topic with no union job, read by the last-searched service directly, and counts the outcome: taps served
a list without the traveller's last search.

MakeMyTrip does not publish where the ~500 ms p99 goes (Kafka, Spark, Aerospike, Redis or the app), its
event rate, or the gap from a search to the next tap. The lane is a proposal; the kit's flags let you set
those to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 travellers, 10,000 hotel searches and views a second
(15% from corporate travel), a 20% chance a traveller moves to another city, half of the events followed by
a tap on the search bar 0.3 to 3 s later, and a 2× sale-opening surge for the first minute. These are
illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python recents.py &                    # the last-searched service, serving every search-bar tap
python app.py &                        # travellers on the B2C app and corporate travel
python clickstream.py --at 20          # a heavy clickstream next door
```

Each prints a line a second:

```
[app]  20,000 searches/s (surge)  B2C …  B2B …  hotel searches …  hotel views …  new destinations …  search-bar taps …
[recents]  20,000 searches/s  search-to-list p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  B2C …  B2B …  travellers …
[search-bar] taps …  after a new hotel …  served without the last search …%  last-searched hotel missing …
```

`served without the last search` is the business number. Each event carries the hotel, city and check-in
date, the segment, and a sequence that only grows per user. The service folds them into a list of the last
10 hotels, newest first, a re-searched hotel moving to the top, the same way in every script
(`common.fold()`). Each tap carries the sequence of the traveller's last search and the hotel that should be
at the top. `recents.py` asks whether it had applied that search when the tap happened. If not, the list was
stale; if the hotel was not in the list at all, the traveller had to search for it again
(`last-searched hotel missing`).

What-if: `python recents.py --hold-ms 1000` applies every search only once it is 1 s old, the low end of the
one to two seconds MakeMyTrip saw with micro-batch, so you can see how many taps such a path serves stale.

**What the numbers mean.** The kit times each search from the moment `app.py` sends it to the moment
`recents.py` has folded it into the list, through Python clients, on whatever machine you run it on. It
does not include writing the list to Aerospike or Redis, or the app's own request. CI results are added
here after the green runs.

| File | What it is |
|---|---|
| `app.py` | The B2C app and corporate travel: hotel searches and views on `hotel-searches` (each segment on its own producer, sequence only grows per user), and the traveller's next tap on `searchbar-taps`. `--gap-min/--gap-max`, `--switch-share`, `--b2b-share` and `--surge` shape the load. |
| `recents.py` | The last-searched service: folds searches by sequence, times search-to-list, serves every tap and checks it against the traveller's last search. `--hold-ms` is the what-if. |
| `common.py` | Topics (64 partitions on the lane), partitioning by user, and the last-10 fold. |
| `clickstream.py` | A bulk, latency-tolerant clickstream (page loads, impressions) on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your B2C and B2B clickstream topics from Kafka as consumer group `iggy-shadow`, read-only, and publishes hotel searches to the lane, keyed by user, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A sale opens:** `python app.py --searches-per-s 10000 --surge 2 --surge-for 60`.
   Pass: search-to-list p99 under 50 ms through the surge, and no tap served without the last search.
2. **Clickstream on the same broker:** `python clickstream.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the service:** stop `recents.py` and start it again. It rebuilds every traveller's list from the
   lane, prints "catching up", then times live searches again.
   Pass: no search applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> \
  --kafka-topic <your-b2c-clickstream>=b2c --kafka-topic <your-b2b-clickstream>=b2b
```

The bridge only reads, as its own consumer group (`iggy-shadow`); the apps, every producer, the Spark RTM job,
Aerospike, Redis and every existing consumer are untouched. Map your fields in `to_search()`: MakeMyTrip's
event schema is not public, so the defaults are guesses (`user_id`, `hotel_id`, `city`, `checkin`). In shadow
mode the clock starts when the bridge reads the record.

## What Apache Iggy 0.9 does not do yet

A Kafka-compatible API is in progress and not in 0.9, so the trial uses the bridge and Spark keeps reading
Kafka. Delivery is at-least-once (the per-user sequence makes repeats harmless). Replicated clusters
(Viewstamped Replication) shipped in September 2026 and are new.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored hotel-search stream, beside your Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
