# Real-time viewed posts for ShareChat and Moj feed dedup, on LaserData

Every post a user has just seen reaches the dedup service in milliseconds, so the next feed page
never repeats it. It runs beside Redpanda and WarpStream, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/MUtzM9gcQu6BifQAE6tQ2e
- **One slide:** [`LaserData_ShareChat_ViewedPosts.pdf`](LaserData_ShareChat_ViewedPosts.pdf)

## Why this use case

From talks by ShareChat engineers:

- The feed's dedup service removes posts a user has already seen before ranking. Viewed posts
  are tracked on the device, sent periodically, queued and persisted: "This creates a delay
  between the time the event occurs and when it's stored. That delay makes it possible to send
  duplicate posts in sequential feed requests." Viewed events moved from Pub/Sub and Dataflow to
  Redpanda and Flink, into ScyllaDB (Andrei Manakov, Monster Scale Summit 2025, as published by
  ScyllaDB in "How ShareChat Cut Recommendation Engine Costs 90%", Mar 2026).
- ML and ads logging moved from Kafka to WarpStream, 58-60% cheaper than multi-AZ Kafka, at about
  400 ms produce and 1 s producer to consumer. "For our use case, which is mostly for ML logging,
  we do not care as much [about latency]" (Shubham Dhal; WarpStream blog, Jun 2025). The same
  talk: "Do you understand your latency [needs]?" (Vivek Chandela).
- The Moj feed serves about 8,000 requests a second for 20 million daily users (P99 CONF 2023);
  Moj has 6 billion video plays a day (Monster Scale Summit 2025).

WarpStream was the right call for logging, and this kit does not touch it. Viewed posts are
different: the next feed request can come a second or two after the app flushes what the user
saw, while they keep scrolling. A path that takes a second, or that waits for a window to close,
can lose that race. This kit runs the viewed posts on their own lane, keyed by user, read by the
dedup service directly, and counts the outcome: repeats served.

Part of the delay ShareChat describes is how often the app sends views, which no broker changes.
The lane removes the rest: the queue and the window between the flush and the dedup check.

## Run it

Needs Docker and Python 3.10+. Defaults: 20 million users, 70,000 views a second in flushes of
five, 55% of flushes followed by a feed request 0.2 to 2 s later (about 7,700 feed requests a
second), and a 1.5× peak for the first minute. ShareChat does not publish its flush interval or
the gap to the next request, so set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python dedup.py &                      # the dedup service, filtering every next-page request
python app.py &                        # users scrolling, flushing viewed posts, asking for the next page
python engagement_flood.py --at 20     # a heavy engagement stream next door
```

Each prints a line a second:

```
[app]  105,000 views/s (peak)  in 21,000 flushes  next-page feed requests …
[dedup]  21,000 flushes/s  flush-to-dedup p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  users in set …
[feed] next-page requests …  viewed candidates removed …  missing the last flush …%  repeats served …
```

`repeats served` is the business number. Each feed request carries the sequence of the user's
last flush and 20 candidates from retrieval, two of them posts the user has just viewed.
`dedup.py` drops every candidate it knows the user viewed, and asks whether it had applied the
user's last flush by the time the request started. If not, the just-viewed posts that got through
are repeats served. The set holds the last 10 minutes (`--keep-s`), the time before the windowed
job has the views in the database; older views stay the database's job.

**What the numbers mean.** The kit times each flush from the moment `app.py` sends it to the moment
`dedup.py` has applied it, through Python clients, on whatever machine you run it on. On a shared
GitHub runner (2 vCPU), with the broker and three Python processes on one box, two green CI runs at
2,500 views a second (500 flushes, 2× for 15 s as the peak) held p50 12.2 to 12.3 ms, p99 23.9 to
24.0 ms and p99.9 25.5 to 25.6 ms (median per second over 63 steady-state seconds each, no second
over the 50 ms budget). Through the peak (1,000 flushes a second) the median per-second p99 was
24.5 to 24.7 ms, worst 26.2 ms. Of about 21,000 next-page feed requests in each run, none missed
the user's last flush and no repeats were served; dedup removed about 42,000 just-viewed candidates
per run. A 1 MB/s engagement stream ran next door for 33 s. That is the end-to-end lane through
this demo code at reduced scale, not the broker alone; the broker's own tail latency is measured
with `iggy-bench` on dedicated machines (`bench/PLAN.md`). On a laptop or a larger machine, raise
the rates.

| File | What it is |
|---|---|
| `app.py` | The apps: flushes of viewed post IDs per user on `viewed-posts` (sequence only grows per user), and the user's next-page request on `feed-requests` with candidates from retrieval. `--gap-min/--gap-max`, `--posts-per-flush` and `--peak` shape the load. |
| `dedup.py` | The dedup service: applies flushes by sequence into each user's recently viewed set, times flush-to-dedup, and filters every next-page request against it. |
| `engagement_flood.py` | A bulk, latency-tolerant engagement stream (likes, views for counters) on its own topic on the same broker. |
| `redpanda_bridge.py` | Reads your viewed-post events from Redpanda as a read-only Kafka consumer group and publishes them to the lane, keyed by user, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Festival peak:** `python app.py --views-per-s 70000 --peak 1.5 --peak-for 60`.
   Pass: flush-to-dedup p99 under 50 ms through the peak, and no repeats served.
2. **Engagement on the same broker:** `python engagement_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart dedup:** stop `dedup.py` and start it again. It replays the lane, prints "catching up",
   then times live flushes again. Pass: no flush applied twice, back under budget within seconds.

To see where the boundary is, shorten the gap: `python app.py --gap-min 0 --gap-max 0.05`. Requests
that start before their user's flush has landed show up as `missing the last flush`.

## Shadow your own traffic

```
pip install confluent-kafka
python redpanda_bridge.py --bootstrap <redpanda-broker>:9092 --kafka-topic <your-viewed-posts-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-256 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads; the apps, the Flink job that persists views, and every existing consumer
are untouched. Map your field names in `to_view()`: the kit needs the user ID and the viewed post
ID or IDs. In shadow mode the clock starts when the bridge reads the record. Everything runs in
your GCP project.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored viewed-posts topic, beside
Redpanda. No advantage, no migration.

kranti@laserdata.com · laserdata.com
