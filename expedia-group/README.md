# Real-time AdaptEx decisions for Expedia Group, on LaserData

Every arm a Sampler shows a traveler (a property's hero image, a layout, a module order) reaches every Sampler
pod's consistency cache in milliseconds, so the traveler sees the same arm on the next page, whichever pod serves
it. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_ExpediaGroup_BanditDecisions.pdf`](LaserData_ExpediaGroup_BanditDecisions.pdf)

## Why this use case

From Expedia Group's own paper, talk and posts:

- AdaptEx is "a self-service contextual bandit platform widely used at Expedia Group". Its Sampler "is an
  auto-scaling Spring Boot app, designed to scale horizontally according to traffic", "periodically fetches the
  most up-to-date parameters from the Bandit Store" and "includes a consistency cache to ensure the user experience
  remains consistent across a session" (RecSys 2023, "AdaptEx: a self-service contextual bandit platform").
- For hero images on Hotels.com: up to 10 candidate images per property, Thompson Sampling on click-through rate,
  "an independent bandit for each property", and "once an image has been selected for a user, it will remain the
  same for this property for the rest of the session and across the website" (Expedia Group Technology blog, 2020).
- The Inference Engine publishes a "bandit proposed action" event for every decision, read by the feedback
  gatherer and offline policy evaluation; "Kafka is the backbone of our system" (Kafka Summit Americas 2021).

A Sampler that scales out and a rule that holds "across the website" meet at the traveler's next page. If the
load balancer sends it to another pod, that pod has to know what the first one picked, or it draws again and the
traveler can see a different image of the same hotel. This kit runs that one feed as a lane on Apache Iggy: every
new decision, keyed by session, read by every Sampler pod directly, kept for two hours so a new or restarted pod
rebuilds its cache from the log. It prints the lane's p99 every second, images that flipped on a revisit, and what
a cache synced every second would have shown on the same pages.

We could not find how the consistency cache is shared today (per pod, a shared store, or sticky routing). If it
is sticky or a shared store with a good tail, this lane's case is cost and one fewer store, not flips.

## Run it

Needs Docker and Python 3.10+. Defaults: 50,000 properties with a hero-image bandit in 500 destinations, 600 page
views a second from sessions of about 6 pages, 10 properties on a results page, and a median pause of 2 s
between pages.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python sampler.py --name sampler-1 --shard 0 --shards 3 &   # three Sampler pods, a third of the pages each;
python sampler.py --name sampler-2 --shard 1 --shards 3 &   # each one holds every session's decisions
python sampler.py --name sampler-3 --shard 2 --shards 3 &
python travelers.py                  # sessions, the load balancer (no stickiness) and the check
```

They print a line a second:

```
[sampler-1]  1,900 decisions/s  decision-to-cache p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  pages …  new picks …  kept …
[travelers] pages requested 600 answered 600 (steady)  images shown …  revisits …  flipped 0 (0.00%)
            a cache synced every 1s would flip … (…%)  page-to-picks p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each decision from the moment a `sampler.py` picks it to the moment
another `sampler.py` has applied it to its cache, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU GitHub runner with the broker and all four scripts on one box (6,000 properties in 60 destinations,
about 150 page views a second, a 3× spike in new sessions for 15 s, 2 MB/s of clickstream next door, two Sampler
pods, one restarted), each pod applied about 410 to 520 decisions a second from the other, and the medians of the
steady-state seconds over three green runs on the final code were p50 6.4–9.4 ms, p99 12.0–17.6 ms and p99.9
12.1–18.1 ms on each pod. Every steady-state second passed the 50 ms budget (61–62 and 50–51 per pod in all three
runs); the worst second reached 12.9–23.7 ms. The restarted pod rebuilt its cache from the log (about 540 sessions,
about 9,200 decisions) in 0.05–0.07 s before it reported healthy. Of about 24,500–25,300 revisits per run, none
showed a flipped image. The same pods' picks from a cache synced every second, judged against what the traveler
saw before, would have flipped 215–286 (0.85–1.17%), and 1.11–1.38% during the spike. Page-to-picks was p50
7.9–10.5 ms and p99 14.0–17.8 ms; 15–26 of about 12,000 pages (0.13–0.22%) went unanswered, the ones in flight
to the pod as it stopped. That is the end-to-end lane through this demo code at reduced scale, not the broker
alone, and the comparison is the kit's model, not an Expedia Group figure. One Python reader tops out near 4,000
messages/s on that runner; past it, the reader falls behind, not the broker. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `travelers.py` | The travelers and the load balancer. Sessions search, re-sort, page through results and open properties; each page goes to a random healthy Sampler; every pick is checked against the image the traveler saw before on that property. `--surge` runs a traffic spike. |
| `sampler.py` | A Sampler pod: reads every partition of the lane directly into its consistency cache, rebuilds from the log on start before it reports healthy, takes its share of page requests (`--shard` of `--shards`), draws with Thompson Sampling only for a bandit the session has no decision for, publishes that decision, and answers each page from a view synced every second too, for comparison. |
| `clickstream_flood.py` | A heavy, latency-tolerant stream (clickstream events) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your Kafka topic of "bandit proposed action" events into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, the partition function, the per-property bandit and Thompson Sampling. |
| `ci.sh` | The CI run: 6,000 properties, 150 page views a second, a 3× spike, a clickstream neighbour, two Sampler pods and a restart. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The travelers are a model, not Expedia Group data: bandit parameters (a learning phase with 40 to 900 impressions
per image), think times, page mix and the one-second comparison sync are the kit's assumptions. Set
`--refresh-s` on `sampler.py` to whatever your consistency cache does today.

## Three tests

1. **A traffic spike:** `python travelers.py --surge 3 --surge-at 60 --surge-for 30`.
   Pass: decision-to-cache p99 under 50 ms on every pod, and flipped images on revisits at zero through the spike.
2. **Clickstream at full volume next door:** `python clickstream_flood.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **A pod that scales out or restarts:** stop one `sampler.py` and start it again with the same `--shard`; it
   rebuilds its cache from the log before it reports healthy. Pass: every live session in its cache within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <bandit proposed action topic> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; the Samplers, the feedback
gatherer, offline evaluation and every other consumer are untouched. Map your record shape in `to_decision()`:
the kit needs `k` (session or visitor), `b` (bandit, or bandit and slot), `arm`, `by` (pod) and `ns`.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; Sampler pods use the
Apache Iggy SDK (Java among others). Delivery is at-least-once; first-decision-wins makes replays and retries
safe. No compacted topics; the lane uses two-hour expiry. Replicated clusters shipped in September 2026, which is
why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored decision stream, beside AdaptEx. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
