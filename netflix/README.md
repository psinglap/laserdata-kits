# Real-time frequency caps for Netflix ads, on LaserData

Every ad impression reaches every ad server's frequency-cap view in milliseconds, before that profile's next
ad decision, even in a live ad break. It runs beside Kafka and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Netflix_FrequencyCaps.pdf`](LaserData_Netflix_FrequencyCaps.pdf)

## Why this use case

From Netflix's own TechBlog and job posts: the Ads Event Publisher "publishes a single, unified stream of ad
events to every downstream consumer: frequency capping, metrics, billing, measurement partners, and
reporting", and when events ran late, "some systems might be fine, but others, such as frequency capping,
cannot. It needs events under a strict SLA to decide whether the next selected ad can run without breaking
policies" (Behind the Scenes: Evolving Netflix's Ads Event Pipeline for Live, Part II, 2026). A live event
"can surge toward tens of millions of concurrent viewers within minutes", and one ad break in one AWS region
meant "hundreds of thousands of events per second" in your load test. Delivery is "at-least-once delivery
with best-effort de-duplication", with the event ID carried so exact-count consumers can de-duplicate
(same post). Frequency Capping "tracks impressions for each campaign, profile" and the Ad Server uses it in
each ad decision (Part I, 2025). The Ad Eventing team runs "billions of ad events per day" with "sub-second
processing SLAs" on Kafka and Flink (Distributed Systems Engineer 6, Ad Eventing, 2026).

A profile's ad decisions do not all land on one ad server. The cap only holds if the impression one server
just decided has reached the cap view of the server that decides that profile's next slot. This kit runs
that one lane on Apache Iggy, prints its latency every second, and prints over-cap impressions and live
sponsor repeats every cap window.

## Run it

Needs Docker and Python 3.10+. Defaults per ad server: 1,000 on-demand ad slots a second over 200,000
profiles, 1% of them binge-watching and making 30% of the requests, 40 campaigns capped at 3 per profile per
60-second window (a compressed day); 6,000 profiles watching a live event with a 3-slot ad break every 30 s,
each pod split across the ad servers, 12 sponsors capped at 1 per profile per window; 0.5% of impressions
delivered twice.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0
python ad_server.py --name ads-1 --index 0 &           # three ad servers, each with its own
python ad_server.py --name ads-2 --index 1 &           # frequency-cap view, each reading
python ad_server.py --name ads-3 --index 2             # every impression
```

Each prints a line a second, and a tally when a cap window closes:

```
[ads-1] 1,000 on-demand + 2,000 live slots/s (live break)  from other servers 6,000/s  impression-to-cap-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  capped …  unfilled …  repeats dropped …  gaps 0
[cap] window … closed: impressions … (live …)  profile-campaign pairs at cap …  over-cap impressions … (…%)  live sponsor repeats … (…% of live)
```

**What the numbers mean.** The kit times each impression from the moment one `ad_server.py` publishes it to
the moment another has applied it to its cap view, through Python clients, on whatever machine you run it on.
CI result on a shared 2-vCPU runner, reduced scale (3 ad servers at 400 on-demand slots/s each over 60,000
profiles, 2,400 live profiles with a 3-slot break every 10 s, 20-second cap windows; ranges over two green runs,
[run 39688431840](https://github.com/psinglap/laserdata-kits/actions/runs/39688431840) and [run 39700037915](https://github.com/psinglap/laserdata-kits/actions/runs/39700037915)): impression-to-cap-view **p50 2.9–4.8 ms, p99 5.7–8.5 ms,
p99.9 6.5–9.6 ms** (medians of 66–67 steady-state seconds per server; worst second p99 20.0–32.5 ms), every
steady-state second inside the 50 ms budget. In live-break seconds, when each reader took about 2,000 impressions
a second, p99 was 7.5–13.0 ms (median). About 14,700 profile-campaign pairs reached their cap in each window;
across six full windows of about 38,400 impressions (14,400 of them live), **no impression went past a cap and no
live sponsor was shown twice to one profile**. Each server dropped 451–523 repeated impressions by event ID, with
no sequence gaps. With 2 MB/s of playback beacons on the same broker, p99 moved from 5.3 to 5.8 ms (run 1) and 8.1
to 8.4 ms (run 2). A restarted ad server rebuilt its cap view from the log in 0.38–0.57 s (about 18,000 profiles,
48,000 impressions) before deciding ads. The same run with every server applying the others' impressions 2 seconds
late (`--hold-ms 2000`): **25.1% of impressions over cap, and 9,600 of 14,400 live impressions (66.7%) a sponsor
the profile had already seen**, every second and third slot of every pod. That is the end-to-end lane through this
demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`,
on dedicated machines.

## Three tests that match your peaks

1. **A live ad break.** `--live-profiles 20000 --ramp-ms 2000` puts more of the audience into each break. Pass:
   impression-to-cap-view p99 stays under 50 ms in break seconds, and live sponsor repeats stay under 0.1%.
2. **Ad playback beacons next door.** `python playback_beacons.py --mbps 50` writes quartile and progress
   beacons to their own topic on the same broker. Pass: the lane's p99 moves by less than 10%.
3. **Restart an ad server.** Stop one and start it again. It reads the lane from the start of the previous
   cap window, rebuilds every profile's counts, then decides ads. Pass: it rebuilds in seconds, with no
   sequence gaps and no impression counted twice.

And one what-if: `--hold-ms 2000` makes each server apply the others' impressions only once they are 2 seconds
old, as when events spill into retries and dead-letter queues under load.

## Try it on your traffic

`kafka_bridge.py` joins the Kafka topic your ads event publisher writes enriched ad events to, as its own
consumer group `iggy-shadow`. It keeps impressions, drops a repeat of an event ID it has already seen, and
writes running counts per profile and campaign into Apache Iggy. It only reads, so the publisher and every
existing consumer are untouched. Map your record in `to_impression()`.

```
python kafka_bridge.py --bootstrap <broker> --kafka-topic <enriched-ad-events-topic> \
  --profile-field <profile-field> --campaign-field <campaign-field> \
  --type-field <event-type-field> --id-field <event-id-field> --live-campaign <sponsor-id>
python ad_server.py --name shadow --rate 0 --live-profiles 0 --window-s 86400
```

Apache Iggy has an official Java SDK, so a Flink job or a JVM service can write to the lane directly once the
shadow run looks right.

## Files

| File | What it is |
|---|---|
| `ad_server.py` | One ad server: on-demand and live-pod decisions under caps, impressions out, everyone's in |
| `playback_beacons.py` | A heavy neighbour: ad playback beacons on their own topic |
| `kafka_bridge.py` | Shadow your enriched ad events topic into the lane, read-only |
| `common.py` | Stream, topics, keys, record shape |
| `bench/PLAN.md` | The benchmark plan against Kafka, rules fixed first |
| `docker-compose.yml` | Apache Iggy 0.9.0 |
| `ci.sh` | The CI smoke test on a real broker |

## What Apache Iggy 0.9 has today, and what it doesn't

Replication (Viewstamped Replication clusters) shipped in September 2026. Delivery is at-least-once; the
running count with a sequence makes repeats harmless. A Kafka-compatible gateway is in progress and not in 0.9,
which is why the trial uses `kafka_bridge.py` and why the Data Mesh, Flink jobs and every other consumer stay
on Kafka.

Contact: kranti@laserdata.com · laserdata.com
