# Real-time ad beacons for Amagi THUNDERSTORM, on LaserData

Every ad segment your stitchers serve reaches the tracker that owns the session in milliseconds, and every quartile
beacon fires once, never for a quartile the viewer did not reach, even when an event arrives twice or a tracker
restarts. It runs beside Kinesis and Redis, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Amagi_QuartileBeacons.pdf`](LaserData_Amagi_QuartileBeacons.pdf)

## Why this use case

From the THUNDERSTORM product page: "When an ad is played, a beacon is fired and registered in the ad network system
as an impression", and "we also perform quartile reporting, firing beacons at the start, 25%, 50%, 75%, and 100%
completion". From the THUNDERSTORM team's tech blog ("Designing SSAI systems to achieve multi-million concurrency",
December 2021): about 100K concurrent viewers per linear FAST channel "with close to 500 million impressions per
month", "up to 2 million concurrent users" planned for the 2020 US election; "We use Kinesis streams to write user
session data and trackers data periodically"; "Elasticache-Redis is used to store replaced ad segments details and
quartile tracking URLs per user"; Kinesis shard changes are allowed "only 10 times in a rolling 24hr window", so the
team built its own shard scaler; and an open problem: "Reducing latency in communication pipeline (say, Kafka to SQS
to gRPC)". From "Combating 5 biggest challenges of SSAI" (December 2022): "The major challenge with server-side ad
insertion is getting the timing right." From the Q4 FY26 shareholder letter (May 2026): 42.4 billion monetized
impressions in FY26, up 62%, across SSAI on THUNDERSTORM and Ads Plus. From the IPO filing (via Devoncroft, July
2025): cloud infrastructure above 27% of revenue, the largest cost-of-goods line.

With server-side insertion the player never tells the ad network what it saw; THUNDERSTORM does, from the segments it
serves. On a linear channel the break is one moment for everyone watching, so the segment fetches arrive as a wall.
This kit runs that one lane on Apache Iggy: every ad segment served, keyed by session, into the tracker that owns the
session, which fires each quartile beacon when the segment holding that point is fetched, and writes every beacon it
fired to a log. An audit checks every beacon against the quartiles the served segments made due: missing, fired
twice, and fired for a quartile never reached must all be zero.

Amagi has not published how THUNDERSTORM fires its beacons today, or whether the 2021 architecture is still current;
this kit is one lane beside Kinesis, Redis and the stitchers, not a replacement for them.

## Run it

Needs Docker and Python 3.10+. Defaults: 100,000 viewers on 50 FAST channels, a break of four 30-second ads in
2-second segments every 15 minutes per channel (staggered across channels), every viewer on a channel reaching the
break within 2 seconds, 8% of ads left partway and 0.5% of segment events sent twice. All illustrative: set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python tracker.py --trackers 2 --index 0 --name tracker-1 &
python tracker.py --trackers 2 --index 1 --name tracker-2 &
python stitcher.py                   # 100,000 viewers: segments out, audit in
```

They print a line a second, and the audit at the end:

```
[tracker-1] … segments/s  segment-to-tracker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
            beacons fired … (impressions …)  repeats dropped …  fired after the ad ended …  stall gc …ms loop …ms
[stitcher]  … segments/s (… of 50 channels in a break)  beacons due …  recorded …
            segment-to-beacon-record p50 …ms  p99 …ms  fired twice 0  not due 0
[audit] beacons due … (impressions …)  recorded …  missing 0  fired twice 0  not due 0
        segment events sent twice …  ads left partway …
```

"Fired after the ad ended" counts beacons that fired after the viewer's ad had already finished on screen. "Stall" is
the longest garbage-collection pause and event-loop stall in that process that second, so a slow second can be traced
rather than guessed at.

**What the numbers mean.** The kit times each segment event from the moment `stitcher.py` serves it to the moment
`tracker.py` has handled it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner
with the broker and all scripts on one box, at reduced scale (5,000 viewers on 12 channels, a break of three 8-second
ads in 1-second segments every 32 s per channel, about 3,600 segment events/s in bursts as breaks begin, two trackers
of about 1,800/s each), three green runs held segment-to-tracker p50 5.2–8.7 ms, p99 9.6–16.7 ms and p99.9
11.0–18.7 ms (medians of the per-second figures; 156 of 156 steady-state seconds inside 50 ms, worst single second
28.9–46.3 ms). With 2 MB/s of session records on the same broker, the lane's p99 went from 6.9–11.6 ms to 9.7–16.8 ms
(medians per second): on a 2-vCPU box shared by the broker and every script, the neighbour costs CPU the lane also
needs. Each run's audit checked 112,032–112,650 beacons (about 25,000 impressions): none missing, none fired twice,
none fired for a quartile never reached, with about 2,000 ads left partway and 895–936 segment events sent twice
(tracker-1 dropped 273–291 repeats); no beacon fired after its ad had ended. tracker-2, stopped mid-run and started
again, rebuilt 23,117–23,357 fired beacons from the log in 0.18–0.25 s, caught up in 0.60–0.88 s and fired the
207–278 beacons it had missed late but once. That is the end-to-end lane through this demo code, not the broker
alone; the broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The
tracker in the kit only counts its beacons; your HTTP call to each tracking URL adds its own time after this.

| File | What it is |
|---|---|
| `stitcher.py` | FAST channels, ad breaks and the viewers watching them, as the stitchers see them. Publishes every ad segment served to `ad-segments`, keyed by session; some viewers zap away mid-ad, some events are sent twice. Audits every beacon in `beacons-fired`. |
| `tracker.py` | The tracker: owns a share of the lane's partitions, fires each (impression, quartile) beacon once, writes it to `beacons-fired`, rebuilds from that log on restart. `fire()` is where your HTTP call goes. Prints the lane's p99. |
| `session_records.py` | Bulk session records on their own topic, for the isolation test. |
| `kinesis_bridge.py` | Copies ad segment records from your Kinesis stream into Iggy, read-only, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by session, the quartile rule. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Election night on one channel:** `python tracker.py --trackers 4 --index 0..3` and `python stitcher.py --channels 5`
   (ten times as many players per break). Pass: segment-to-tracker p99 under 50 ms through each break start, and
   the audit shows zero missing, zero fired twice, zero never due.
2. **Session records next door:** `python session_records.py --mbps 50` while they run.
   Pass: the tracker lane's p99 moves by less than 10%.
3. **Restart a tracker in a break:** stop one tracker with Ctrl-C (it writes out every beacon record it holds) and
   start it again. It reads back what it fired in the last `--lookback-s` seconds, replays its segments from the same
   moment, and fires what it missed. Pass: it catches up within a second or two, and the audit still shows no beacon
   fired twice and none missing.

## Shadow your own traffic

```
pip install boto3
python kinesis_bridge.py --stream <your-tracker-stream> --region us-east-1
```

The bridge only reads (ListShards, GetShardIterator, GetRecords), from LATEST, within each shard's five reads a
second; on a busy stream, run it on a copy or register it as an enhanced fan-out consumer. The stitchers and every
consumer of Kinesis are untouched. Map your record in `to_segment()`: the kit needs the session, the impression (or
break and slot), the creative, the segment index, and the ad and segment lengths. Because those records are written
periodically today, a shadow run checks the beacon logic and counts against yours; the latency comes from the
stitcher publishing to the lane directly. Run it in your own AWS account, next to the stitchers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, and there is no Kinesis API, so the trial uses the bridge
and Kinesis stays. Delivery is at-least-once: the (impression, quartile) beacon ID makes repeats harmless, but a
tracker that crashes hard (rather than stopping cleanly) can fire its last 5 ms batch of beacons again on restart.
Replicated clusters (Viewstamped Replication) shipped in September 2026. Partitions are set when a topic is created
(more can be added, which moves sessions), so size for the biggest night up front. Storage is local disk per node, not
object storage: that keeps the tail in milliseconds, and it is why this is one lane with hours of retention.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored tracker stream, alongside Kinesis. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
