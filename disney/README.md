# Live watch signals for Disney+ and ESPN personalization, on LaserData

Every start, switch and stop of a live game reaches the personalization features in milliseconds, so
the next Live rail matches what the viewer is watching. It runs beside Kinesis, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Disney_LiveWatchSignals.pdf`](LaserData_Disney_LiveWatchSignals.pdf)

## Why this use case

From Disney's own sources:

- "Disney+ uses Amazon Kinesis to drive real-time actions like providing title recommendations for
  customers", and the platform "ingests billions of events per hour in Amazon Kinesis Data Streams",
  processed in Managed Service for Apache Flink (AWS customer story; re:Invent 2020 ANT309, Martin
  Zapletal, Disney+). Hundreds of streams, thousands of shards, multiple regions.
- The Event Platform team is "transmitting billions of client-emitted signals throughout the Disney
  ecosystem", with "real-time enrichment and aggregation" for nearline uses across Disney+, Hulu and
  ESPN (Disney job post, Sep 2026).
- "ESPN is investing in large-scale data infrastructure and real-time processing platforms" for
  "next-generation personalization and live sports experiences", including real-time feature delivery
  for online ML services (Disney job post, Oct 2026). ESPN on Disney+ reached about 100 markets in
  April 2026, growing to thousands of live events a year.
- Disney's open-source Rust Kinesis client polls each shard with a 200 ms gap between reads and does
  not yet support enhanced fan-out (`disney/disney-kinesis-client-rust`).

Kinesis is the right home for billions of events an hour, and this kit does not touch it. A Live rail
is a narrower job with a tighter clock: a viewer switches games and goes back to the rail a second
later. AWS documents an average of about 200 ms for a shared-throughput Kinesis read with one consumer
on the stream and about 1 s with five (about 70 ms with enhanced fan-out, billed per consumer and
shard), per hop. This kit runs watch signals on their own lane, keyed by profile, read by the feature
service directly, and counts the outcome: rails built without the viewer's last signal.

Disney does not publish which stream carries playback starts and stops, how the Live rail reads its
features, or the gap to the next rail request. The lane is a proposal; the kit's flags let you set
those to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 5 million profiles in a live window, 60 live events airing,
20,000 watch signals a second (start, switch, stop), half of them followed by a Live rail request
0.3 to 2 s later, and a 2× tip-off surge for the first minute. These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python features.py &                   # the live personalization features, building every rail
python apps.py &                       # viewers starting, switching and stopping live events
python client_signals.py --at 20       # a heavy client-signal stream next door
```

Each prints a line a second:

```
[apps]  40,000 watch signals/s (surge)  start …  switch …  stop …  Live rail requests …  watching now …
[features]  40,000 signals/s  signal-to-feature p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  profiles in view …
[rail] Live rail requests …  tiles …  built without the last signal …%  wrong tiles shown …
```

`built without the last signal` is the business number. Each signal carries the resulting state (the
event the profile is watching now, and the one it just left) with a sequence that only grows per
profile. Each Live rail request carries the sequence of the profile's last signal and the live events
airing. `features.py` builds the rail ("Watching", "Back to", "Watch live") and asks whether it had
applied the profile's last signal by the time the request started. If not, the rail was built on stale
features; if the stale now-watching event differs, the viewer saw a wrong tile.

What-if: `python features.py --hold-ms 1000` applies every signal only once it is 1 s old, the average
AWS documents for a shared Kinesis read with five consumers, so you can see how many rails such a path
would build on stale features.

**What the numbers mean.** The kit times each signal from the moment `apps.py` sends it to the moment
`features.py` has applied it, through Python clients, on whatever machine you run it on. On a shared GitHub
runner (2 vCPU), with the broker and four Python processes on one box, at 1,500 watch signals a second over
200,000 profiles and 40 live events (2x for 15 s as the surge), two green CI runs held p50 4.0 to 5.5 ms,
p99 7.1 to 9.4 ms and p99.9 8.4 to 10.7 ms (median per second over 62 steady-state seconds each, no second
over the 50 ms budget). Through the surge (3,000 signals a second) the median per-second p99 was 8.9 to
11.6 ms, worst 12.2 to 16.6 ms, with a 1 MB/s client-signal stream next door. Of about 57,000 to 58,000 Live
rail requests per run, none was built without the profile's last signal and no wrong tile was shown. The
what-if with every signal applied 1 s late built 40.8 to 40.9% of rails (median per second) without the last
signal, about 9,900 per run with a wrong now-watching tile. That is the end-to-end lane through this demo code
at reduced scale, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `apps.py` | The Disney+ and ESPN apps: watch signals per profile on `watch-signals` (resulting state, sequence only grows per profile), and the profile's next Live rail request on `live-rail-requests`. `--gap-min/--gap-max`, `--switch-share` and `--surge` shape the load. |
| `features.py` | The live personalization feature service: applies signals by sequence, times signal-to-feature, builds every Live rail and checks it against the profile's last signal. `--hold-ms` is the what-if. |
| `client_signals.py` | A bulk, latency-tolerant client-signal stream (playback beacons, analytics) on its own topic on the same broker. |
| `kinesis_bridge.py` | Reads your playback events from a Kinesis stream with GetRecords, read-only, and publishes them to the lane, keyed by profile, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A slate tips off:** `python apps.py --signals-per-s 20000 --surge 2 --surge-for 60`.
   Pass: signal-to-feature p99 under 50 ms through the surge, and no rail built without the last signal.
2. **Client signals on the same broker:** `python client_signals.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the feature service:** stop `features.py` and start it again. It rebuilds what every
   profile is watching from the lane, prints "catching up", then times live signals again.
   Pass: no signal applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install boto3
python kinesis_bridge.py --stream <your-playback-events-stream> --region us-east-1
```

The bridge only reads; the apps, the Event Platform and every existing consumer and Flink application
are untouched. Each shard allows five GetRecords calls a second shared by every polling consumer, so
run it on a copy of the stream or for a short window, or register it as an enhanced fan-out consumer.
Map your field names in `to_signal()`: the kit needs the profile ID and the live event it is now
watching (empty on stop). In shadow mode the clock starts when the bridge reads the record. Everything
runs in your AWS account.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored watch-signal stream, beside
Kinesis. No advantage, no migration.

kranti@laserdata.com · laserdata.com
