# Real-time episode publish status for Spotify, on LaserData

Every upload's receipt and every stage change after it reaches the episode status service in milliseconds, so a
creator sees "received and queued" at once, a re-upload of a file already in the pipeline is answered "already
received" instead of being transcoded twice, and a transcoding backlog pages within a second of crossing its
line. It runs beside Pub/Sub and the publishing pipeline, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Spotify_EpisodeStatus.pdf`](LaserData_Spotify_EpisodeStatus.pdf)

## Why this use case

On June 24, 2026 "our video transcoding infrastructure reached maximum capacity", and video podcast episodes that
normally publish within minutes were delayed by several hours. A bulk spike in content delivery met a scheduled
batch job re-processing existing episodes, a costlier encoding change and a scheduler bug. New episodes wait in a
medium-priority queue and updates to older episodes in a low-priority one. Creators whose episodes seemed missing
re-uploaded them, which added load, and Spotify wrote: "the system should have confirmed their upload was received
and queued, and it did not." Roughly four hours passed between the first alerts and formal incident response.
The planned follow-ups include prioritising creator-facing content over background work, backpressure, and telling
creators sooner (Spotify Engineering, "Incident Summary: Content Ingestion & Podcast Video Publishing", July 2026).

The root cause was transcoding capacity, and a status lane does not add capacity. What it changes is how fast
everyone knows: the creator gets a receipt in milliseconds, a re-upload is answered from the view, and the watch
reads the oldest waiting new episode off the lane instead of waiting for a queue-depth threshold. Spotify has not
published how its publishing pipeline records stage changes; the kit is a lane beside it, not a claim about it.

## Run it

Needs Docker and Python 3.10+. Defaults (illustrative; Spotify has not published its rates): 400 new episode
uploads and 150 updates a second, a transcoding pool that starts 800 a second, about 2 minutes of transcoding and
30 s of content analysis per episode, a bulk delivery of 4x new uploads for 5 minutes from minute 1, 10% of creators
re-uploading after 15 minutes without going live, a 5-minute line for the oldest new episode, and 0.5% of stage
changes delivered twice.

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python status.py &                         # the episode status service and its backlog watch
python pipeline.py                         # uploads, transcoding, analysis, publishing (simulated)
```

`status.py` prints a line a second, and a line for each alert:

```
[status-1] stage changes 1,977/s  stage-change-to-view p50 4.5ms  p99 7.8ms  p99.9 8.5ms  PASS (<50ms)  receipts 780/s (p99 7.7ms, within 1 s 100.0%)  re-uploads answered 0 wrong 0  waiting new 397 (oldest 0.6s) updates 86 (oldest 1.1s)  repeats dropped 143  gaps 0
[watch] ALERT new episodes waiting: oldest 3.05s over the 3s line, 2,132 waiting; crossed at 1792725342.660, raised at 1792725342.708, 48 ms after it crossed
```

`pipeline.py` prints the truth the watch is judged against: the real queues each second, and the exact moment the
oldest new episode crossed the line (and fell back under half of it).

**What the numbers mean.** The kit times each stage change from the moment `pipeline.py` publishes it to the moment
`status.py` has applied it to its view, through Python clients, on whatever machine you run it on. CI result on a
shared 2-vCPU runner, reduced scale (200 new uploads and 80 updates a second into a pool of 330 a second, a 3.5x
bulk delivery for 12 s, a 3-second line; ranges over three green runs,
[run 39893975068](https://github.com/psinglap/laserdata-kits/actions/runs/39893975068),
[run 39894165427](https://github.com/psinglap/laserdata-kits/actions/runs/39894165427) and
[run 39894363946](https://github.com/psinglap/laserdata-kits/actions/runs/39894363946)): about 1,640 stage changes a
second, up to 2,318 during the bulk delivery; stage-change-to-view **p50 4.7–7.9 ms, p99 8.6–15.1 ms, p99.9
9.6–16.6 ms** (medians of 63 steady-state seconds per run; worst second p99 21.2–33.9 ms), with 189 of 189
steady-state seconds inside the 50 ms budget. **Every receipt was in the view within a second** (receipt p99 8.3–14.5
ms, median per second). **2,404–2,429 re-uploads were answered "already received" from the view, with no wrong
answer.** The oldest new episode waited up to 13.2 s against the 3 s line; **the watch raised its alert 43–85 ms
after the real crossing** and cleared 45–124 ms after the real fall under half the line, with no alert without a
crossing. A replica started in the middle of the backlog rebuilt its view from the log in 0.33–0.52 s (about 16,700
episodes from about 63,000 stage changes, about 3,600 new episodes waiting) and alerted on the backlog it found. The
reader dropped 600–625 repeated stage changes by event number, with no sequence gaps. With 2 MB/s of the
reprocessing job's segment log on the same broker, p99 went from 7.3 to 8.1 ms, 13.3 to 15.1 ms and 7.4 to 8.1 ms.
That is the end-to-end lane through this demo code, not the broker alone; the broker's own tail latency is measured
with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

## Three tests that match your peaks

1. **A bigger bulk delivery.** `--spike-x 8 --spike-for 600` puts eight times the new uploads into the pipeline for ten
   minutes. Pass: stage-change-to-view p99 stays under 50 ms, every receipt is in the view within a second, and the
   watch alerts within a second of the real crossing.
2. **The reprocessing job next door.** `python reprocess_flood.py --mbps 50` writes the batch job's segment log to its
   own topic on the same broker. Pass: the lane's p99 moves by less than 10%.
3. **A replica mid-backlog.** Start a second `status.py --name status-2` while the backlog is over the line, as a
   deploy or a new region would. Pass: it rebuilds from the log in seconds and alerts on the backlog it finds.

And one policy check: `--policy share` takes from both queues in turn instead of new episodes first, so you can see
the oldest new episode's wait with and without the prioritisation your follow-ups plan.

## Try it on your traffic

If your publishing pipeline already publishes stage changes to a Pub/Sub topic, `pubsub_bridge.py` reads them through
its own subscription `iggy-shadow` and writes each one into Apache Iggy, keyed by episode. A Pub/Sub topic delivers
every message to every subscription, so publishers and every subscriber you run today are untouched. Map your
record in `to_msg()`: the kit needs the episode key, the stage, the priority queue, the time it was queued and a
per-episode event number.

```
gcloud pubsub subscriptions create iggy-shadow --topic=<stage-change-topic> --enable-message-ordering
python pubsub_bridge.py --project <gcp-project>
python status.py --name shadow
```

Compare the shadow's queues and alerts with your dashboards. If no such topic exists, the upload API and the workers
write to the lane directly, one small write per stage change. Apache Iggy has official Go, Java, Node.js, Rust, C#
and Python SDKs.

## Files

| File | What it is |
|---|---|
| `status.py` | The episode status service: receipts, re-upload answers, the backlog watch, rebuild from the log |
| `pipeline.py` | The publishing pipeline, simulated: uploads, priority queues, transcoding pool, analysis, live, and the truth |
| `reprocess_flood.py` | A heavy neighbour: the reprocessing job's segment log on its own topic |
| `pubsub_bridge.py` | Shadow a Pub/Sub topic of stage changes into the lane, read-only |
| `common.py` | Stream, topics, keys, record shape |
| `bench/PLAN.md` | The benchmark plan against Kafka, rules fixed first |
| `docker-compose.yml` | Apache Iggy 0.9.0 |
| `ci.sh` | The CI smoke test on a real broker |

## What Apache Iggy 0.9 has today, and what it doesn't

Replication (Viewstamped Replication clusters) shipped in September 2026 and is new; cross-region replication is
not built in, so run a lane per region and mirror it. Delivery is at-least-once; the per-episode event number makes
repeats harmless. A Kafka-compatible gateway is in progress and not in 0.9, and Iggy does not speak the Pub/Sub API,
which is why the trial uses `pubsub_bridge.py` and why every existing subscriber stays where it is. LaserData Cloud
runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure, run Apache Iggy
self-hosted on your own VMs; LaserData supports it.

Contact: kranti@laserdata.com · laserdata.com
