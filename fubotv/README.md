# Key-play Game Alerts for Fubo, on LaserData

Every key play reaches Game Alerts in milliseconds and is pushed the moment its clip is playable, so a tap always
opens the play; a home run overturned on review before then is never pushed, one overturned after is corrected,
and a Game Alerts restarted mid-game is current from the log within a second and pushes nothing twice. It runs
beside Kafka and Pub/Sub, so your moment models, the cloud DVR, your Go services, the Beam and Scio pipelines and
the push provider stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Fubo_KeyPlayAlerts.pdf`](LaserData_Fubo_KeyPlayAlerts.pdf)

## Why this use case

From Fubo's own posts and product:

- Game Alerts expanded (April 9, 2026): "Now, pro baseball fans can receive push notifications about the biggest
  plays", and one tap opens the play, for example a go-ahead home run. Fubo's AI identifies key moments in the
  coverage fans have DVR'd. Isaac Josephson, SVP Product Management: "At Fubo, we've already mastered the living
  room. Now, we're conquering the on-the-go space." (Fubo press release, Business Wire.)
- In 2025, "Fubo is distributing game alerts to opted-in subscribers in real time based on their past viewership
  and location", at team level (Awful Announcing, April 2025).
- Fubo's data stack: "Streaming: Apache Kafka, Google Pub/Sub", "Apache Beam, Scio", "BigQuery, Redis, Bigtable",
  on GCP with Kubernetes (Software Engineer, Data Engineering, 2026). Backend services are Go microservices on GCP
  (Engineering Manager, Backend, October 2026).
- 5.7 million subscribers in North America across Fubo and Hulu + Live TV (FuboTV FQ2 2026 earnings call, May 2026).

This kit runs one narrow lane: every pitch of every live game, every key play, the clip-ready event for it and any
replay review of it, keyed by game with a per-game sequence, into Game Alerts. Game Alerts holds a key play until
its clip is playable, records each alert before it goes out, and reads the lane directly with its own position,
so another reader on the same lane (an in-app key-moments row, the AI Assistant) never slows it. Fubo has not
published how key plays, clips and reviews reach Game Alerts today, how long a clip takes to cut, or whether
reviews change an alert; those are our assumptions, and the first thing to check with your team.

## Run it

Needs Docker and Python 3.10+. Defaults: a full slate of 15 live pro baseball games, pitch by pitch, game time 20x
real time so every second has moments to time (`--speed 1` for real time); clips playable 4-12 s after the play
and reviews 30-120 s after it, on real clocks (both illustrative, `--clip-s`, `--review-s`).

```
docker compose up -d                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt       # apache-iggy 0.9.0
python moments.py &                   # every pitch, key play, clip and review, every live game
python game_alerts.py                 # push on clip ready, held back or corrected on review, never twice
```

Each prints a line a second:

```
[moments]   15 moments/s  key plays 1  clips ready 0  … writes  write ack p50 …ms  p99 …ms
[alerts]     1 alerts/s  clip-to-alert p50 …ms  p99 …ms  PASS (<50ms)  pushed 1  waiting for clip 2
             held back by review 0  corrected after push 0  pushed late 0  gaps 0  play-to-push …s
```

`waiting for clip` is how many key plays are held right now for their clip. `held back by review` counts alerts
never pushed because the call was overturned first; `corrected after push` counts the ones overturned after the
alert went out. `play-to-push` is the median time from the key play to the push decision, almost all of it the
clip's cutting time. `gaps` must stay 0.

**What the numbers mean.** The kit times each alert from the moment the clip-ready event was created (including its wait for the next write) to the moment `game_alerts.py` decides the alert, through Python clients, on whatever machine runs it. On a shared
2-vCPU GitHub runner, with the broker, the game feed, Game Alerts and the viewing-events script on one box, at about
250 moments a second across 80 games with clips playable 1-4 s after each key play, three green CI runs on the
final code held a median per-second clip-to-alert p50 of 4.8 to 5.9 ms and p99 of 6.6 to 8.0 ms against a 50 ms
budget (worst second 8.3 to 10.1 ms), passing in every one of 53 to 54 steady-state seconds with alerts. A 4x
viewing-events surge next door left the median p99 at 6.7 to 8.1 ms (7.1 to 8.0 ms before it). Each run pushed 205
to 230 alerts in its steady-state seconds, held back 10 whose home run was overturned before the clip was ready,
and corrected 19 to 20 overturned after the push, with no gaps. Restarted mid-run, Game Alerts rebuilt all 80 live
games (242 to 276 key plays) from the log in 0.06 to 0.11 s, and the record held 375 to 411 alerts with none
recorded twice. That is the end-to-end lane through this demo code at reduced scale, not the broker alone, and it
excludes the moment model, the clipper, your services, the push provider and the fan's network. The broker's own
tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `moments.py` | The game feed: one writer per partition, every pitch of every live game keyed by game id, a key play when the moment model would flag one (home runs, go-ahead and game-tying hits), its clip seconds later, and replay reviews of some home runs, upheld or overturned. Each message carries the game's sequence, the play id and the resulting score. Teams and plays are illustrative. |
| `game_alerts.py` | Game Alerts: holds each key play until its clip is ready, then decides the alert; never pushes one overturned first, corrects one overturned after; records each alert on `alerts-sent` (tagged with `--name`) before push, so a restart pushes nothing twice. Rebuilds every live game from the log on start. `--audit` checks the record. |
| `viewing_events.py` | Player viewing events on their own topic, with a 4x first-pitch surge, for the isolation test. |
| `kafka_bridge.py` | Reads the Kafka topics your key plays, clips and reviews land on as one more consumer group (`iggy-shadow`) and writes them into Iggy, read-only, for a shadow run on real games. |
| `ci.sh` | The CI run: 80 games at 60x, Game Alerts, a viewing surge next door, Game Alerts restarted; pass only on 95% PASS seconds, zero gaps, overturns held back and corrected, a full rebuild and no alert recorded twice. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The evening slate starts:** `python viewing_events.py` while the rest runs: 5,000 viewing events a second,
   then 20,000 a second for 20 s. Pass: Game Alerts' p99 moves by less than 10%.
2. **A restart mid-game:** stop `game_alerts.py` and start it again, then `python game_alerts.py --audit`. Pass:
   every live game rebuilt within a second, PASS from the next line, `gaps 0`, and `recorded twice 0`.
3. **Another reader on the lane:** `python game_alerts.py --name key-moments`. Pass: the first Game Alerts' p99
   moves by less than 10%.
4. **Real time, a double slate:** `python moments.py --speed 1 --games 30`. Pass: p99 holds; `gaps` stays 0.

## Shadow your own games

```
python kafka_bridge.py --bootstrap <brokers> --topics <key plays>,<clips>,<reviews>
python game_alerts.py --name shadow
```

The bridge reads as one more consumer group, so producers and existing consumers are untouched. Map your records
in `to_moment()`: the kit expects a game id, the kind (pitch, key, clip, review), the play id and the score. If
these events live on Pub/Sub, read them from a new subscription instead; the rest stays the same. Compare the
shadow service's decisions with the alerts Game Alerts sends today. The latency it prints then starts at the
bridge, not at the moment model or the clipper.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so the feed and the bridge use the Iggy SDK (Go, Java, Rust,
Python, C# and Node.js); delivery is at-least-once, which is why every message carries the game's sequence and why
Game Alerts records an alert before it goes out (a crash in the few milliseconds between the record and push can
miss that alert); clustering with replication is new (September 2026), so the evaluation includes a failover step.
LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure,
run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored game-moments feed, beside Kafka and Pub/Sub.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
