# Real-time anti-cheat terminations for Riot Games, on LaserData

Every shot reaches the anti-cheat scorer, and every match termination reaches its game server, in milliseconds,
before the cheater's next shot lands. It runs beside MSK, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.riot-games-kit
- **One slide:** [`LaserData_RiotGames_MatchTermination.pdf`](LaserData_RiotGames_MatchTermination.pdf)

## Why this use case

From the Anti-Cheat job post ("Staff Applied AI/ML Scientist, Anti-Cheat", Oct 2026): "Design, train, and deploy
real-time machine learning models to detect gameplay anomalies", "evaluating millions of high-throughput game
actions per second with minimal latency impact", and "engineer high-signal features from raw game client
telemetry".

From "Vanguard x VALORANT" (playvalorant.com, Sep 2024, by a data engineer on the Vanguard team): "Many times,
we're able to immediately detect when a cheater has entered the game, but we don't immediately ban them", since a
delay keeps detections valid for longer; meanwhile "players are losing their hard-earned RR to these cheaters".
Riot is "experimenting with mixing in match terminations" (and player notifications) as an alternative to bans on
console, and since patch 10.05 (Mar 2025) Ranked Rollbacks refund RR lost to a confirmed cheater.

From "VALORANT's 128-Tick Servers" (Riot tech blog, 2020): a frame budget of "just 2.34ms", 3 games per core and
108 games per 36-core host. Whatever carries game actions off the server has to cost the frame almost nothing.

From AWS re:Invent 2022 ("How Riot Games processes 20 TB of analytics data daily on AWS", ANT341): game telemetry
runs on Kafka and Amazon MSK at 500,000 events per second and 75 TB a day, with a 15-minute analyst SLA, and REBus,
the Riot Event Bus, runs service-to-service events on MSK hubs. Those stay where they are; this kit is about one
small lane beside them.

Ending a match for cheating is a loop with two hops: a shot has to reach the scorer that judges the player, and the
termination has to reach the game server running that match. While the termination is in flight the aimbot keeps
firing and the match keeps counting toward rating. This kit runs that one loop on Apache Iggy, prints both hops'
latency every second, and prints the aimbot shots that still landed after a termination was published, next to
what a lane one second behind would have let land.

## Run it

Needs Docker and Python 3.10+. Defaults: one shard, 8 game server hosts × 108 matches, 10,000 shots a second, an
aimbot switched on every 2 s (8 shots a second, inhumanly fast and on target) and matches compressed to 120 s. All
rates are illustrative; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python anticheat.py &                # the anti-cheat scorer: shots in, terminations out
python game_servers.py               # 8 game server hosts: shots out, terminations in
```

They print a line a second:

```
[anticheat]  10,000 shots/s  action-to-scorer p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
             terminations sent …  matches terminated …
[servers]  10,000 shots/s (steady)  aimbot shots …  refused …  terminations … (aimbot caught in …ms median)
           cheater shots landed after the termination … (at 1000 ms it would be …)
           cheater matches with a result 0  false terminations 0  termination-to-host n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each shot from the moment `game_servers.py` sends it to the moment
`anticheat.py` has applied it, and each termination from `anticheat.py` to the game server host running the match,
through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (3,000 shots/s, 4 hosts × 75 matches, 40 s matches), three green runs held
action-to-scorer p50 3.5–7.6 ms, p99 6.3–14.1 ms and p99.9 7.1–15.7 ms (all 60–62 steady-state seconds of each run
inside 50 ms), and a termination reached its host in 2.1–3.3 ms (median over the seconds with a termination). With
2 MB/s of analytics telemetry on the same broker, the lane's p99 moved by 0.6 ms or less. Each run terminated 30
cheater matches with no false terminations, and no cheater match ended with a result. Hosts let 0 aimbot shots land
after a termination was published; a lane one second behind would have let 246–248 land. The kit's stand-in model
needs about 640 ms (635–639 ms) of aimbot hits to decide; that part is your models' and is not what the lane
measures. All of this is the end-to-end loop through this demo code, not the broker alone. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39894994877,
https://github.com/psinglap/laserdata-kits/actions/runs/39895171224,
https://github.com/psinglap/laserdata-kits/actions/runs/39895326565

| File | What it is |
|---|---|
| `game_servers.py` | A fleet of game server hosts. Every shot publishes one record (host, match, player, weapon, reaction time, snap, hit, headshot, per-match sequence) to `combat-actions`, keyed by match, one batch per partition per 10 ms tick. Each host reads `match-terminations` directly on its own connection and ends its own match the moment a termination arrives, with no result recorded. It also plays the players: human shots, aimbots switched on mid-match, and matches ending with a result. |
| `anticheat.py` | The anti-cheat scorer. Reads the 16 combat-action partitions directly, keeps each player's last 8 hits per match (`suspicious()` is the stand-in for your models), and publishes each termination with the host, the player and the evidence. |
| `analytics_flood.py` | A heavy analytics-telemetry stream on its own topic, for the isolation test. |
| `msk_bridge.py` | Copies combat actions from Amazon MSK into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Evening queue:** `python game_servers.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: action-to-scorer p99 under 50 ms, and aimbot shots landed after a termination under 1% of what a lane
   one second behind would let land.
2. **Analytics next door:** `python analytics_flood.py --mbps 20` while both run.
   Pass: the combat lane's p99 moves by less than 10%.
3. **Restart the scorer:** stop `anticheat.py` and start it again; it replays the combat-action topic and rebuilds
   its windows and its list of terminated matches, publishing again only terminations younger than
   `--replay-terminate-s`. A repeated termination changes nothing on the host.
   Pass: it catches up within seconds and no match is ended twice.

## Design notes

- A termination carries the match's resulting state ("terminated") with a sequence, so at-least-once delivery is
  safe: a repeated or replayed termination changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this
  lane doesn't need it.
- The game server never waits on the lane: shots go out in one batch per partition per tick, so the cost per frame
  is one small write, not one per shot.
- The lane carries gameplay features, never Vanguard client internals. Your models and your ban policy stay the
  source of truth; the log is how a game server hears about a decision, and a durable, replayable record of each
  termination and its evidence, for review and for the agents that action flagged players.
- How game actions reach Riot's anti-cheat models today, and whether scoring runs on the server, a backend or the
  client, is not public. If scoring runs on the game server itself, the same lane carries the features out for
  review and model updates back in.
- Match terminations ship as a test on console, per Riot's 2024 post. Where a ban is delayed on purpose, the same
  lane can carry a quiet flag instead, and the termination when policy calls for it.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-bootstrap:9092> --kafka-topic <combat-actions-topic> \
  --match-field <match-id-field> --player-field <puuid-field> --rt-field <reaction-ms-field>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the game servers, REBus, Honu and every other
consumer are untouched. Map your record in `to_shot()`: the kit needs the match, the player, whether the shot hit,
and the features your model reads.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored combat-actions topic for one shard,
alongside MSK. No advantage, no migration.

kranti@laserdata.com · laserdata.com
