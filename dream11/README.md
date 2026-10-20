# Live fantasy points on every ball for Dream11, on LaserData

Every ball's points reach every points node in milliseconds, in order per match, and a points
node added at the toss or restarted mid-match rebuilds every live match from the log. It runs
beside Kafka, so the consumer library, Flink, Aerospike, the leaderboard and everything else stay
where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/T1YuouTWRmWzhaiYG98hUv
- **One slide:** [`LaserData_Dream11_LivePoints.pdf`](LaserData_Dream11_LivePoints.pdf)

## Why this use case

From Dream11's own posts and talks:

- "To ensure real-time updates for game users, the Dream11 platform heavily relies on Apache
  Kafka in the critical pipelines of end user services." Peak concurrency passed 15 million in
  IPL 2024, with "hockey-stick traffic surges just before the start of matches". The legacy
  consumer hit "delays and data loss, severely impacting user trust", and a new low-level consumer
  "fixed our frequent rebalancing problem" (Dream11 at Current Bengaluru 2025).
- The leaderboard: points "updated every minute during every match", read by polling, with a
  stated plan for "a push model ... greatly reducing the time between an actual event and
  corresponding update of points in the frontend". The team view and the leaderboard must show
  "the same calculation event" (Dream11 Engineering, 2017).
- Spikes come from "the toss, squad announcement, team creation" and "mid-hour events like fall
  of wickets, hitting of sixes, hat-tricks"; autoscaling is too slow for them, so capacity is
  predicted and provisioned ahead of the match (Dream11 Engineering, 2021).
- IPL 2020: "8000+ contest joins per second" and "20000+ team updates per second".

Since 2025 contests are free to play, and the app adds live Watch Along streams; points that move
with the match matter as much as before. This kit runs one narrow lane: every ball of every live
match, with the resulting fantasy points of the players it touched, to every points node (the
service instances that hold team totals and push them to the app). Each node reads the lane with
its own offsets, so adding nodes before the toss rebalances nothing, and a node that joins
mid-match replays the match day's log and has every team total right before it serves.

## Run it

Needs Docker and Python 3.10+. Defaults: 40 live matches, a ball every 2 s per match (match time
compressed about 15× so every second has balls to time; `--ball-s 35` for real time), and points
nodes holding 200,000 user teams each (11 players, captain 2×, vice-captain 1.5×).

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python scoring.py &                        # the scoring feed, every ball of every live match
python points_node.py --name points-1 &    # a points node
python points_node.py --name points-2      # and another: each reads every ball
```

Each prints a line a second:

```
[points-1] 20 balls/s  ball-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)  team totals updated …/s
           pushed after a newer ball existed …%  gaps 0  totals off 0
[points-2] rebuilt from the log in …s: points for 40 of 40 live matches, … balls replayed, totals off 0;
           without the log … of 200,000 teams (…%) would show the wrong total
[scoring]  20 balls/s  … writes  write ack p50 …ms  p99 …ms
```

**What the numbers mean.** The kit times each ball from the moment the scoring feed created it
(including its wait for the next write) to the moment `points_node.py` has updated every team it
touches, through Python clients, on whatever machine you run it on. On a shared 2-vCPU GitHub
runner, with the broker, the feed, two points nodes and the surge script all on one box, at about
120 balls a second across 60 matches and 20,000 teams per node, the green CI run held a median
per-second p50 of 2.4 ms, p99 4.1 to 4.2 ms and p99.9 4.3 to 4.4 ms; every steady-state second
passed the 20 ms budget (63 of 63 and 48 of 48), and the worst second reached 9.3 ms. Each node
updated about 40,000 team totals a second. A 4× toss surge next door (250 to 1,000 events a
second at CI scale) left the median p99 at 4.1 ms. The restarted node rebuilt all 60 live matches
from the log (4,627 balls) in 0.08 s with every sampled team total matching a full recount;
without the log, 19,786 of its 20,000 teams (99%) would have shown the wrong total. No sequence
gaps, no totals off, and no points pushed after a newer ball existed. That is the end-to-end lane
through this demo code at reduced scale, not the broker alone, and it excludes the data feed from
the venue and the fan's network. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `scoring.py` | The scoring feed: one writer per partition, every ball of every live match, keyed by match id. Each ball carries the match's sequence and the resulting points of the players it touched, never a delta. Fantasy points are illustrative. |
| `points_node.py` | A points node: reads every partition with its own offsets, keeps every player's points and every team's total, checks gaps, recounts a sample of teams every second. Rebuilds every live match from the log on start. |
| `toss_surge.py` | Team saves and contest joins on their own topic, with a 4× surge after the toss, for the isolation test. |
| `kafka_bridge.py` | Reads your existing score or points topic as consumer group `iggy-shadow` and writes it into Iggy, for a shadow run on real matches. |
| `ci.sh` | The CI run: 60 matches at a ball every 0.5 s, two nodes, a toss surge next door, a node restart; pass only on 95% PASS seconds per node, zero gaps, zero totals off and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The toss surge:** `python toss_surge.py` while the rest runs: 7,000 team saves and contest
   joins a second, then 28,000 a second for 20 s. Pass: the points lane's p99 moves by less than
   10%.
2. **More nodes before the match:** start a third and a fourth `points_node.py`. Pass: the first
   node's p99 moves by less than 10%; every node reports the same ball count.
3. **A node added or restarted mid-match:** stop a `points_node.py` and start it again. Pass:
   `rebuilt from the log … points for 40 of 40 live matches … totals off 0` within seconds, and
   PASS from the next line. Compare with the `without the log` figure on the same line.
4. **Real time:** `python scoring.py --ball-s 35` and nodes with `--teams 1000000`. Pass: p99 holds
   and `totals off` stays 0 with a million teams on one node.

## Shadow your own matches

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> --topic '<your ball-by-ball score or points topic>'
python points_node.py --name shadow
```

The bridge is one more consumer group, `iggy-shadow`, so your producers and existing consumers are
untouched. Map your records in `to_ball()`: the kit expects, per ball, a match id, the over, the
event and the resulting points of the players it touched. The latency it prints then starts at the
bridge, not at the venue.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored score topic, alongside
Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
