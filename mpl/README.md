# Real-time player form for MPL matchmaking, on LaserData

Every finished game reaches every matchmaker in milliseconds, so a player on a losing streak gets an easier match
on the next tap of Play again, even at a live-match peak. It runs beside Confluent Cloud, so nothing else has to
move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.mpl-kit
- **One slide:** [`LaserData_MPL_PlayerForm.pdf`](LaserData_MPL_PlayerForm.pdf)

## Why this use case

From MPL's own accounts of its platform:

- Batch-era matchmaking hurt: "Players need to be assigned the right level of play, matched with other similarly
  skilled players to ensure that they win as much as they lose. Lack of real-time data in matchmaking caused some
  players to experience losing streaks." Today MPL streams "skill levels, scores, win rates" into Confluent Cloud
  "to detect when players with a streak of losses are approaching the point of churn", sending offers "while
  adjusting matchmaking to give them a higher probability of winning the next game" (Confluent blog, "How Mobile
  Premier League Reduced Player Churn with Confluent Cloud", June 2023).
- Scale: "200 million user events/day with 600-700 million events/day at peak during live sporting events", over
  a billion marketing events a day, "hundreds of millions of tournaments every month" (same post).
- The ML platform on Kafka and Flink, with a feature store on BigTable and Redis, runs "over 300 million
  inferences per day, with p99 turnaround times under 10 seconds", guiding "matchmaking, mission assignment, and
  fraud alerts" (Current India 2025 talk by Mahesh Jadhav and Lakhan Marda, as summarised by Kai Waehner, June
  2025).

Ten seconds is fine for a mission or an offer. A matchmaking decision is made when the player taps Play again,
a few seconds after the last result. If the matchmaker's view of the player does not have that result yet, a
player on a third loss in a row is matched at full strength, and a player who just broke the streak still gets
an easy opponent. This kit runs that one feed as a lane on Apache Iggy: every finished game, as the player's
resulting form, keyed by player, read by every matchmaker directly, kept for a day so a new or restarted
matchmaker rebuilds every player's form from the log. It prints the lane's p99 every second, decisions made on a
stale form, losing streaks missed and protection given after a win, and what a view 10 seconds behind would have
decided for the same taps.

## Run it

Needs Docker and Python 3.10+. Defaults: 100,000 players online in 16 lobbies, games of 30 to 90 seconds, Play
again 2 to 8 seconds after each result (about 1,500 results a second), streak protection after 3 losses in a
row, an opponent 150 rating points easier.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python players.py &                  # players, games, results, taps on Play again, and the check
python matchmaker.py --name mm-1 --shard 0 --shards 2 &   # a matchmaker: half the lobbies' requests
python matchmaker.py --name mm-2 --shard 1 --shards 2     # the other half; each holds every player's form
```

They print a line a second:

```
[mm-1]  1,500 results/s  result-to-matchmaker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decided …  streak protected …
[players] 1,500 results/s  100,000 players  taps … matched … (steady)  on a losing streak …
          live view: stale form 0 streak missed 0 protected after a win 0
          a view 10s behind: stale form … streak missed … protected after a win …
```

**What the numbers mean.** The kit times each result from the moment `players.py` creates it to the moment a
`matchmaker.py` has applied it to its view, through Python clients, on whatever machine you run it on. Over three
green CI runs on a shared 2-vCPU GitHub runner, with the broker and all four scripts on one box (5,000 players,
3-6 s games, Play again 1-4 s after a result, about 730 results a second, a live-event surge to 12,500 players
and about 1,600 taps a second, 2 MB/s of app events next door, two matchmakers and one of them restarted), the
steady-state medians per matchmaker were p50 9.6 to 10.8 ms, p99 18.4 to 21.2 ms and p99.9 20.6 to 23.6 ms.
111 to 112 of 112 steady-state seconds per run stayed inside the 50 ms budget; the worst second reached 51.9 ms.
The restarted matchmaker rebuilt the form of 5,000 players from about 19,600 results in 0.22 to 0.23 s.
Tap-to-match was p50 13.7 to 13.9 ms and p99 21.8 to 22.3 ms. One Python reader tops out near 4,000 results a
second on that runner; past it, the reader falls behind, not the broker. This is the end-to-end lane through demo
code at reduced scale, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

**The business number.** In those runs the matchmakers fed by the lane decided about 59,100 to 59,900 taps each
run, 4,340 to 4,441 of them from players on a losing streak, with 0 decisions on a stale form, 0 losing streaks
missed and 0 protections after a win. The same matchmakers' views 10 seconds behind, judged against the same
true form at the same taps, missed 79.9 to 81.3% of the losing streaks (69.2 to 71.0% in the surge) and protected
2,635 to 2,726 players who had just won. The players, game lengths and the time to Play again are the kit's model,
not MPL data, so read the gap as the shape of the problem, not its size at MPL.

| File | What it is |
|---|---|
| `players.py` | Players, Elo-rated games, results published to `player-form` keyed by player with a per-player sequence and the resulting rating and streak, taps on Play again to `match-requests`, and the check of every decision. |
| `matchmaker.py` | Reads every partition of `player-form` directly, rebuilds every player's form from the log on start, decides each request (rating, streak protection), and records what a view 10 s behind would have decided. |
| `app_events.py` | A heavy neighbour: app and marketing events on their own topic. |
| `confluent_bridge.py` | Copies an existing Confluent Cloud topic of game results into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A live-match peak:** `python players.py --players 60000 --surge 3 --surge-at 60 --surge-for 120` (3× the
   players online, arriving over 5 s; about 2,800 results a second, inside one Python reader's reach). Pass: result-to-matchmaker p99 under 50 ms on every matchmaker, and losing streaks missed
   stay at zero.
2. **App events at full volume next door:** `python app_events.py --mbps 50` while both run.
   Pass: the player form lane's p99 moves by less than 10%.
3. **Rebuild a matchmaker:** stop one `matchmaker.py` and start it again with the same `--shard`. It replays the
   lane and holds every player's form before it takes a request. Pass: within seconds, and its first decisions
   are no more often stale than the other matchmaker's.

## Shadow your own traffic

```
pip install confluent-kafka
python confluent_bridge.py --bootstrap <bootstrap-server> --topic <game-results-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
```

The bridge only reads, as its own consumer group (`iggy-shadow`), and never commits offsets; game servers, Flink
jobs, the feature store and existing consumers are untouched. Map your record shape in `to_form()`: the kit needs
`k` (player), `seq`, `r` (rating), `streak` and `games`. If your topic carries raw outcomes, compute the form per
player in `to_form()`.

## What Apache Iggy 0.9 does not do yet

No Kafka API (a gateway is in progress), so matchmakers read the lane with the Apache Iggy SDK and the trial uses
the bridge. Replication clusters shipped in September 2026, newer than Kafka's. Delivery is at-least-once; the
per-player sequence makes repeats harmless.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored game-results topic, alongside Confluent
Cloud. No advantage, no migration.

kranti@laserdata.com · laserdata.com
