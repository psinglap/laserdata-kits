# Real-time team race progress for Scopely, on LaserData

Every dice roll in a MONOPOLY GO! team race reaches the one race owner that holds its race, and every lap, finish
and race end reaches every game server, in milliseconds and in order per race, counted once when a client
retries, even as load triples at the race start. It runs beside DynamoDB and ElastiCache, so nothing else has to
move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Scopely_RaceLane.pdf`](LaserData_Scopely_RaceLane.pdf)

## Why this use case

From Scopely's AWS re:Invent 2025 session on MONOPOLY GO! (DAT324, from the session transcript): daily peaks of
"over a million concurrent users", served by "as many as 2000 or more hosts at a time" running one .NET game
server; data that scales with users in DynamoDB, configuration in MySQL, "Redis for things like leaderboards and
temporary job state" (now ElastiCache Serverless). Mini games "always have a start and a stop time": when the race
mini game starts, the game forms teams, players "get the car together" and collect extras to make the car go
faster, and "we triple our baseline in a short period, like five minutes", with some tables going "from zero load
to maybe 100,000 read or write units per second". On a throttled write the server gives up fast and "the client
will retry a few times". From the AWS Database Blog post co-written with Brandon Cuff, Principal Engineer
(June 2024): "At peak traffic, 'MONOPOLY GO!' drove 2.1 million writes/second to DynamoDB", and DynamoDB Streams
with Lambda for derived views was rejected because "the Lambda updates significantly increased our write
throughput." From Scopely's Tycoon Racers announcement (June 2024): "form a team of 4", race "against three other
teams", "complete laps with your team" for rewards, medals after each race.

A race is shared state: 16 players, landing on any of thousands of hosts, move four cars on one track, and one roll
decides who finishes first. This kit runs that one lane on Apache Iggy: rolls keyed by race into the owner that
holds the race, and laps, finishes and race ends out to every game server. It prints both hops' latency, checks
every place against the race rules, and counts the business number: of the players whose team just finished (or
whose race just ended), how many rolled again before their game server had the news.

## Run it

Needs Docker and Python 3.10+.

```bash
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python race_owner.py &               # holds every race, fed by the race lane
python game_servers.py               # 16 game servers: rolls out, race updates in
```

Defaults: 20,000 rolls a second across 16 game servers, about 45,000 players in 2,800 races of 4 teams of 4,
arriving from zero over five minutes; a roll every 0.5 to 4 seconds per player, two dice times a multiplier per
roll, a 1,200-point track with a lap every 300 points, and 0.5% of rolls sent twice. Races are compressed from 25
hours to under a minute so finishes happen while you watch. All of these are illustrative; set them to yours.

| File | What it does |
|---|---|
| `game_servers.py` | Players and game servers: every roll to `race-rolls` keyed by race, every lap, finish and race end read back by each server; runs the race rules itself and checks every place |
| `race_owner.py` | Holds the races on its partitions (direct partition reads), decides laps, finishes and places, publishes them to `race-updates` |
| `common.py` | The race rules (`Race.apply`), shared by the owner and the check, plus topics and partitioning |
| `analytics_flood.py` | Client analytics at a set MB/s on their own topic, the heavy neighbour |
| `dynamodb_bridge.py` | Shadow your race table's DynamoDB stream into the lane, read-only |
| `bench/PLAN.md` | The benchmark against Kafka, rules fixed first |

## What CI measured

On a real Apache Iggy 0.9.0 broker on a shared 2-vCPU GitHub runner, with the broker and every script on one box, at
reduced scale (6,752 players in 422 races arriving over 15 s, about 2,750 rolls a second at full rate on 16
partitions, two race owners with 8 partitions each, 4 game servers, a 600-point track, 0.5% of rolls sent twice,
2 MB/s of client analytics next door from second 30), run 39708381177:

| | Result |
|---|---|
| Roll to race owner | p50 8.2–8.4 ms, p99 15.5–15.8 ms, p99.9 18.2–19.1 ms (medians of per-second values); worst second p99 17.6–18.5 ms; 140 of 140 steady-state seconds inside the 50 ms budget |
| Roll to every game server (both hops) | p50 13.2 ms, p99 21.8 ms |
| Analytics next door | owner p99 15.3–15.7 ms before, 15.6–15.8 ms with 2 MB/s on the same broker |
| Places | 5,063 finishes; every place the owners announced matched the race rules run on the rolls in send order |
| Retries | 885 retried rolls counted once |
| Decided races | Of 35,413 first taps after a team finished or a race ended, 145 (0.41%) rolled before that game server had the news; with a race view refreshed 2 s behind it would have been 24,676 (69.7%) |

What the numbers mean: each roll is timed from creation in `game_servers.py` to the race owner, and each lap,
finish and race end from the roll that caused it to every game server, through Python clients on a busy shared
runner. The broker's own tail latency is measured separately with `iggy-bench` on dedicated machines
(`bench/PLAN.md`). Your roll gaps, track lengths and race view refresh decide the real business number, which is
why the evaluation runs on your traffic.

## Three tests

1. **The race starts.** Everyone arrives in one minute at twice the rate, two owners:
   `python race_owner.py --worker 0 --workers 2 & python race_owner.py --worker 1 --workers 2 &`
   `python game_servers.py --rate 40000 --ramp-s 60`. Pass: roll-to-race-owner p99 under 50 ms from the first
   second, no place checked wrong.
2. **Analytics next door.** `python analytics_flood.py --mbps 50`. Pass: the race lane's p99 moves by less than 10%.
3. **Restart an owner mid-race.** Stop `race_owner.py` and start it again. It replays its partitions and rebuilds
   every race it owns; the laps and finishes it re-announces carry the same update numbers, so the game servers
   ignore them. Pass: it catches up within seconds and no place is checked wrong.

## Shadow your own traffic

`dynamodb_bridge.py` reads the DynamoDB stream of the table that holds race progress (enable Streams with
NEW_IMAGE on that table; no change to the game server) shard by shard from LATEST, and writes each changed item,
keyed by race, into Apache Iggy. It never writes to DynamoDB: no Lambda and no derived table, so the table's write
units do not grow. A stream shard serves at most two readers without throttling, so on a stream that already has
two, run it on a copy or for a short window. Map your item in `to_roll()`.

```bash
pip install boto3
python dynamodb_bridge.py --stream-arn arn:aws:dynamodb:us-east-1:...:table/<race-table>/stream/... --region us-east-1
```

## What Apache Iggy 0.9 doesn't have yet

- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9. The race lane doesn't need it: the game
  server would publish with the official C# SDK.
- **Delivery:** at-least-once. Rolls carry the player's race total and sequence, and updates carry their number,
  which is why a retry or a replay changes nothing.
- **Clustering:** Viewstamped Replication clusters shipped in September 2026; they are new.
- **Storage:** local NVMe per node, not object storage. This is one lane, not the analytics history.

## Next step

Run the kit this week. If the numbers are interesting, we set up a 2–4 week evaluation on one mirrored race table
stream, alongside DynamoDB and ElastiCache. No advantage, no migration. kranti@laserdata.com · laserdata.com
