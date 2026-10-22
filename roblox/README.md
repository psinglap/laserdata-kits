# Real-time game server state for Roblox matchmaking, on LaserData

Every join, leave, new server and shutdown reaches every matchmaker's index in milliseconds, so players land on
servers with room, even in a Saturday thundering herd. It runs beside matchmaking and Kafka, so nothing else has
to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.roblox-kit
- **One slide:** [`LaserData_Roblox_ServerState.pdf`](LaserData_Roblox_ServerState.pdf)

## Why this use case

From Roblox's own posts and docs:

- Matchmaking looks at "thousands of data points from multiple Roblox engine instances". "Every Roblox engine
  instance is constantly pushing data into this system. Any experience join request scans the properties of every
  active place, ranks them across multiple indexes", and the system has "to track and refresh the state of all
  Roblox experiences every few seconds" (Roblox newsroom, "Inside the Tech: Solving for Matchmaking on Roblox",
  October 2023).
- Thundering herds are routine: Saturday updates and limited releases bring "a quick 10x jump in requests", and
  more than 21 million players have joined one experience at once. 24 edge data centers run the game servers;
  matchmaking evaluates up to 4 billion join combinations a second at peak, toward 10 million joins in 10 seconds
  (Roblox newsroom, "The Infrastructure Supporting Record-Breaking Experiences", June 2025).
- Matchmaking drops servers "that are full, private, reserved, or about to be shut down", then scores the rest
  with weighted signals such as Latency and Occupancy (Roblox Creator Hub, Matchmaking docs).

The two fields that decide eligibility, full and about to be shut down, change fastest in a herd: friends follow
friends into servers matchmaking did not pick, creators publish updates that close a place's servers, and new
servers start that nobody can be sent to until the index has them. This kit runs that one feed as a lane on
Apache Iggy: every server's state, keyed by server, read by every matchmaker directly, kept for an hour so a new
or restarted matchmaker rebuilds its index from the log. It prints the lane's p99 every second, joins sent to a
full or closing server, and what an index refreshed every 3 seconds would have done with the same joins.

## Run it

Needs Docker and Python 3.10+. Defaults: 10,000 game servers of 500 places in 24 edge data centers, 400 players
pressing Play a second, 150 friend joins and 550 leaves a second, and a heartbeat from every server every 10 s.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python fleet.py &                    # game servers, players pressing Play, and the check
python matchmaker.py --name mm-1 --shard 0 --shards 2 &   # a matchmaker: half the places' joins
python matchmaker.py --name mm-2 --shard 1 --shards 2     # the other half; each holds every server
```

They print a line a second:

```
[mm-1]  2,100 changes/s  change-to-index p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  joins matched …  no server 0
[fleet] 2,100 changes/s  10,000 servers  placed … (steady)  bounced … (full … shutting down …) no server …
        an index refreshed every 3s would bounce … (full … shutting down …) no server …
```

**What the numbers mean.** The kit times each change from the moment `fleet.py` creates it (including its wait
for the next write) to the moment a `matchmaker.py` has applied it to its index, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box
(about 1,500 servers of 60 places in 6 edge data centers, 100 players pressing Play a second, a creator update
with a 10× herd for 15 s, 2 MB/s of analytics events next door, two matchmakers, one restarted), the medians of
the steady-state seconds over two green runs on the final code were p50 5.3–6.7 ms, p99 13.9–16.3 ms and p99.9
16.3–19.5 ms on each matchmaker, at about 690 server changes a second (about 1,600 in the herd). Every
steady-state second passed the 50 ms budget (65 of 65 and 53 of 53 per matchmaker in both runs); the worst second
reached 28.8–48.1 ms. The restarted matchmaker rebuilt its index from the log (1,813–1,821 servers, about 19,600
changes) in 0.10–0.18 s before it took a join. Of about 19,900 placements checked against the servers, 21
(0.11%) bounced, 18–19 of them at the moment of the update, sent to servers closing within milliseconds of the
pick. The same matchmakers' index copies refreshed every 3 s would have bounced 3,953–4,117 joins (19.9–20.8%)
and found no open server for 9,651–9,746 (48.5–49.2%), because the servers started for the herd filled before the
next refresh. Press-to-placement was p50 7.3–8.1 ms and p99 12.6–14.8 ms. That is the end-to-end lane through this
demo code at reduced scale, not the broker alone, and the comparison is the kit's model, not a Roblox figure. One
Python reader tops out near 4,000 changes/s on that runner; past it, the reader falls behind, not the broker. The
broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `fleet.py` | The game servers and the players. Publishes each server's state to `server-state` with a per-server sequence and the resulting state (players, capacity, open or closing, placements seen); sends join requests keyed by place; starts servers where a place runs short of slots; checks every placement against the server at that moment. `--herd` runs a creator update and a thundering herd. |
| `matchmaker.py` | A matchmaker: reads every partition of the lane directly into its index, rebuilds from the log on start, takes the join requests of its share of places (`--shard` of `--shards`), filters and scores servers like the default configuration (Latency, Occupancy), and keeps a copy of its index refreshed every 3 s for comparison. |
| `analytics_flood.py` | A heavy, latency-tolerant stream (analytics events) on its own topic, for the isolation test. |
| `qaas_bridge.py` | Copies a Kafka-protocol topic that carries your engine instances' state reports into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `ci.sh` | The CI run: about 1,500 servers, a creator update with a 10× herd, an analytics neighbour, two matchmakers and a restart. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The fleet is a model, not Roblox data: server capacities, popularity, friend joins (players who follow a friend
into a server, which matchmaking does not pick) and the 3-second comparison index are the kit's assumptions.
Roblox says its index refreshes "every few seconds"; set `--refresh-s` on `matchmaker.py` to what you run.

## Three tests

1. **A creator update and its thundering herd:** `python fleet.py --joins 150 --direct 60 --leaves 210 --herd 10 --herd-at 60 --herd-for 30`.
   Pass: change-to-index p99 under 50 ms on every matchmaker, and joins sent to a full or closing server near zero
   through the herd.
2. **Analytics at full volume next door:** `python analytics_flood.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **A new cell's matchmaker:** stop one `matchmaker.py` and start it again with the same `--shard`; it rebuilds
   its index from the log before it takes a join. Pass: every live server in its index within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python qaas_bridge.py --bootstrap <brokers> --topic <server-state reports> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; engine instances, the
matchmaking index and every other consumer are untouched. Map your record shape in `to_state()`: the kit needs
`k` (server), `seq`, `place`, `dc`, `cap`, `n` (players) and `st`. If the state reports reach the index another
way than a Kafka-protocol topic (the public posts don't say), the evaluation starts with a tap on that path.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; matchmakers use the
Apache Iggy SDK. Delivery is at-least-once; the per-server sequence makes replays and retries safe. Replicated
clusters shipped in September 2026, which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored server-state feed, beside the matchmaking
index. No advantage, no migration.

kranti@laserdata.com · laserdata.com
