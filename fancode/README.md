# Live scorecards on every node for FanCode, on LaserData

Every score update reaches every scorecard node in milliseconds, in order per match, so the next
`scorecardPoll` gets the new key wherever it lands, and a node added for a headline match or restarted
mid-match rebuilds every live scorecard from the log before it answers. It runs beside Kafka and Memorystore,
so the score source, the GraphQL API and everything else stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.fancode-kit
- **One slide:** [`LaserData_FanCode_LiveScorecard.pdf`](LaserData_FanCode_LiveScorecard.pdf)

## Why this use case

From FanCode's own posts and product:

- "Real-time data processing powers interactive features like live scores, match statistics, and player
  analytics." FanCode serves "over 160 million users" and supports "over 15,000 live events each year";
  "during live events, where large spikes in concurrent viewers require both high performance and
  reliability, this caching layer is especially crucial". Each microservice behind the gateway has its own
  database and Memorystore for Redis Cluster (Guruwinder Singh Rishi and Amit Mirchandani, Google Cloud blog,
  December 2024).
- Up to 6 million concurrent viewers in the West Indies tour of India, 2022 (AWS case study).
- FanCode's web app polls `scorecardPoll(matchId, lastKey)` every 5 s on a live match page, and every 10 s
  beside the commentary. The answer carries the match's `key`, `refresh` and the `delay` before the next poll;
  the client sends the `key` back as `lastKey` (fancode.com, October 2026).

That `key` is already a per-match sequence number. This kit runs one narrow lane: every score update of every
live match, in six sports, keyed by match, carrying that key and the resulting score (never a delta), to
every scorecard node, the service instances that build the scorecardPoll answer. Each node reads the lane
with its own offsets, so adding nodes before a headline match rebalances nothing, and a node that joins
mid-match replays the match day's log and holds every live scorecard before it serves. We have not seen how
score updates reach those services today; the kit assumes a Kafka topic for the shadow run, and that is the
first thing to check with your team.

## Run it

Needs Docker and Python 3.10+. Defaults: 40 live matches (cricket, kabaddi, football, tennis, basketball,
hockey), match time 15x real time so every second has updates to time (`--speed 1` for real time), and
6 million viewers spread over the matches by popularity, each polling every 5 s.

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python score_feed.py &                                # the score feed, every update of every live match
python scorecard_node.py --name scorecard-1 &         # a scorecard node
python scorecard_node.py --name scorecard-2           # and another: each reads every update
```

Each prints a line a second:

```
[scorecard-1]  110 updates/s  update-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)
               viewer polls answered behind the newest score … of 1,200,000/s (…%)  gaps 0  FC120001 key 154
[scorecard-2] rebuilt from the log in …s: scorecards for 40 of 40 live matches, … updates replayed;
               without the log 1,200,000 viewer polls a second would get no score until each match's next update
[score-feed]   110 updates/s  … writes  write ack p50 …ms  p99 …ms
```

`viewer polls answered behind the newest score` counts the polls that arrive after an update exists but
before this node holds it, so they get the older key: the viewers on that match divided by the poll interval,
times how late the update was. With match time compressed 15x (70x in CI) there are that many more updates,
so the figure is that many times higher than at real time.

**What the numbers mean.** The kit times each update from the moment the score feed created it (including its
wait for the next write) to the moment `scorecard_node.py` can answer a poll with it, through Python clients,
on whatever machine you run it on. On a shared 2-vCPU GitHub runner, with the broker, the feed, two scorecard
nodes and the app-events script all on one box, at about 110 updates a second across 60 matches, three green CI
runs held a median per-second p50 of 2.4 to 2.6 ms, p99 4.2 to 4.4 ms and p99.9 4.4 to 4.5 ms; every
steady-state second passed the 20 ms budget (63 of 63 and 48 of 48 in each run), and the worst second reached
4.7 to 10.5 ms. A 4x app-events surge next door (250 to 1,000 events a second at CI scale) left the median p99
at 4.2 to 4.4 ms. The restarted node rebuilt all 60 live matches from the log (4,247 to 4,288 updates) in 0.04
to 0.06 s. At CI's 70x match time, 5,500 to 5,800 of 1.2 million viewer polls a second (under 0.5%) were
answered behind the newest score. No key gaps. That is the end-to-end lane through this
demo code at reduced scale, not the broker alone, and it excludes the data provider's feed, the GraphQL edge,
the 5 s poll and the fan's network. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `score_feed.py` | The score feed: one writer per partition, every update of every live match, keyed by match id. Each update carries the match's key and the resulting score of both sides, never a delta. Scores are illustrative. |
| `scorecard_node.py` | A scorecard node: reads every partition with its own offsets, keeps every live match's key and score, answers `scorecardPoll`-shaped requests, checks key gaps. Rebuilds every live match from the log on start. |
| `app_events.py` | App events in data-highway-sized batches on their own topic, with a 4x surge at a headline match start, for the isolation test. |
| `kafka_bridge.py` | Reads your existing score topic as consumer group `iggy-shadow` and writes it into Iggy, for a shadow run on real matches. |
| `ci.sh` | The CI run: 60 matches at 70x, two nodes, an app-events surge next door, a node restart; pass only on 95% PASS seconds per node, zero key gaps and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **A headline match start:** `python app_events.py` while the rest runs: 5,000 app events a second, then
   20,000 a second for 20 s. Pass: the scorecard lane's p99 moves by less than 10%.
2. **More nodes before the match:** start a third and a fourth `scorecard_node.py`. Pass: the first node's
   p99 moves by less than 10%; every node reports the same update count and the same key for `FC120001`.
3. **A node added or restarted mid-match:** stop a `scorecard_node.py` and start it again. Pass:
   `rebuilt from the log … scorecards for 40 of 40 live matches` within a second, and PASS from the next
   line.
4. **Real time, more matches:** `python score_feed.py --speed 1 --matches 200` and nodes with
   `--matches 200`. Pass: p99 holds and `gaps` stays 0.

## Shadow your own matches

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> --topic '<your live score topic>'
python scorecard_node.py --name shadow
```

The bridge is one more consumer group, `iggy-shadow`, so your producers and existing consumers are untouched.
Map your records in `to_update()`: the kit expects, per update, a match id, the sport, the event and the
resulting score of both sides, plus the key if your records carry it. Compare the shadow node's key for a
match with what `scorecardPoll` returns. The latency it prints then starts at the bridge, not at the venue.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so producers and the bridge use the Iggy SDK; delivery is
at-least-once, which is why every update carries its key and the score so far; clustering with replication
is new (September 2026), so the evaluation includes a failover step.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored score topic, beside Kafka. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
