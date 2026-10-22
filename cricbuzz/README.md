# Ball-by-ball commentary for Cricbuzz, on LaserData

Every commentary line and every correction reaches every commentary node and match alerts in milliseconds, in
order per match, so a wicket overturned on review is corrected on every node, never flips back, and its alert is
taken back; a node added before a big match, or match alerts restarted mid-innings, is current from the log within
a second and sends nothing twice. It runs beside your commentary services, so the desk's tools, the apps and push
stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Cricbuzz_Commentary.pdf`](LaserData_Cricbuzz_Commentary.pdf)

## Why this use case

From Cricbuzz's own posts and product:

- A Cricbuzz.com backend role in Bengaluru: "Develop and maintain backend systems using Go, Java, and Scala."
  "Enhance and optimize high-speed ball-by-ball commentary services." (Cricbuzz post, November 2024, as listed
  on Airtribe.)
- The App Store listing leads with "super-fast scores and immersive ball-by-ball commentary" and "live match
  alerts and breaking news" (version 27.03, October 2026).
- IPL 2025: 25 million daily active users, 110 billion impressions, 12 billion minutes (Cricbuzz's numbers);
  53 million unique visitors in Comscore's Sports Highlights on Digital 2025 (indiantelevision.com, November 2025).

This kit runs one narrow lane: every commentary line of every live match, and every correction to a line already
published, keyed by match, with a per-match sequence and a per-line version, to every commentary node and to match
alerts. Each reader has its own position, so adding nodes before an India match rebalances nothing, and a reader
that joins mid-match replays the match day's log before it serves. Cricbuzz has not published how commentary moves
between its services today, or whether a broker is in that path; the kit assumes neither, and that is the first
thing to check with your team. How the desk corrects a line (the same line id with a higher version) is our
assumption.

## Run it

Needs Docker and Python 3.10+. Defaults: 30 live matches (T20, ODI and Test), ball by ball, match time 20x real
time so every second has lines to time (`--speed 1` for real time).

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python commentary_desk.py &                           # the desk: every line and correction, every live match
python commentary_node.py --name node-1 &             # a commentary node
python commentary_node.py --name node-2 &             # and another: each reads every line
python match_alerts.py                                # wicket and milestone alerts from the same lane
```

Each prints a line a second:

```
[node-1]   17 lines/s  line-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)  corrections live 1 (overturned 1),
           slowest +…ms  old lines held 0  gaps 0  CB110001 151/4
[node-2] rebuilt from the log in …s: commentary for 30 of 30 live matches, … lines and corrections replayed
[alerts]   1 alerts/s  ball-to-alert p50 …ms  p99 …ms  PASS (<50ms)  wickets 1  milestones 0
           taken back after review 0  sent late 0
[desk]     17 lines/s  corrections 1  … writes  write ack p50 …ms  p99 …ms
```

`corrections live` counts the corrections that reached this node that second, and `slowest` is how long a fan on
that match could still read the old line after the desk corrected it. `old lines held` counts corrections that
would have put an older version back, and must stay 0. `taken back after review` counts wicket alerts withdrawn
when the desk overturned the wicket.

**What the numbers mean.** The kit times each line from the moment the desk created it (including its wait for the
next write) to the moment `commentary_node.py` holds it, through Python clients, on whatever machine runs it. On a
shared 2-vCPU GitHub runner, with the broker, the desk, two commentary nodes, match alerts and the impressions
script on one box, at about 100 lines a second across 60 matches, three green CI runs on the final code held a
median per-second line-to-node p50 of 2.3 to 2.9 ms, p99 4.1 to 5.0 ms and p99.9 4.2 to 5.2 ms. The node that ran
throughout passed the 20 ms budget in 62 of 63 steady-state seconds in every run; the one second over (p99 22 to
41 ms) was the second in which the other node was stopped and restarted on the same two vCPUs. The other node
passed 47 to 48 of its 48. A 4x impressions surge next door left the median p99 at 4.0 to 5.1 ms. The restarted
node rebuilt all 60 live matches from the log (3,904 to 3,913 lines and corrections) in 0.05 to 0.08 s. Each run
carried 96 to 97 corrections to the node, the slowest in 4.4 to 5.2 ms, with no line held at an old version and no
gaps. Match alerts, polling every 5 ms against a 50 ms budget, decided 324 to 325 wicket alerts at a median
per-second ball-to-alert p99 of 7.0 to 8.9 ms (worst second 9.2 to 11.4 ms) and took back 60 to 61 after review;
after its restart it read 301 to 305 alerts from its record, and the record held 524 to 531 alerts with none
recorded twice. That is the end-to-end lane through this demo code at reduced scale, not the broker alone, and it
excludes the desk's tools, your services, the app's refresh, push delivery and the fan's network. The broker's own
tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `commentary_desk.py` | The desk: one writer per partition, every line of every live match keyed by match id, with wickets overturned on review and boundaries re-scored as leg byes. Each message carries the match's sequence, the line id and version, and the resulting score. Matches and text are illustrative. |
| `commentary_node.py` | A commentary node: reads every partition with its own position, holds each live match's recent lines by version and the score, checks gaps and old versions. Rebuilds every live match from the log on start. |
| `match_alerts.py` | Match alerts: wicket and every-50-runs alerts from the same lane, taken back when a wicket is overturned, recorded on `alerts-sent` before push so a restart sends nothing twice. `--audit` checks the record. |
| `impressions.py` | Ad impressions on their own topic, with a 4x innings-break surge, for the isolation test. |
| `commentary_bridge.py` | Polls the commentary JSON your own service answers for each live match and writes new and changed lines into Iggy, read-only, for a shadow run on real matches. |
| `ci.sh` | The CI run: 60 matches at 60x, two nodes and match alerts, an impressions surge next door, a node and match alerts restarted; pass only on 95% PASS seconds per reader, zero gaps and old lines, a full rebuild, overturned wickets taken back and no alert recorded twice. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The innings break:** `python impressions.py` while the rest runs: 5,000 impressions a second, then 20,000 a
   second for 20 s. Pass: the commentary lane's p99 moves by less than 10%.
2. **More nodes before an India match:** start a third and a fourth `commentary_node.py`. Pass: the first node's
   p99 and the alerts' p99 move by less than 10%; every node shows the same score for `CB110001`.
3. **A restart mid-innings:** stop `commentary_node.py` and `match_alerts.py` and start them again, then
   `python match_alerts.py --audit`. Pass: `commentary for 30 of 30 live matches` within a second, PASS from the
   next line, `old lines held 0`, and `recorded twice 0`.
4. **Real time, a full calendar:** `python commentary_desk.py --speed 1 --matches 120` and nodes with
   `--matches 120`. Pass: p99 holds; `gaps` and `old lines held` stay 0.

## Shadow your own matches

```
python commentary_bridge.py --source 'http://<commentary-service>/v1/{match}' --matches <live match ids>
python commentary_node.py --name shadow
python match_alerts.py --name shadow-alerts
```

The bridge only reads what your commentary service already answers, so nothing upstream changes. Map your response
in `to_lines()`: the kit expects, per line, a line id, over.ball, the event, the text and the resulting score.
Compare the shadow node's feed and the shadow alerts with your apps. The latency they print then starts at the
bridge, not at the desk. If your desk already writes to a Kafka topic, read that topic as one more consumer group
instead.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so the desk and the bridge use the Iggy SDK (Go, Java, Rust,
Python, C# and Node.js); delivery is at-least-once, which is why every message carries the match's sequence and
the line's version, and why match alerts records an alert before it goes out (a crash in the few milliseconds
between the record and push can miss that alert); clustering with replication is new (September 2026), so the
evaluation includes a failover step.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored commentary feed, beside your services. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
