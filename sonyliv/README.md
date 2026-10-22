# Real-time KBC Play Along scoring for Sony LIV, on LaserData

Every locked answer reaches its player's score in milliseconds, on time and counted once, before the answer is
revealed on TV, even at a 1.6 million-player peak. It runs beside Kinesis, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_SonyLIV_KBCPlayAlong.pdf`](LaserData_SonyLIV_KBCPlayAlong.pdf)

## Why this use case

From Sony LIV's own sources:

- The interactive KBC game, where viewers answer from their phones in real time, reached a peak of "1.6 million
  simultaneous users", with "sub-100 millisecond latency without any issues". "Latency is as bad as downtime for
  OTT applications." (Manish Verma, Head of Technology, SonyLIV Technology, AWS case study; it describes the years
  after the 2020 relaunch.)
- The KBC 2025 Play Along FAQ: players select an answer and lock it; the time "varies for each question", and "if
  the next question has been asked on the TV show, the timer of the current question will not extend beyond that
  time"; "Answers will be revealed on TV and App simultaneously"; "200 additional points for answering the
  questions within 10 seconds"; scores "are computed and checked at the backend" and decide daily, episode and
  season prizes.
- KBC 18 streams on Sony LIV from 10 August 2026, 120 episodes, Monday to Friday at 9 PM, with Play Along as "a
  separate interactive layer" (MediaBrief, Aug 2026).
- Priority events "requiring immediate action" (playback failures, user interactions, live sports engagement
  signals) go from EKS through Kinesis Data Streams to ClickHouse; high-volume events through Firehose to S3, EMR
  and Iceberg (AWS Big Data Blog, Sep 2026, co-written with Mukund Acharya, CTO). "Traffic patterns can spike
  several multiples within minutes after a wicket, goal, or major match event" (Vaibhav Gupta, VP of Platform
  Engineering, ClickHouse blog, Aug 2026).

Kinesis and ClickHouse are the right home for analytics, QoE and dashboards, and this kit does not touch them.
Play Along scoring is a narrower job with game rules attached: each lock judged against a close the TV show can
move, counted once when the app retries, scored before a reveal that happens on TV and in the app at the same
moment, and kept so the backend can check and rebuild prize-deciding scores. Because a right answer inside 10
seconds earns 200 more points, most of a question's locks arrive in its first few seconds: at 1.6 million players,
on the order of 100,000 locks a second. This kit runs locks on their own lane, keyed by player, read by the scorer
directly, and counts the outcome: valid locks scored after their reveal.

Sony LIV does not publish which system scores Play Along today, whether locks travel on the Kinesis priority
stream, what the case study's sub-100 ms measures, the per-question timers, or the gap from close to reveal. The
lane is a proposal; the kit's flags let you set those to yours. The kit assumes the 10-second bonus applies to
right answers only.

## Run it

Needs Docker and Python 3.10+. Defaults: 100,000 players, 80% locking an answer on each question, 70% of locks
inside the 10-second bonus window, an illustrative timer per level (30 to 60 s), 15% of questions cut short when
the next one is asked, 1% of locks arriving after the close, 1% sent twice, and the reveal 4 s after the close.
These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python scorer.py &                     # the Play Along scorer
python episode.py &                    # the show desk, and players locking answers
python engagement_flood.py --at 20     # a heavy engagement stream next door
```

Each prints a line a second, and the scorer one at each reveal:

```
[episode]   6,600 locks/s  E061-Q06 open 4.2 s of 45 s  late …  repeats …
[scorer]    6,600 locks/s  lock-to-score p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  players scored …
[play-along] scored …  repeats dropped …  second locks dropped …  scored after the reveal 0
[reveal] E061-Q06 level 6 (500 pts)  locks scored …  right …%  10 s bonus …  late rejected …  scored after the reveal 0  top episode score …
[digest] E061-Q06  locks …  points …  late …  digest 1a2b3c4d
```

`scored after the reveal` is the business number. Each lock carries the time the answer API received it and a
sequence that only grows per player. `scorer.py` drops repeats and second locks, rejects a lock received after the
question's close (taking back points it scored before it read the close), adds the bonus for a right answer inside
10 seconds, and counts every valid lock it scored after the reveal went out, when the player had already seen the
answer without their points. About 5 s after each reveal it prints a digest of that question's scores per player;
a scorer started later rebuilds the same scores from the log and prints the same digests.

What-if: `python scorer.py --hold-ms 5000` scores every lock only once it is 5 s old, as a path measured in seconds
would, so you can see how many locks land after their reveal.

**What the numbers mean.** The kit times each lock from the moment `episode.py` sends it to the moment `scorer.py`
has scored it, through Python clients, on whatever machine you run it on. On a shared GitHub runner (2 vCPU), with
the broker and four Python processes on one box, at 30,000 players, 80% locking on each 16-second question, about
half the questions cut short and a 1 MB/s engagement stream next door, three green CI runs held p50 4.6 to 7.0 ms,
p99 8.5 to 13.6 ms and p99.9 9.6 to 15.0 ms (median per second over 50 to 51 steady-state seconds each, no second
over the 100 ms budget; the worst second's p99 was 49 to 53 ms). In the bonus-window bursts (above 1,500 locks a
second) the median per-second p99 was 9.2 to 15.2 ms. Each run scored 86,000 to 90,000 locks over four questions
with none scored after its reveal, rejected 1,040 to 1,088 late locks and dropped 848 to 909 repeats, and a second
scorer started mid-episode matched the live scorer's digests on all four questions. That is the end-to-end lane
through this demo code at reduced scale, not the broker alone; the broker's own tail latency is measured with
`iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `episode.py` | The show desk (open, close, reveal on `show-desk`, one partition) and the answer API (every lock on `answer-locks`, keyed by player, stamped on receipt, sequence only grows per player). `--players`, `--answer-share`, `--fast-share`, `--timer-s`, `--cut-share`, `--late-share`, `--repeat-share` and `--reveal-gap-s` shape the episode. |
| `scorer.py` | The Play Along scorer: on time, once, bonus, episode totals; times lock-to-score, counts locks scored after the reveal, prints per-question digests. `--hold-ms` is the what-if; `--name rebuild` runs a second scorer from the log. |
| `engagement_flood.py` | A bulk, latency-tolerant playback and engagement stream on its own topic on the same broker. |
| `kinesis_bridge.py` | Reads Play Along locks (and the question timeline, if present) from a Kinesis stream with GetRecords, read-only, and publishes them to the lane, keyed by player, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The bonus-window burst:** `python episode.py --players 200000 --fast-share 0.8 --timer-s 30`.
   Pass: lock-to-score p99 under 100 ms through every burst, and no valid lock scored after its reveal.
2. **Engagement traffic on the same broker:** `python engagement_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **A second scorer mid-episode:** `python scorer.py --name rebuild` while the episode runs, or restart the
   scorer. It rebuilds every player's score from the log, prints "catching up", then scores live locks again.
   Pass: its digests match the live scorer's, and it is back under budget within seconds.

## Shadow your own traffic

```
pip install boto3
python kinesis_bridge.py --stream <your-priority-events-stream> --region ap-south-1
```

The bridge only reads; the apps, the EKS ingest services, ClickHouse and every existing consumer are untouched.
Each shard allows five GetRecords calls a second shared by every polling consumer, so run it on a copy of the
stream or for a short window, or register it as an enhanced fan-out consumer. Map your field names in
`to_event()`: a lock needs the player ID, the question ID, the option and the time your answer API received it.
If the question timeline is not on that stream, publish it to the `show-desk` topic in the format `episode.py`
uses. In shadow mode the clock starts when the bridge reads the record. Everything runs in your AWS account.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored Play Along locks during KBC 18 episodes,
beside Kinesis. No advantage, no migration.

kranti@laserdata.com · laserdata.com
