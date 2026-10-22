# Real-time Autoplay queues for Pocket FM listeners, on LaserData

Every play, skip and unlock reaches the listener's Autoplay queue in milliseconds, so the next show is
never one they just skipped. It runs beside your Kafka and Redis, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.pocket-fm-kit
- **One slide:** [`LaserData_PocketFM_Autoplay.pdf`](LaserData_PocketFM_Autoplay.pdf)

## Why this use case

From Pocket FM's own sources:

- Autoplay picks the next show from listening behaviour: "Using signals from users' in-app behavior, such
  as what they listen to, skip, or complete, we tailor the next show specifically to users' interests."
  The playlist is fetched "from our Redis cache", is "a dynamic, evolving queue that updates daily", and
  "once the playlist is generated, it's routed through Apache Kafka and pushed back to Redis". The hybrid
  model (a customised EASE plus a tree-based ensemble) lifted playlist playtime 40% in online tests
  (Pocket Xtra, Jun 2025).
- "Autoplay is Pocket FM's end-of-content queue: when a listener finishes a show or runs out of unlocked
  episodes, the next one simply starts." Listeners unlock episodes with ads, coins or a subscription, and
  the post charts Autoplay revenue since launch (Pocket Xtra, Apr 2026, co-written by CTO Umesh Bude).
- Pocket FM's Go services share a library with "Kafka integrations" and Redis and MySQL abstractions
  (Pocket Xtra, May 2026), on multiple Amazon EKS clusters, with services on AWS and GCP (Pocket Xtra,
  Apr 2026).
- 250M+ listeners in 20+ countries; about $415M of a $500M annualised run rate comes from listeners paying
  to unlock episodes, and the US is about 70% (TechCrunch, Sep 2026).

Your Kafka, the daily playlist build and Redis are the right home for the model's ranking, and this kit
does not touch them. Applying what a listener just did to that queue is a narrower job with a tighter
clock: a listener lets Autoplay start a pick, skips it twenty seconds in, and the player asks for the next
show. If the skip is still waiting in a batch, the queue offers the same show again. This kit runs
listening actions on their own lane, keyed by listener, read by the Autoplay queue service directly, and
counts the outcome: Autoplay picks made without the listener's last action.

Pocket FM does not publish whether the Autoplay API already applies in-session skips before serving, its
event rates, or the gap from an action to the next end-of-content request. The lane is a proposal; the
kit's flags let you set those to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 listeners in session, 20,000 listening actions a
second (play, episode complete, unlock with coins, an ad or a subscription, skip, show complete), 60% of
plays starting the Autoplay pick, half of the actions followed by an end-of-content request 0.3 to 2 s
later, and a 2× new-episode surge for the first minute. These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python autoplay.py &                   # the Autoplay queue service, answering every end-of-content request
python listeners.py &                  # listeners playing, skipping, unlocking and finishing shows
python analytics.py --at 20            # heavy listening analytics next door
```

Each prints a line a second:

```
[listeners]  40,000 listening actions/s (surge)  play … (Autoplay picks …)  episode complete …  unlock …  skip …  show complete …  end-of-content requests …
[autoplay]  40,000 actions/s  action-to-queue p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  listeners in queue …
[next-show] picks …  next show changed …  picked without the last action …%  wrong show started …
```

`picked without the last action` is the business number. Each action carries its type, the show, the
listener's session and a sequence that only grows per user. The queue service starts each listener from
today's queue (here a fixed function of the user, standing in for the playlist your model writes to
Redis) and applies the session on top, the same way in every script (`common.fold()`): a skipped pick or
a finished show leaves the queue, and the show playing now is never offered next. Each end-of-content
request carries the sequence of the listener's last action and the show Autoplay should start after it.
`autoplay.py` asks whether it had applied that action when the request started. If not, the pick was made
from a stale queue; if the stale queue leads with another show, Autoplay started a show the listener just
skipped or finished, or the one already playing (`wrong show started`).

What-if: `python autoplay.py --hold-ms 1000` applies every action only once it is 1 s old, as a
one-second batch window would, so you can see how many picks such a path makes from a stale queue.

**What the numbers mean.** The kit times each action from the moment `listeners.py` sends it to the
moment `autoplay.py` has applied it to the queue, through Python clients, on whatever machine you run it
on. On a shared GitHub runner (2 vCPU), with the broker and four Python processes on one box, at 1,500
listening actions a second over 200,000 listeners (2x for 15 s as the surge), three green CI runs held
p50 9.3 to 9.8 ms, p99 16.6 to 17.8 ms and p99.9 18.6 to 20.2 ms (median per second over 61 to 62
steady-state seconds each, no second over the 50 ms budget). Through the surge (3,000 actions a second)
the median per-second p99 was 22.1 to 22.6 ms, worst 23.6 to 30.3 ms, with a 1 MB/s analytics stream next
door. Of about 55,500 to 56,300 end-of-content picks per run (16,900 to 17,300 of them changed by the
listener's last action), none was made without that action and no wrong show was started. The what-if
with every action applied 1 s late made 41.0 to 41.1% of picks (median per second) from a stale queue, and
about 6,800 to 6,900 per run started the wrong show. That is the end-to-end lane through this demo code at
reduced scale, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `listeners.py` | Pocket FM listeners: listening actions on `listening-actions` (type, show, session, sequence only grows per user), and the player's end-of-content request on `autoplay-requests`. `--gap-min/--gap-max`, `--autoplay-share` and `--surge` shape the load. |
| `autoplay.py` | The Autoplay queue service: applies actions by sequence, times action-to-queue, answers every end-of-content request and checks it against the listener's last action. `--hold-ms` is the what-if. |
| `common.py` | Topics, partitioning by user, today's queue and the queue update (skips and completions out, the show playing now never next). |
| `analytics.py` | A bulk, latency-tolerant analytics stream (playtime pings) on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your listening events from Kafka as consumer group `iggy-shadow`, read-only, and publishes them to the lane, keyed by user, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A hit's new episodes drop:** `python listeners.py --actions-per-s 20000 --surge 2 --surge-for 60`.
   Pass: action-to-queue p99 under 50 ms through the surge, and no pick made without the last action.
2. **Analytics on the same broker:** `python analytics.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the queue service:** stop `autoplay.py` and start it again. It rebuilds every listener's
   queue from the lane, prints "catching up", then times live actions again.
   Pass: no action applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> --kafka-topic <your-listening-events>
```

The bridge only reads, as its own consumer group (`iggy-shadow`); the apps, every producer, every existing
consumer, the playlist pipeline and Redis are untouched. Map your fields in `to_action()`: your event
schema is not public, so the defaults accept `user_id`, `show_id`, `event`, `session_id` and
`unlock_method`, with common spellings. In shadow mode the clock starts when the bridge reads the record.
Everything runs in your AWS account.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored listening-actions stream, beside
your Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
