# Real-time outcomes for CleverTap's online learner, on LaserData

Every checkout, add-to-cart and uninstall reaches the online learner in milliseconds, so the user's next
decision is made with it, even at IPL peak. It runs beside TesseractDB, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_CleverTap_DecisionOutcomes.pdf`](LaserData_CleverTap_DecisionOutcomes.pdf)

## Why this use case

From CleverTap's own sources:

- The Tesseract Decisioning Engine has two learners. "The online learner is a contextual bandit" that
  "changes its mind after every outcome" and is "consulted on every decision"; "once an outcome is
  observed, the next decision for that user is computed with it." "Every delivered action produces one
  logged record", and "the reward for a decision is a function of the goal events that follow it within
  the goal's horizon H": "positive signals (checkout, add-to-cart) and negative signals (uninstall,
  subscription cancellation)". Frequency and touch-point caps define the feasible set. "Both systems run
  on every request in milliseconds" (CleverTap blog, Oct 2026, Jacob Joseph, Head of Data Science).
- TesseractDB counts "50B+ user actions processed daily", with up to ten years of event-level history
  (TesseractDB page). For IPL 2023 one customer's ingestion went from about 45M to a 309M events-a-minute
  peak in under five days (CleverTap tech blog, Apr 2023).
- The Log Collector runs on ECS Fargate with Graviton (tech blog, Nov 2024); thousands of message delivery
  service instances run across AWS regions (tech blog, Nov 2023).

TesseractDB, the offline learner and delivery are the right home for history, retraining and campaign
responses, and this kit does not touch them. Getting the outcome to the online learner before the user's
next request is a narrower job with a tighter clock: a user taps a cart reminder, checks out, and opens
the app half a second later. If the checkout is still waiting in a batch, the engine sees an open cart and
sends the reminder again. This kit runs delivered decisions and goal events on their own lane, keyed by
user, read by the online learner directly, and counts the outcome: decisions made without the user's last
outcome.

CleverTap does not publish how outcomes reach the online learner today, which transport carries them, its
event rates, the horizon and cap values, or the gap from an outcome to the next decision request. The lane
is a proposal; the kit's flags let you set those to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 2,000,000 users of one brand's app, 20,000 outcomes a second
(delivered decisions, add to cart, checkout, uninstall, subscription cancellation), half of them followed
by a decision request 0.3 to 2 s later, a frequency cap of 3 touches a day, and a 3× IPL surge for the
first minute. These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python learner.py &                    # the online learner, answering every decision request
python apps.py &                       # decisions delivered, then carts, checkouts, uninstalls, cancellations
python event_history.py --at 20        # heavy event history next door
```

Each prints a line a second:

```
[apps]  60,000 outcomes/s (surge)  delivered …  add to cart …  checkout …  uninstall …  subscription cancel …  decision requests …  users …
[learner]  60,000 outcomes/s  outcome-to-learner p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  rewards credited …  users in context …
[next-decision] decisions …  changed by the last outcome …  decided without the last outcome …%  wrong action chosen …  (sent when nothing was due …)
```

`decided without the last outcome` is the business number. Each record carries its kind (a delivered
decision with its action, or a goal event), the user and a sequence that only grows per user. The learner
folds each record into the user's context the same way in every script (`common.fold()`): cart open,
checked out, uninstalled, cancelled, touches today. It credits each goal event inside the horizon to the
action delivered before it (the (x, a, r) record), and decides from the feasible set that context allows
(`common.feasible()`): a cart reminder only while the cart is open, push and in-app only while the app is
installed, nothing past the cap. The action scores are a fixed function of the user, standing in for both
learners' scores; the kit tests the lane, not the model. Each decision request carries the sequence of the
user's last record and the action the engine should choose after it. `learner.py` asks whether it had
applied that record when the request started. If not, the decision was made without the last outcome; if
that changes the action, the engine chose wrongly (`wrong action chosen`), and `sent when nothing was due`
counts the ones that should have been no action at all: past the cap, or nobody left to reach.

What-if: `python learner.py --hold-ms 1000` applies every outcome only once it is 1 s old, as a one-second
batch window would, so you can see how many decisions such a path makes without the last outcome.

**What the numbers mean.** The kit times each record from the moment `apps.py` sends it to the moment `learner.py` has applied it, through Python clients, on whatever machine you run it on. On a shared GitHub runner (2 vCPU), with the broker and four Python processes on one box, at 1,500 outcomes a second over 200,000 users (2x for 15 s as the surge), three green CI runs held p50 4.3 to 8.2 ms, p99 7.3 to 15.3 ms and p99.9 8.1 to 17.5 ms (median per second over 61 to 62 steady-state seconds each, no second over the 50 ms budget). Through the surge (3,000 outcomes a second) the median per-second p99 was 8.5 to 19.8 ms, worst 9.7 to 20.7 ms, with a 1 MB/s event-history stream next door. Of about 56,800 to 57,700 decisions per run (16,500 to 16,800 of them changed by the user's last outcome), none was made without that outcome and no wrong action was chosen; 9,178 to 9,312 goal events per run were credited to the decision before them. The what-if with every outcome applied 1 s late made 40.5 to 41.2% of decisions (median per second) without the last outcome, and about 3,600 to 3,700 per run chose the wrong action, 431 to 480 of them a message when nothing was due. That is the end-to-end lane through this demo code at reduced scale, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `apps.py` | Users of a brand's app: delivered decisions and goal events on `decision-outcomes` (kind, action, sequence only grows per user), and the next decision request on `decision-requests`. `--gap-min/--gap-max`, `--request-share` and `--surge` shape the load. |
| `learner.py` | The online learner: applies records by sequence, credits rewards, times outcome-to-learner, answers every decision request and checks it against the user's last outcome. `--hold-ms` is the what-if. |
| `common.py` | Topics, partitioning by user, the context update, the guardrails (feasible set), the decision. |
| `event_history.py` | A bulk, latency-tolerant stream of raw user actions on its own topic on the same broker. |
| `tee_bridge.py` | Reads JSON lines on stdin (the Upload Events API shape by default) and publishes delivered decisions and goal events to the lane, keyed by user, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **An IPL innings break:** `python apps.py --outcomes-per-s 20000 --surge 3 --surge-for 60`.
   Pass: outcome-to-learner p99 under 50 ms through the surge, and no decision made without the last outcome.
2. **Event history on the same broker:** `python event_history.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the learner:** stop `learner.py` and start it again. It rebuilds every user's context, caps
   and rewards from the lane, prints "catching up", then times live outcomes again.
   Pass: no touch or reward counted twice, back under budget within seconds.

## Shadow your own traffic

```
tail -F <your-json-lines> | python tee_bridge.py
python learner.py
```

CleverTap does not publish what carries events between the Log Collector, TesseractDB and Experience
Builder, so the bridge takes one JSON record per line on stdin: a log tail, an export being written, or a
command-line consumer of your internal queue. It only reads its input; the SDKs, the collector, TesseractDB
and delivery are untouched. The defaults read the Upload Events API shape (`identity` or `objectId`,
`evtName`, `evtData`) and map `Notification Sent` to a delivered decision, `Added To Cart`, `Charged`,
`App Uninstalled` and `Subscription Cancelled` to the goal events; rename them in `to_record()` to your
brands' event names. In shadow mode the clock starts when the bridge reads the record. Everything runs in
your AWS account.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored decision-outcome stream, beside
TesseractDB. No advantage, no migration.

kranti@laserdata.com · laserdata.com
