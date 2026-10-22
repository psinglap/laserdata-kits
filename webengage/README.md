# Real-time Contextual Journey trips for WebEngage, on LaserData

Every tracked event reaches the worker that holds its user's trips, in order and in milliseconds,
so a Policy Renewed exit cancels that policy's reminder. It runs beside your event pipeline, so
nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_WebEngage_JourneyTrips.pdf`](LaserData_WebEngage_JourneyTrips.pdf)

## Why this use case

From WebEngage's own posts and docs:

- Contextual Journeys: "User + Context = Unique Journey Trip". One user can run several trips of the
  same journey at once, one per Policy ID, Loan ID or Category ID ("three loan application journeys
  running at the same time"; "four different instances for the same browse abandonment journey").
  Trips "must be triggered by the occurrence of an event", and context carries into Wait for Event
  (Payment Successful for the same loan ID) and Exit Trigger (Policy Renewed for the same policy
  number), keeping the journey "context-bound end-to-end" (WebEngage blog, "Introducing Contextual
  Journeys", Manoj Chawda, Dec 2025).
- 1.6T+ events tracked a year, 60B+ messages sent a month, 900+ brands; segments "re-evaluate on
  every event" (webengage.com, Oct 2026).
- Events arrive from SDKs and `POST /v1/accounts/<license code>/events` (`userId`, `eventName`,
  `eventTime`, `eventData`), which answers `"queued"` (WebEngage REST API docs).

The rule a trip needs is per user and per context: the exit for POL-1042 must end that trip, and
only that trip, before its reminder goes out, while the user's POL-2210 trip carries on. A log keyed
by user, with each partition owned by exactly one worker, gives that worker every trip of its users
in order. This kit runs that lane and checks the outcome a user would notice: did anyone who had
already renewed, paid or added to cart still get the reminder?

WebEngage has not published how journeys are implemented, what carries events internally, or a
latency target. The 50 ms budget, the reminder timing and the scale defaults below are ours. Set
them to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 20 million users, 50,000 tracked events a second (1.6
trillion a year is about 50,000 a second on average) at 2× for the first minute, three journeys
(policy renewal, loan application, browse abandonment), 30% of new trips going to a user already in
one, the reminder 2 s after entry (hours in production, compressed so a minute covers thousands of
trips), 35% of trips exiting 0.1 to 0.8 s before their reminder is due, 64 partitions, four workers,
and 50,000 campaign messages a second next door.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
for w in 0 1 2 3; do python journey_engine.py --worker $w --workers 4 & done
python send_audit.py &                 # every reminder, checked against the exits
python sdk_events.py &                 # tracked events, keyed by user
python campaign_blast.py               # a one-time campaign next door
```

Each prints a line a second (from the kit's CI run):

```
[journey-0]  1,472 events/s applied  event-to-trip p50 8.0ms  p99 14.9ms  p99.9 15.5ms  PASS (<50ms)  trips entered 481/s  reminders 305/s  cancelled by exit 187/s  out of order 0  missing 0  on another partition 0  trips running 2,487  users in 2+ trips 331
[send]   666 reminders/s queued (262 policy-renewal, 186 loan-application, 218 browse-abandonment)  sent after due p50 2.2ms  p99 10.3ms  reminders sent after the trip had exited 0 (0.000% of 58,576 judged)  trips reminded twice 0  users advanced by 2+ workers 0  exit events seen 46,564
```

The business numbers: `reminders sent after the trip had exited`, a reminder for a policy, loan or
category whose exit event was written before the reminder was due. It must stay 0 (CI allows at
most 0.1%). `cancelled by exit` is how many reminders a second the exit trigger stopped in time;
`trips reminded twice` and `users advanced by 2+ workers` must stay 0; `out of order`, `missing` and
`on another partition` must stay 0 while events are keyed by user. `users in 2+ trips` shows
Contextual Journeys at work: one user, several trips of one journey, held by one worker.

**What the numbers mean.** The kit times each tracked event from the moment `sdk_events.py` writes
it to the moment its worker has applied it to the trip, through Python clients, on whatever machine
you run it on. On a shared GitHub runner, with the broker and five Python processes on one box, at
3,000 tracked events a second from 200,000 users on 16 partitions (about 1,000 trips started a
second, 300 of them a user's second or later trip, and 6,000 events a second for the first 15 s), two
workers, and 10,000 campaign messages a second next door from second 30, three CI runs (the last two on the final code; the first predates the audit's exit counter) held
event-to-trip p50 7.8 to 9.1 ms, p99 14.9 to 16.5 ms and p99.9 15.8 to 17.6 ms (medians per second)
over 70 steady-state seconds per worker, with median p99 the same before and during the campaign.
Every second of every run was inside the 50 ms budget (worst second 16.7 to 35.7 ms). Each worker
sent about 325 reminders a second and cancelled about 175 a second because the exit arrived first;
across about 59,000 reminders judged per run (against about 46,500 exit events in the two runs that count them), the send side counted
0 reminders sent after the trip had exited, 0 trips reminded twice and 0 users advanced by two
workers, and no event was out of order, missing or on another partition. Reminders were queued 2 to
8 ms after their due time at p50 and 6 to 10 ms at p99, within the worker's 10 ms timer tick.

The closest exit in the kit comes 100 ms before its reminder, so the zero says the lane delivered
every exit in under 100 ms through these runs; it does not say what happens to an exit one
millisecond before the reminder, which no lane can win. All of this is the end-to-end lane through
this demo code, not the broker alone. The broker's own tail latency and cost per billion events are
measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `sdk_events.py` | Tracked events, keyed by user, with a per-user sequence: trips entered, activity, and exits before or after the reminder, several trips per user. |
| `journey_engine.py` | A journey worker: owns partitions `p % workers == worker`, reads them directly, keeps every trip (user + context) of its users, fires due reminders from its own timer and sends them from their own task. Offsets stored per partition, once a second. |
| `send_audit.py` | The send side: reads every reminder and every exit event, counts reminders sent after the trip had exited, trips reminded twice, and users advanced by two workers. |
| `campaign_blast.py` | One-time campaign messages on their own topic, for the noisy-neighbour test. |
| `kafka_bridge.py` | Copies /events-shaped records from a Kafka topic into the lane, read-only, as consumer group `iggy-shadow`. |
| `common.py` | Names, the three journeys (context attribute, entry, exit, activity events), the partition and ownership functions. |
| `bench/PLAN.md` | The benchmark we run with you, Kafka included; rules fixed before results. |

## Three tests

1. **Renewal-day rate, with a campaign:** `sdk_events.py --rate 100000 --peak 1` and
   `campaign_blast.py --rate 50000`. Pass: every worker keeps p99 under 50 ms; `reminders sent after
   the trip had exited` stays at 0 and `trips reminded twice` at 0.
2. **Tighten the race:** `sdk_events.py --gap-min-s 0.02 --gap-max-s 0.2` puts every racing exit 20
   to 200 ms before its reminder. Expect `cancelled by exit` to hold and `reminders sent after the trip
   had exited` to stay near 0; whatever it counts is the exits the lane delivered later than their
   lead. Run it so you know the margin.
3. **Scale the workers:** stop the four workers and start eight with `--workers 8`. Offsets are stored
   per partition, so each partition's new owner resumes from the last stored offset, at most a second
   back. The kit keeps trip state in memory, so a new owner only knows trips entered after it started;
   in production, trip state lives in your journey store and is loaded by the partition's owner.
   Move ownership stop-then-start: there is no fencing, so never run two workers that claim one partition.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topics <topic your tracked events pass through> \
  --context "Renewal Due=policy_id" --context "Policy Renewed=policy_id"
for w in 0 1 2 3; do python journey_engine.py --worker $w --workers 4 & done
python send_audit.py
```

The bridge joins as consumer group `iggy-shadow` and only reads: producers and every existing
consumer are untouched. It reads records in your /events body shape (`userId` or `anonymousId`,
`eventName`, `eventTime`, `eventData`) and takes each event's context from the `eventData`
attribute you name with `--context`. If your events travel on something other than Kafka (SQS,
RabbitMQ or an internal queue), replace the consumer loop; the rest of the kit stays.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads,
consumer offsets stored on the broker, message expiry per topic, at-least-once delivery, official
Java, Go, Rust, Python, C# and Node.js SDKs. Ownership is by partition: head-of-line blocking stays
within a partition (use many partitions and keep the trip step fast), and moving partitions between
workers is stop-then-start, with no fencing tokens. Not yet: delayed delivery (the reminder timer
stays in the worker or your scheduler; the lane carries the events that decide it) and a Kafka API
gateway (in progress), so the trial uses `kafka_bridge.py`.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored events for a set of Contextual
Journeys, beside your pipeline, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
