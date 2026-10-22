# Real-time Osprey actions for Discord, on LaserData

Every action reaches the rules worker that owns its user, in order and in milliseconds, so a spammer
label is in place before that user's next message. It runs beside Pub/Sub or Kafka, so nothing else
has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Discord_OspreyActions.pdf`](LaserData_Discord_OspreyActions.pdf)

## Why this use case

From Discord's own posts and Osprey's docs:

- Osprey handles "~400 million actions per day" across "204 action types" and "2288 rules", with
  "the average action triggering ~500 rules". Actions arrive "synchronously via GRPC or asynchronously
  via a Message Queue"; "Actions are first sent to the Osprey Coordinator", which balances them across
  Rules Workers and "adds some resilience against spiky traffic patterns". Rules emit effects such as
  `LabelAdd(entity=UserId, label='spammer')` and `DeclareVerdict(verdict='reject')`; results go to a
  Kafka output sink and Apache Druid (Discord Engineering, "Osprey: Open Sourcing our Rule Engine",
  Jared Miller and Ayu, Feb 2026).
- The asynchronous input is Kafka (`osprey.actions_input`), Google Pub/Sub, or a custom input stream
  registered with the `register_input_stream` plugin hook; labels live in a labels service the
  deployment supplies (Osprey docs, `docs/integration/data-flow.md` and `integrations.md`). InfoQ
  describes "a Rust coordinator and stateless Python worker nodes" (Mar 2026).
- Discord moved its search indexing queue from Redis, which "began dropping messages once CPU maxed
  out", to PubSub (Vicki Niu, "How Discord Indexes Trillions of Messages", Apr 2025), and reindexes its
  largest guilds by backfilling history: that backfill is the kit's neighbour.

Rules carry state across actions through labels, so the order in which one user's actions are
evaluated matters: a spam run's next message is 80 to 250 ms behind the one that earned the label. A
log keyed by user, with each partition owned by exactly one worker, evaluates one user's actions one
at a time, in order, so the label is in place for the next one. This kit runs that lane and checks the
outcome members of a server would notice: was any message allowed after its sender was labelled?

Discord has not published that its workers miss labels today; the kit's `--route spread` mode shows
what happens without the key. We do not know the action queue's peak rates, partitioning or latency
targets, so the 50 ms budget and the scale defaults below are ours. Set them to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 5 million active users, 10,000 actions a second (about twice
the ~400 million a day average) at 3× for the first minute as a raid, 3% of sessions spam runs of 8 to
20 link messages 80 to 250 ms apart from accounts under 12 hours old, 64 partitions, four rules
workers, and a 40,000 documents a second search reindex backfill next door.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
for w in 0 1 2 3; do python rules_worker.py --worker $w --workers 4 & done
python effects_audit.py &              # every execution result, let-throughs counted
python actions.py &                    # actions keyed by user, with spam runs
python reindex_backfill.py             # search reindex backfill next door
```

Each prints a line a second (from the kit's CI run):

```
[rules-1]  1,570 actions/s evaluated  action-to-verdict p50    9.5ms  p99   15.4ms  p99.9   16.2ms  PASS (<50ms)  labels added   15/s  rejected with label in place   128/s  out of order 0  missing 0  on another partition 0  users 28,515
[effects]  3,109 results/s    259 rejected    26 labels added  action-to-result p50   29.6ms  p99   50.0ms  messages let through after the label 0/s (0 total)  users labelled twice or by 2+ workers 0  users labelled 1,977
```

The business numbers: `messages let through after the label`, a `send_message` from an already
labelled user that was still allowed, and `users labelled twice or by 2+ workers`. Both must stay 0.
`labels added` and `rejected with label in place` show the rules working: each rejection is a message
that a worker without the label would have allowed. `out of order`, `missing` and `on another
partition` must stay 0 while actions are keyed by user.

The two rules are deliberately small and stand in for your SML: an account younger than 24 hours that
sends 5 messages with links inside 10 s gets `LabelAdd likely_spammer` and that message is rejected;
every later message from a labelled user is rejected. The kit keeps labels in worker memory; in Osprey
they live in the labels service.

**What the numbers mean.** The kit times each action from the moment `actions.py` writes it to the
moment its rules worker has evaluated it, through Python clients, on whatever machine you run it on.
On a shared GitHub runner, with the broker and five Python processes on one box, at 3,000 actions a
second from 200,000 users on 16 partitions (about 22 spam runs a second), two workers, and 8,000
reindex documents a second next door from second 30, a CI run held action-to-verdict p50 8.9 to 9.0 ms,
p99 15.7 to 15.9 ms and p99.9 16.8 ms (medians per second) over 70 steady-state seconds per worker,
with every second inside the 50 ms budget (worst second 18.3 ms) and median p99 the same before and
during the backfill. Action to result, through the worker's 20 ms send batches to the effect side, held
p50 29 ms and p99 48 ms. Each worker added about 10 labels and rejected about 95 messages with the label
in place every second; across about 2,000 users labelled per run, the effect side counted 0 messages
let through after the label and 0 users labelled twice or by two workers, and no action was out of
order, missing or on another partition. All of this is the end-to-end lane through this demo code, not
the broker alone. The broker's own tail latency and cost per billion events are measured with
`iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `actions.py` | Osprey-shaped actions (logins, messages, identity changes, guild joins) and spam runs from fresh accounts, keyed by user, with a per-user sequence. `--route spread` drops the key to show the problem. |
| `rules_worker.py` | A rules worker: owns partitions `p % workers == worker`, reads them directly, runs the two rules per user in order, writes execution results from its own task every 20 ms. Offsets stored per partition, once a second. |
| `effects_audit.py` | The effect side: reads every execution result, counts messages let through after the label and users labelled twice or by two workers. |
| `reindex_backfill.py` | Message documents on their own stream, for the noisy-neighbour test. |
| `pubsub_bridge.py` | Copies Osprey actions from your Pub/Sub topic into the lane, read-only, on its own `iggy-shadow` subscription. |
| `common.py` | Names, action types, the partition and ownership functions, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, Pub/Sub and Kafka included; rules fixed before results. |

## Three tests

1. **A raid, with a reindex next door:** `actions.py --rate 30000 --peak 1 --spam-share 0.1` and
   `reindex_backfill.py --rate 80000`. Pass: every worker keeps p99 under 50 ms; `messages let through
   after the label` and `users labelled twice or by 2+ workers` stay 0.
2. **Take the key away:** `actions.py --route spread` writes the same sessions with no key, the way a
   queue without ordering keys spreads them across workers. Expect the effect side to count
   let-throughs and users labelled by two workers, and some spam runs never to be labelled because no
   single worker sees five of their links. This is what the key removes. (Not part of CI.)
3. **Add workers mid-run:** stop the four workers and start eight with `--workers 8`. Offsets are
   stored per partition, so each partition's new owner resumes from the last stored offset, at most a
   second back. Pass: `out of order` and `missing` stay 0 and the workers are back under budget within
   seconds. Move ownership stop-then-start: there is no fencing, so never run two workers that claim
   one partition.

## Shadow your own traffic

```
gcloud pubsub subscriptions create iggy-shadow --topic=<your Osprey actions topic> --message-retention-duration=1h
pip install google-cloud-pubsub
python pubsub_bridge.py --project <your GCP project>
for w in 0 1 2 3; do python rules_worker.py --worker $w --workers 4 & done
python effects_audit.py
```

The bridge reads through its own subscription, `iggy-shadow`, and only reads: the services that
publish actions, Osprey's subscription and everything else are untouched. It reads Osprey's JSON
envelope; map your fields in `to_action()`. If your actions go through Kafka, the same mapping works
on a confluent-kafka consumer in its own group, `iggy-shadow`, on `osprey.actions_input`. Delete the
subscription when the run ends.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads,
consumer offsets stored on the broker, message expiry per topic, at-least-once delivery, official
Rust, Python, Go, Java, C# and Node.js SDKs. Ownership is by partition: head-of-line blocking stays
within a partition (use many partitions; a user whose rules make a slow RPC holds up its partition),
and moving partitions between workers is stop-then-start, with no fencing tokens. Not yet: a Kafka API
gateway (in progress) and any Pub/Sub API, so the trial uses `pubsub_bridge.py`, and Osprey would read
the lane through an input stream plugin or a coordinator consumer.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored Osprey actions, beside your queue,
priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
