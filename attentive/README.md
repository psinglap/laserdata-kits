# Real-time subscriber decisions for Attentive, on LaserData

Every shopping event reaches the one worker that owns its subscriber, in order and in milliseconds,
so a burst of cart updates queues one text, with no Redis lock. It runs beside Pulsar, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/51FaSEx5WR7tGKB2YQasqw
- **One slide:** [`LaserData_Attentive_SubscriberDecisions.pdf`](LaserData_Attentive_SubscriberDecisions.pdf)

## Why this use case

From Attentive's own posts:

- The decision engine decides when to send marketing messages to subscribers based on recent
  shopping activity. With a SHARED subscription, "events for a given user can be consumed
  concurrently by multiple consumers", and "a user may receive multiple messages triggered by
  events being processed in parallel by different consumers. This is not an ideal user experience
  and may increase subscription opt-outs." Redis locks (Redlock) stalled consumers at peak; FAILOVER
  brought head-of-line blocking and a consumer count capped by partitions; KEY_SHARED needs
  key-based producer batching and can still duplicate on abrupt pod shutdowns, so a
  mutual-exclusivity counter alerts on it (Attentive Tech Blog, Apr 2025, "Goodbye Distributed
  Locks: Message Orchestration at Scale with Apache Pulsar", Danish Rehman and Arash Rakhshan).
- 8,000+ brands, 620 million texts on Black Friday (same post); over 10 billion events across a
  BFCM weekend (Attentive blog, Aug 2026); a send path that must sustain "a minimum rate of around
  25,000 messages per second" (Andrew Sega, Jul 2024).

The rule the engine needs is per subscriber: one owner at a time, in order. A log keyed by
subscriber, with each partition owned by exactly one worker, gives that by construction. This kit
runs that lane and checks the outcome a subscriber would notice: did anyone get two texts from one
shopping burst?

We do not know the decision engine's internal rates, partition counts or latency targets beyond
"sub-second decisions", so the 50 ms budget and the scale defaults below are ours. Set them to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 20 million subscribers across 8,000 brands, 30,000
shopping events a second (10 billion events over a BFCM weekend is about 29,000 a second on average)
at 2× for the first minute, sessions of about five events 50 to 600 ms apart (about 40% with two or
more triggers), 64 partitions, four workers, a one-hour reminder cooldown, and 25,000 campaign texts
a second next door.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
for w in 0 1 2 3; do python decision.py --worker $w --workers 4 & done
python send_audit.py &                 # every queued text, duplicates counted
python storefront.py &                 # shopping sessions, keyed by subscriber
python bfcm_flood.py                   # campaign texts next door
```

Each prints a line a second (from the kit's CI run):

```
[decide-0]  1,571 events/s decided  event-to-decision p50    8.0ms  p99   13.0ms  p99.9   13.9ms  PASS (<50ms)  texts   143/s  repeat triggers held   211/s  out of order 0  missing 0  on another partition 0  subscribers 22,971
[send]   261 texts/s queued (261 cart, 0 checkout)  event-to-send-intent p50   25.3ms  p99   43.6ms  duplicate texts 0/s (0 total)  subscribers decided by 2+ workers 0  subscribers texted 26,485
```

The business numbers: `duplicate texts`, a second text to the same subscriber inside the cooldown,
and `subscribers decided by 2+ workers`, the same signal as your mutual-exclusivity counter measured
on the output. Both must stay 0. `repeat triggers held` is how many triggers per second the
per-subscriber state absorbed (cart edits and checkouts after a text was already queued); each one is
a text that two unsynchronised consumers could have sent. `out of order`, `missing` and `on another
partition` must stay 0 while events are keyed by subscriber.

**What the numbers mean.** The kit times each shopping event from the moment `storefront.py`
writes it to the moment its decision worker has decided it, through Python clients, on whatever
machine you run it on. On a shared GitHub runner, with the broker and five Python processes on one
box, at 3,000 shopping events a second from 200,000 subscribers on 16 partitions (about 230 sessions
a second with two or more triggers, 6,000 events a second for the first 15 s), two workers, and
10,000 campaign texts a second next door from second 30, two CI runs held event-to-decision p50 6.3
to 7.0 ms, p99 11.7 to 14.2 ms and p99.9 12.3 to 15.2 ms (medians per second) over 70 steady-state
seconds per worker, with median p99 the same before and during the campaign blast. One run had every
second inside the 50 ms budget (worst second 23 ms); the other had 2 to 3 seconds out of 70 over it,
the same seconds on both workers (worst 449 ms), which points at the runner or the broker rather than
the workers. Event to send-intent, through the worker and its 20 ms send batches to the send side,
held p50 24 to 26 ms and p99 41 to 44 ms. Each worker queued about 140 texts a second and held about
190 repeat triggers a second; across about 26,500 subscribers texted per run, the send side counted 0
duplicate texts and 0 subscribers decided by two workers, and no event was out of order, missing or
on another partition. Checkout reminders stayed at 0 because every checkout in the kit's sessions
follows an add to cart that already queued a text.

Earlier versions of the worker stored its offset on the broker on every poll and sent each round's
texts from inside the read loop. They passed on some runners and missed the budget in most seconds on
others (p50 still 6 to 10 ms, p99 70 to 470 ms, both workers in step). The workers now store offsets
once a second and send from their own task in 20 ms batches; we have not proven which change mattered.
All of this is the end-to-end lane through this demo code, not the broker alone. The broker's own tail
latency and cost per billion events are measured with `iggy-bench` on dedicated machines
(`bench/PLAN.md`).

| File | What it is |
|---|---|
| `storefront.py` | Shopping sessions (views, add to cart, checkout, purchase, clicks), keyed by subscriber, with a per-subscriber sequence. `--route spread` drops the key to show the problem. |
| `decision.py` | A decision worker: owns partitions `p % workers == worker`, reads them directly, decides one reminder per subscriber per cooldown, writes it to `send-intents` from its own task every 20 ms. Offsets stored per partition, once a second. |
| `send_audit.py` | The send side: reads every queued text, counts duplicate texts and subscribers decided by two or more workers. |
| `bfcm_flood.py` | Campaign texts on their own topic, for the noisy-neighbour test. |
| `pulsar_bridge.py` | Copies shopping events from your Pulsar topics into the lane, read-only, on its own `iggy-shadow` subscription. |
| `common.py` | Names, event types, the partition and ownership functions, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, Pulsar KEY_SHARED and FAILOVER included; rules fixed before results. |

## Three tests

1. **Black Friday rate, with the campaign blast:** `storefront.py --rate 60000 --peak 1` and
   `bfcm_flood.py --rate 50000`. Pass: every worker keeps p99 under 50 ms; `duplicate texts` and
   `subscribers decided by 2+ workers` stay 0.
2. **Take the key away:** `storefront.py --route spread` writes the same sessions with no key, as a
   shared subscription spreads them. Expect the send side to count duplicate texts and subscribers
   decided by two workers. This is the problem the lane removes. (Not part of CI.)
3. **Scale the workers for BFCM:** stop the four workers and start eight with `--workers 8`. Offsets
   are stored per partition, so each partition's new owner resumes from the last stored offset, at
   most a second back. Pass:
   `out of order` and `missing` stay 0 and the workers are back under budget within seconds. Move
   ownership stop-then-start: there is no fencing, so never run two workers that claim one partition.

## Shadow your own traffic

```
pip install pulsar-client
python pulsar_bridge.py --service-url pulsar+ssl://<your cluster>:6651 --pulsar-topics <topics> --token <JWT>
for w in 0 1 2 3; do python decision.py --worker $w --workers 4 & done
python send_audit.py
```

The bridge attaches with its own subscription, `iggy-shadow`, and only reads: the producers, the
decision engine's subscription and every other subscription are untouched. Remove `iggy-shadow` when
the run ends, since Pulsar keeps a backlog for it. Map your record shape in `to_event()`: the kit
needs the subscriber ID, the brand, the event name and its time.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads,
consumer offsets stored on the broker, message expiry per topic, at-least-once delivery, official
Java, Go, Rust, Python, C# and Node.js SDKs. Ownership is by partition, which is the FAILOVER model,
not KEY_SHARED: head-of-line blocking stays within a partition (use many partitions and keep the
decision step fast), and moving partitions between workers is stop-then-start, with no fencing
tokens. Not yet: delayed delivery, a Kafka API gateway (in progress) and any Pulsar protocol support,
so the trial uses `pulsar_bridge.py`.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored shopping events for a set of
brands, alongside Pulsar, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
