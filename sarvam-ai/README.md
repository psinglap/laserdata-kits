# Real-time call concurrency for Sarvam AI, on LaserData

Every conversation start and end reaches the plan-ceiling ledger, and every headroom change reaches every
voice-agent worker, in milliseconds, even at peak. It runs beside your call stack, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_SarvamAI_CallConcurrency.pdf`](LaserData_SarvamAI_CallConcurrency.pdf)

## Why this use case

From Sarvam's Voice Agents docs ("Dialing rate & concurrency", read Oct 2026): a call "occupies a concurrency slot
from the moment the agent and caller start speaking until the call ends", and dialing and ringing hold none. Plan
ceilings on attempts per second and on concurrency are "shared across everything that dials — every campaign,
instant outbound, and inbound traffic". "When combined activity pushes past a ceiling, new calls are queued or
rejected", and "Inbound arrives on its own schedule, so reserve capacity for it explicitly when you also run
outbound."

From Sarvam's job posts (Staff Engineer - Telephony and Staff Engineer, Product Infrastructure, Oct 2026): the
platform that runs voice agents and campaigns "currently powers millions of voice calls a day", "handles 50M+
minutes of conversations every month", and is "a multi-tenant SaaS platform operating at sub-second latency SLOs",
built in Python and FastAPI with Temporal, PostgreSQL and Redis.

A shared ceiling is a loop with two hops. Every start and end has to reach the ledger that owns the
organization's count, and the ledger's headroom change has to reach every worker, because the next campaign call or
inbound call for that organization can land on any of them. While either hop is in flight, workers decide on a
stale view: campaign calls start inside the capacity kept for inbound, or wait while a slot is free. This kit runs
that one loop on Apache Iggy, prints both hops' latency every second, and counts the conversations that started
past a ceiling, next to what the same attempts would have done with a view one second behind.

Sarvam has not published how the shared ceiling is enforced today; the shadow run on your call events is how to
compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 12 voice-agent workers, 300 organizations, 60 conversations offered a
second (80% campaign and instant outbound, 20% inbound) at 90 s average talk time, about 5,400 live conversations:
roughly twice the calling-hour average that 50M+ minutes a month implies. Each organization has a plan ceiling
with 20% kept for inbound (illustrative values; set yours in `plan()` in `common.py`).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python ledger.py &                   # the plan-ceiling ledger, fed by the slot lane
python call_workers.py               # 12 workers: starts and ends out, headroom in
```

They print a line a second:

```
[ledger]  … slot events/s  slot-event-to-ledger p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent …
[ceiling] live conversations …  orgs outbound-full …  full …/300  starts …
          outbound past its share … (…% of outbound starts)  past the plan ceiling …  slots released by lease …
[workers] … conversations started/s (steady; outbound … inbound …)  live …  held back at the ceiling …
          inbound turned away …  decision-to-workers n … p50 …ms p99 …ms
[1s-behind] with every decision 1 s late: outbound started past the share …  inbound turned away …
          outbound held with room free …
```

**What the numbers mean.** The kit times each slot event from the moment `call_workers.py` creates it to the
moment `ledger.py` has applied it, and each headroom decision from `ledger.py` to every worker, through Python
clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one
box, at reduced scale (400 conversations offered a second over 200 organizations and 4 workers, 6 s average talk
time so ceilings fill and free within a minute, about 600 slot events a second), three green runs on the final
code held slot-event-to-ledger p50 3.3–5.2 ms, p99 6.1–9.5 ms and p99.9 6.8–10.6 ms (every steady-state second
inside 50 ms, 56 per run), and decision-to-workers p50 2.6–2.9 ms and p99 4.9–5.6 ms. With 2 MB/s of post-call
payloads on the same broker, the lane's p99 moved by 0.2 ms or less. Up to 71–75 of 200 organizations were at a
ceiling in a given second, and workers held back 5,305–5,436 outbound attempts. With the live loop, 0.19–0.35% of
outbound conversations started inside the inbound reserve, and 0–2 past the plan ceiling, from workers deciding at
the same moment. Judged against the same view at the same moment, a view one second behind would have started
16.5–17.3% of outbound conversations inside the inbound reserve, turned away 37–43 inbound calls the live view
admitted, and held back 2,003–2,102 outbound attempts while a slot was free. When the workers stopped, the ledger released all
their slots by lease within the lease time. That is the end-to-end loop through this demo code, not the broker
alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.
CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39868270885, https://github.com/psinglap/laserdata-kits/actions/runs/39868482556, https://github.com/psinglap/laserdata-kits/actions/runs/39868491390

| File | What it is |
|---|---|
| `call_workers.py` | A fleet of voice-agent workers. Every conversation start and end publishes the worker's live conversations for that organization (the resulting count, with a sequence) to `slot-events`, keyed by organization, plus a beat a second. Each worker reads `org-headroom` on its own connection and holds outbound or turns inbound away when told. |
| `ledger.py` | The plan-ceiling ledger. Sums the latest count from every worker per organization, compares it with the ceiling and the inbound reserve, and publishes OPEN, OUTBOUND_FULL or FULL on each change. Releases a silent worker's slots after a 3 s lease. Prints the lane's p99 and conversations started past a ceiling. |
| `transcript_flood.py` | A heavy post-call payload stream (outcome, transcript, collected variables) on its own topic, for the isolation test. |
| `call_events_bridge.py` | Copies your conversation start and end records from stdin (a log tail, or a CLI consumer of whatever carries them today) into Iggy, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A campaign launch:** `python call_workers.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: slot-event-to-ledger p99 under 50 ms, and outbound past its share under 1% of outbound starts.
2. **Post-call payloads next door:** `python transcript_flood.py --mbps 20` while both run.
   Pass: the slot lane's p99 moves by less than 10%.
3. **Lose a worker, restart the ledger:** stop `call_workers.py` and watch the ledger release its slots after the
   lease; then stop `ledger.py` mid-run and start it again. It replays `slot-events` and rebuilds every
   organization's count from the latest per-worker counts.
   Pass: no slot stays held by a stopped worker, and the restarted ledger catches up within seconds.

## Design notes

- Resulting counts with a sequence make at-least-once delivery idempotent: a repeated or replayed record changes
  nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it: it decides when a new
  conversation may start, not what is billed.
- A crashed worker cannot hold an organization's capacity: its beats stop and the ledger releases its slots after
  the lease. A counter that only moves on start and end keeps those slots until someone resets it.
- The kit leaves ring time out: an attempt is judged at the moment its conversation would start. Dialing and
  ringing hold no slot in Sarvam's model, so a production dialer would add its own pacing on top of the headroom.

## Shadow your own traffic

```
tail -F /var/log/agent-runtime/conversations.jsonl | python call_events_bridge.py \
  --event-field event --org-field org_id --worker-field host --source-field source --id-field interaction_id
```

The bridge only reads its input; telephony, the agent runtime, Temporal and the webhooks are untouched. Map your
record in `to_event()`: the kit needs the event (start or end), the organization and the worker, and uses the
interaction ID to ignore a repeated start or end. The post-call webhook alone is not enough, because it arrives
after the call ends. Put your plan ceilings in `plan()`, then compare the ledger's headroom changes with the
admissions and rejections your platform made.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored stream of call events, alongside your
current stack. No advantage, no migration.

kranti@laserdata.com · laserdata.com
