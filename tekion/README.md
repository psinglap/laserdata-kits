# Live vehicle availability and prices for Tekion's AI agents, on LaserData

Every vehicle status, price and mileage change reaches the context your agents read, in milliseconds, even in a
dealer group's repricing pass. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.tekion-kit
- **One slide:** [`LaserData_Tekion_VehicleContext.pdf`](LaserData_Tekion_VehicleContext.pdf)

## Why this use case

Tekion's 2026 AI Engineer posting describes the platform behind its agents: "Serve real-time context to agents
(profiles, inventory, pricing, appointments, service history)" and "Power retrieval with hybrid search ... and smart
cache/TTL to balance accuracy, latency, and cost". In "The AI-Native Dealership", Jay Vijayan and Binu Mathew set
the standard that agents act on the dealership's live records, not an outdated copy left by overnight refreshes.
Salesperson AI, launched at Tekion One 2026, answers leads on "pricing, availability, and vehicle details".

A TTL makes every cached answer a trade between accuracy and cost. When a deal starts in desking and a vehicle goes
to Pending Sale, or a group reprices its used inventory, a cached context keeps offering the old state until the
entry expires. This kit runs the one lane that removes that trade: every vehicle change, keyed by rooftop:VIN,
pushed into the agent context as it happens. It prints the lane's p99 every second, and the answers a TTL cache
would have got wrong against the same live state.

## Run it

Needs Docker and Python 3.10+. Defaults: 3,000 rooftops × 150 vehicles, 300 vehicle changes a second, and 100
agent answers a second (illustrative; Tekion has not published change or lead volumes).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python dms_inventory.py &            # every vehicle on every rooftop, and the DMS's answer to test-drive bookings
python agent_context.py              # the live vehicle context + agents answering leads from it
```

Both print a line a second:

```
[context] … changes/s  change-to-context p50 …ms  p99 …ms  PASS (<100ms)
          answers …  test drives …  a 30s TTL cache would have offered
          … vehicles no longer in stock and … old prices (…% of answers)
[dms] vehicle changes …  test drives booked …  declined as no longer available …
```

**What the numbers mean.** The kit times each change from the moment `dms_inventory.py` creates it to the moment
`agent_context.py` has applied it, through Python clients, on whatever machine runs it. On a shared 2-vCPU GitHub
runner with the broker and all three scripts on one box (200 rooftops × 150 vehicles, about 900 changes/s, a
repricing pass to about 2,000/s for 15 s, 150 agent answers a second, 2 MB/s of lakehouse CDC next door), with the
context reading every partition directly, it held p50 3.0–6.9 ms and p99 5.3–13.2 ms over three green runs (p99
6.0–15.7 ms in the repricing seconds, worst second 16.0–20.3 ms), with no second over the 100 ms budget in 171. The
DMS declined 0–0.02% of test drives as no longer available, the vehicles that went to Pending Sale in the milliseconds between
answer and booking.

A 30 s TTL cache would have got about half the answers wrong (51–53%) in CI. That figure comes from CI compressing
churn: each vehicle changes about every 33 s there, far more often than on a real lot. The share scales with the
TTL and with how often a vehicle changes, so read it as the mechanism, and run the kit on your own change rate for
your number. That is the end-to-end lane through this demo code, not the broker alone. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `dms_inventory.py` | Status, price and miles per `rooftop:VIN`, published to `vehicle-changes` with a per-vehicle sequence and the resulting state. Confirms or declines test-drive bookings against the true state. `--reprice-run` multiplies the rate and puts the extra changes on one group's rooftops. |
| `agent_context.py` | The live vehicle context from the lane, agents answering leads and booking test drives from it, and a TTL cache of the same view for comparison. |
| `cdc_flood.py` | A heavy, latency-tolerant stream (DMS table CDC on its way to the lakehouse) on its own topic, for the isolation test. |
| `msk_bridge.py` | Copies your existing vehicle inventory topic from Kafka/MSK into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A group repricing pass:** `python dms_inventory.py --reprice-run 10 --reprice-at 60 --reprice-for 120`.
   Pass: change-to-context p99 under 100 ms, and DMS declines during the pass within their range before it.
2. **Lakehouse CDC at full volume:** `python cdc_flood.py --mbps 50` while both run.
   Pass: the vehicle lane's p99 moves by less than 10%.
3. **Rebuild the context:** stop `agent_context.py` and start it again; it replays the vehicle topic.
   Pass: it catches up within seconds and answers with the right status and price for every vehicle.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <brokers> --kafka-topic <vehicle-inventory-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=… -X sasl.password=…
```

The bridge reads as its own consumer group, `iggy-shadow`; publishers and existing consumers are untouched. Map your
record shape in `to_change()`: the kit needs the rooftop and VIN, `status`, `price` and `miles`. The bridge starts
from the latest record, so the context fills as vehicles change; seed it from a snapshot if you need every vehicle
at once.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so this is one lane beside your Kafka, not a replacement for it.
Delivery is at-least-once; the per-vehicle sequence makes replays and retries safe. Replicated clusters shipped in
September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored vehicle stream, beside Kafka, read by one
agent's context in shadow. No advantage, no migration.

kranti@laserdata.com · laserdata.com
