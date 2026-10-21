# Real-time vehicle state for the Rivian app, on LaserData

Every vehicle's latest charge, locks, closures, cabin temperature and position reach the latest-state
service the app reads in milliseconds, in order per vehicle, with the full signal stream flowing next to
it. It runs beside MSK, so the telematics pipeline, Flink, Event Watch and the data lake stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.rivian-kit
- **One slide:** [`LaserData_Rivian_VehicleState.pdf`](LaserData_Rivian_VehicleState.pdf)

## Why this use case

From Rivian's own talks:

- The telemetry architecture runs vehicle (TCM) → sync service on EKS → Amazon MSK (raw) → Flink
  normalization, feeding Event Watch and geofence, the data lake, and a telematics service on EKS whose
  "sensor latest state" is "managed in Redis Cache", with "latency of less than 100 ms", "used for mobile app
  and reliability engineering" (AWS re:Invent 2023, ANT317, Anirban Kundu and Rupesh More).
- "Over 150,000 vehicles stream 5,500+ signals every five seconds" to power push notifications, AI use cases
  and analytics (Guruguha Marur Sreenivasa and Saahil Khurana, RV Tech, 2025).
- Before a pre-filter, every Flink job read the entire Kafka topic and discarded about 99.9% of it; filtering
  early cut daily volume from 288 TB to 34 TB (RV Tech at P99 CONF, as reported by Kai Waehner, Jan 2026).
- Producers batch with `linger.ms=100` and snappy compression, which cut bytes in by 70% and cluster CPU by
  57% (re:Invent 2023). Right for bulk telemetry; on a path the app reads, linger alone can add up to 100 ms.

So the number that decides whether the app shows the vehicle as it is, is how fast each report's
app-facing signals reach the latest-state service, and how that tail holds when the full signal stream is
heavy. This kit runs that one lane on Apache Iggy, with only the signals the app shows on it, and prints its
p99 every second.

## Run it

Needs Docker and Python 3.10+. Defaults: 150,000 vehicles reporting every 5 s (30,000 reports/s), 2,000 app
opens a second, half of them right after a lock or charging change.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python vehicle_state.py &            # the latest-state service the app reads
python vehicles.py                   # 150,000 vehicles: reports out, app reads checked
```

They print a line a second:

```
[vehicle-state]  30,000 reports/s  report-to-state p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
                 lock or charge changes …  app reads served …  vehicles 150,000
[vehicles]       30,000 reports/s  awake 150,000  lock or charge changes …  app reads checked …
                 on the latest report …%  one behind …%  two or more behind …%  wrong lock or charge state shown …
```

`vehicles.py` knows every report it has sent, so it checks each app read the state service served: did it
show the vehicle's latest report, or one the vehicle had already replaced, and if so, did the owner see the
wrong lock or charging state? That is the business cost of a slow lane.

**What the numbers mean.** The kit times each report from the moment `vehicles.py` creates it to the
moment `vehicle_state.py` has applied it, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU CI runner with the broker and all scripts on one box, at 3,000 reports/s (15,000 vehicles) and
300 app opens a second, three green runs held p50 4.6 to 8.9 ms, p99 7.4 to 15.0 ms and p99.9 8.2 to 16.1 ms
(median of 61 to 62 steady-state seconds each), with at most one second over the budget in a run. 3 MB/s of
full signal reports on the topic next door moved the lane's p99 by 2% or less. Of about 18,000 app opens per
run checked against the truth (about 60% of them right after a lock or charging change), a median of 100% per
second showed the vehicle's latest report; 16 to 29 per run were one report behind because the newer report
was still in flight, none were two behind, and 0 to 2 per run showed the wrong lock or charging state. That is the
end-to-end lane through this demo code, not the broker alone, and it does not include the cellular network.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `vehicles.py` | Connected vehicles parking, driving, charging, locking and unlocking. A report every 5 s per vehicle to `app-vehicle-state`, keyed by vehicle, with a per-vehicle sequence and the resulting value of every app signal. Checks every app read against the truth. |
| `vehicle_state.py` | The latest-state service: the newest value of every app signal per vehicle, rebuilt from the log on start. Plays the app's opens and publishes what it served to `app-reads`. |
| `signal_flood.py` | A neighbour on the same broker: the full signal set of every report (16 KB records) on `vehicle-signals`. |
| `msk_bridge.py` | Copies your normalized telemetry topic from MSK into the lane as a read-only consumer group, keeping only the app signals. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Evening plug-in:** `python vehicles.py --peak 2 --peak-at 60 --peak-for 120` (twice the vehicles
   awake for two minutes). Pass: report-to-state p99 under 50 ms, and no app read two or more reports behind.
2. **The full signal stream next door:** `python signal_flood.py --mbps 100` while both run. Pass: the
   lane's p99 moves by less than 10%. The script prints the rate it actually reached; one Python process may
   not reach 100 MB/s on a laptop.
3. **Restart the state service:** stop `vehicle_state.py` and start it again; it replays the retained
   reports and rebuilds every vehicle. Pass: it catches up within seconds and app reads return to the latest
   report.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <brokers> --kafka-topic <normalized-telemetry-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, as its own consumer group `iggy-shadow`; the vehicles, the sync service, every
producer and the Flink jobs are untouched. Map your record shape in `to_state()`: the kit needs the vehicle
ID and whichever app signals the record carries. Run it inside your AWS account, next to your brokers.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored telemetry topic, alongside MSK.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
