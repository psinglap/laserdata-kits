# Real-time fuel theft alerts for BlackBuck, on LaserData

Every fuel-sensor ping reaches the fuel rules, and every drain alert reaches the truck owner, in milliseconds and in
order per device, while the fuel is still going. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.blackbuck-kit
- **One slide:** [`LaserData_BlackBuck_FuelAlerts.pdf`](LaserData_BlackBuck_FuelAlerts.pdf)

## Why this use case

From BlackBuck's fuel telematics post: the capacitive fuel sensor connects to the truck's GPS device, which is "the
communication gateway" and sends fuel level, location, ignition status and timestamp to BlackBuck's servers;
refuels and thefts show as sharp rises and falls, checked against ignition OFF-to-ON patterns, location, speed and
a time window, on raw and smoothed signals; and "4 out of 10 truck owners" on the platform reported fuel pilferage
at least once a year (Behind the Sensor, BlackBuck engineering blog, Aug 2025). From the telematics stack post:
device pings are formatted by MINT and pushed to Kafka topics, consumed by the Timeline and Alarm services
(Telematics Stack and Platform, Oct 2020). From the Annual Report 2025-26: 500K+ average monthly active devices in
FY26, and fuel monitoring sold on "insights on fuel efficiency, pilferage".

A drain alert is worth most while the fuel is still going: the owner can call the driver, or the dhaba where the
truck is parked, before the tank is empty. A siphon moves litres a minute, so every minute between the drop and the
owner's phone is fuel gone. This kit runs that one lane on Apache Iggy: fuel-sensor pings keyed by device into the
fuel rules, drain alerts out to the owner's app, the fleet dashboard and SMS. It prints both hops' latency every
second, counts the drain alerts that arrived while the drain was still running, and adds up the litres still in
the tank at that moment. Because a truck's pings stay in order in one partition, the rules can compare each reading
with that truck's last ones: the smoothing, the stop's high mark and the ignition change all depend on order.

BlackBuck has not published its alert latency, its ping interval or how fuel alerts are wired today; the
architecture post is from 2020. This kit is one lane beside Kafka, not a replacement for it.

## Run it

Needs Docker and Python 3.10+. Defaults: 50,000 trucks with a fuel sensor and a ping every 10 s each (about 5,000
pings a second; both illustrative, set them to yours), trucks alternating 20–120 min of driving with 10–90 min
parked, 6 drains and 20 refuels a minute across the fleet (drains compressed so a demo shows some), siphon rates of
10–25 L/min for 2–10 minutes, and sensor noise: jitter on every reading, slosh while driving, and now and then a
single-reading spike.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python fuel_rules.py &               # the fuel rules, fed by the fuel ping lane
python trucks.py                     # 50,000 trucks: pings out, alerts in
```

They print a line a second:

```
[fuel-rules]  5,000 pings/s  ping-to-rules p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  parked …
              alerts sent … (drain …, refuel …)  stall gc …ms loop …ms
              slowest ping->broker … ms, broker->rules … ms
[trucks]  5,000 pings/s (steady)  parked …  drains started …
          drain alerts at the owner app …, while still draining … (at 60 s it would be …)
          litres still in the tank … (at 60 s …)  after the drain ended … (lane delay …)
          false drain alerts …  refuels recorded … (false …)  ping-to-notifiers n … p50 …ms p99 …ms
```

"At 60 s it would be …" counts the same drain alerts that would still have found the drain running if the alert
had arrived 60 seconds after the ping that raised it, and the litres still in the tank then. Set `--compare-s`.
"After the drain ended" counts alerts for drains the rules only saw after they stopped (smoothing and the ping
interval set that); "lane delay" is the part of those whose ping was taken while the fuel was going, which is the
lane's to keep at zero. "False drain alerts" are alerts on trucks nobody drained, from noise or a refuel; they must
stay at zero. "Stall" is the longest garbage-collection pause and event-loop stall in that process that second.

**What the numbers mean.** The kit times each ping from the moment `trucks.py` creates it to the moment
`fuel_rules.py` has evaluated it, and each alert from the ping that raised it to all three notifiers, through Python
clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box,
at reduced scale (20,000 trucks, a ping every 10 s, about 2,000 pings/s, drains compressed to 150 a minute and 3
notifiers), three green runs in a row on the final code held ping-to-rules p50 4.4–6.9 ms, p99 7.9–13.0 ms and
p99.9 8.7–14.4 ms (182 of 182 steady-state seconds inside 50 ms), and ping-to-notifiers (both hops) p50 7.2–10.0 ms
and p99 10.2–14.6 ms. With 2 MB/s of low-network history on the same broker, the lane's p99 moved by at most 0.2 ms.
Of 151 to 163 drain alerts per run at the owner app, 96.7–98.1% arrived while the fuel was still being drained, with
9,864 to 11,003 litres still in those tanks; with the same illustrative siphon rates, alerts arriving 60 s later
would have found 29–32% less fuel left to save. The 3 to 5 alerts per run that arrived after a drain had ended were
drains the rules only saw after they stopped (smoothing and the ping interval); none was delayed past a drain's end
by the lane, and there were no false drain or refuel alerts (25 to 26 refuels recorded per run). That is the end-to-end loop
through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines. The rules in the kit are stand-ins that take no time; yours add their own,
and the cellular uplink from the device adds its own before any of this.

| File | What it is |
|---|---|
| `trucks.py` | A fleet of trucks with GPS devices and fuel sensors. Each device publishes a ping every 10 s to `fuel-pings`, keyed by device, with fuel level, ignition, speed, position and its ping sequence. Trucks drive, park, get refuelled and sometimes drained. Three notifiers (owner app, fleet dashboard, SMS) read `fuel-alerts`; the owner app checks whether each drain is still running. |
| `fuel_rules.py` | The fuel rules: a drain while parked with the ignition off, and a refuel at a stop (recorded, never alarmed), on the median of the last 3 readings. `Truck.apply()` is where yours go. Prints the lane's p99. |
| `history_flood.py` | Pings buffered during low network, uploaded on reconnect, on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your formatted ping topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by device, rule thresholds. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Night halt:** `python trucks.py --peak 5 --peak-at 60 --peak-for 120`, five times the drains while most trucks
   are parked at roadside stops.
   Pass: ping-to-rules p99 under 50 ms, no drain alert delayed past its drain's end by the lane, no false alerts.
2. **Low-network history next door:** `python history_flood.py --mbps 50` while both run.
   Pass: the fuel ping lane's p99 moves by less than 10%.
3. **Restart the fuel rules:** stop `fuel_rules.py` and start it again; it resumes from its stored offset (stored
   once a second) and rebuilds each truck's state from that truck's next pings.
   Pass: it catches up within seconds, and no alert reaches a notifier twice as a new alert.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <formatted-ping-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the devices, the load balancer, ping formatting, every producer and the timeline, alarm and
warehouse services are untouched. Map your record in `to_ping()`: the kit needs the device ID, fuel level in litres,
ignition, speed and position; pings without a fuel reading are skipped. Run it inside your AWS account, next to your
brokers, and compare the kit's drain and refuel events with your production ones.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every other consumer of
your ping topics stays on Kafka. Delivery is at-least-once (pings carry their sequence and alerts name their ping, so
repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is local
disk per node, not object storage: that keeps the tail in milliseconds, and it is why this is one lane, not the
whole telemetry history or the S3 archive your Kafka Connect sink writes. Java services can use the Java SDK.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored ping topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
