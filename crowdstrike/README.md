# Real-time identity enforcement for CrowdStrike, on LaserData

Every authentication reaches the identity detection engine, and every block reaches every domain controller, in
milliseconds, before the account's next logon. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.crowdstrike-kit
- **One slide:** [`LaserData_CrowdStrike_IdentityEnforcement.pdf`](LaserData_CrowdStrike_IdentityEnforcement.pdf)

## Why this use case

From the Identity Protection job post (Sep 2026): "Design and implement real-time detection rules and pipelines
over high-throughput Kafka event streams, maintaining sub-second detection latency at massive scale." The engine
"turns raw authentication telemetry into indicators" and is "evolving ... from on-prem appliances toward
cloud-native, event-streaming microservices".

From the Tech Hub ("Identity Protection – Getting Started with Policies"): policy rules are triggered by "identity
detections" and "account events (such as a compromised password)", and block access or require MFA, with domain
controllers as a destination.

From the 2026 Global Threat Report press release: "the fastest observed breakout occurring in only 27 seconds";
intrusions "now move through trusted identities".

From the Data Platform job post (Jun 2026): "processing almost 3 trillion events per day", on Kafka, Flink, Storm
and Spark. That firehose stays on Kafka; this kit is about one small lane beside it.

Identity enforcement is a loop with two hops: an authentication has to reach the engine that judges the account,
and its block has to reach every domain controller before the attacker's next logon. Whatever the loop takes, a
stolen credential keeps moving. This kit runs that one loop on Apache Iggy, prints both hops' latency every second,
and prints the attacker logons let through after a block, with the new hosts they reached, next to what a lane one
second behind would have let through.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 DC sensors, 100,000 accounts across customers, 10,000 authentications a
second, a stolen credential moving laterally every 2 s (100 logons a second from one foothold to host after host)
and a compromised password every 3 s. All rates are illustrative; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python detection.py &                # the identity detection engine: logons in, blocks out
python dc_sensors.py                 # 8 DC sensors: logons out, blocks in
```

They print a line a second:

```
[detection]  10,000 auths/s  auth-to-detection p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
             enforcements sent … (lateral …, compromised …)  event-to-enforcement max …ms  accounts enforced …
[dcs]  10,000 auths/s (steady)  attacker attempts …  refused …  blocks … (lateral detected in …ms median)
       let through after the block …, new hosts … (at 1000 ms it would be …, new hosts …)  false blocks 0
       enforcement-to-DCs n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each authentication record from the moment `dc_sensors.py` lets it
through to the moment `detection.py` has applied it, and each enforcement from `detection.py` to every DC, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on
one box, at reduced scale (3,000 authentications/s, 20,000 accounts, 4 DCs), three green runs held auth-to-detection
p50 3.7–7.4 ms, p99 6.5–13.8 ms and p99.9 7.3–15.1 ms (all 61 steady-state seconds of each run inside 50 ms), and
enforcement-to-DCs p50 2.6–3.7 ms and p99 2.8–4.0 ms. A compromised-password event became a published enforcement in
about 3 ms (2.3–3.0 ms, median of each second's slowest). With 2 MB/s of endpoint telemetry on the same broker, the
lane's p99 moved by less than 0.5 ms. Each run blocked 31 accounts for lateral movement and required MFA on 20, with
no false blocks. DCs let 0–14 attacker logons through after a block was published; a lane one second behind would
have let 4,676–4,677 through, to about as many new hosts, so the lane kept out 99.7–100% of it. The kit's stand-in
detection needs about 120 ms (120–123 ms) of attacker logons to call lateral movement; that part is your detections'
and is not what the lane measures. All of this is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39889801513,
https://github.com/psinglap/laserdata-kits/actions/runs/39890011038,
https://github.com/psinglap/laserdata-kits/actions/runs/39890172355

| File | What it is |
|---|---|
| `dc_sensors.py` | A fleet of domain controller sensors. Every authentication let through publishes one record (DC, account with customer ID, source host, destination host, protocol, sequence) to `auth-events`, keyed by account. Each DC reads `identity-enforcement` directly on its own connection and refuses an account the moment its block or MFA step arrives. It also plays the outside world: the accounts' own logons, stolen credentials used to move laterally, and compromised-password events. |
| `detection.py` | The identity detection engine. Reads the 16 authentication partitions directly, judges each account over a 1 s window (`lateral()` is the stand-in for your detections), requires MFA on a compromised password at once, and publishes each enforcement with the account's sequence. |
| `telemetry_flood.py` | A heavy endpoint-telemetry stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your authentication events from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Monday 9 am:** `python dc_sensors.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: auth-to-detection p99 under 50 ms, and logons let through after a block under 1% of what a lane one
   second behind would let through.
2. **The firehose next door:** `python telemetry_flood.py --mbps 20` while both run.
   Pass: the identity lane's p99 moves by less than 10%.
3. **Restart detection:** stop `detection.py` and start it again; it replays the authentication topic and account
   events and rebuilds its windows and its list of enforced accounts, publishing again only enforcements younger
   than `--replay-enforce-s`. Restart `dc_sensors.py` and every DC rebuilds its view from `identity-enforcement`.
   Pass: it catches up within seconds and blocked accounts stay blocked.

## Design notes

- An enforcement carries the account's resulting state with a sequence, so at-least-once delivery is safe: a
  repeated or replayed enforcement changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane
  doesn't need it.
- The lane carries identities and hosts, never hashes, tickets or secrets. Your policy engine stays the source of
  truth for rules; the log is how every DC hears about a decision, and a durable, replayable record of each
  enforcement and its evidence, for the analyst and for Fusion workflows.
- An unblock after investigation fits the same topic: publish it on `identity-enforcement` with the next sequence.
- How Identity Protection connects cloud detections to DC enforcement today is not public. If enforcement is
  decided on the sensor itself, the second hop applies to the policy and risk updates the sensor acts on.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --kafka-topic <authentication-topic> \
  --account-field <account-field> --src-field <source-host-field> --dst-field <target-host-field>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the sensors, your Flink and Spark jobs and every
other consumer are untouched. Map your record in `to_auth()`: the kit needs the account with its customer ID and the
source and destination hosts and, if you have them, the DC and protocol.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored authentication-events topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
