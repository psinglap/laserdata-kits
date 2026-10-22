# Real-time 5G core probe records for Jio Platforms, on LaserData

Every vProbe record reaches your anomaly check, and every network-function alert reaches the workflow engine, in
milliseconds, even in a signalling storm. It runs beside ATOM, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_JioPlatforms_ProbeRecords.pdf`](LaserData_JioPlatforms_ProbeRecords.pdf)

## Why this use case

From Jio Platforms' "Cloud Native 5G Core" whitepaper (2021): "vProbe is a probing agent which collects probing
data (Streaming Data Record-SDR) from 4G/5G Network nodes." The probes are "embedded in the virtual network
functions" and "connected through a real-time conductor fabric" to ATOM, which uses the records "to perform anomaly
detection on the collected data based on the historical models", with "Automated SLA management capabilities in the
workflow engine, orchestrating the operational tasks between the systems". The Data Management Services layer keeps
session data outside the CNF, so on a failure another CNF "that has instant access to the same data" can carry on.

From Computer Weekly (Mar 2021), on ATOM: built in-house to "process more than 500 billion records a day", with
"real-time analytics capabilities to process 50 million records every second"; anomaly detection "can drill down to
individual end-nodes, whether a physical server, virtual machine or containerised service". Jio built it to reduce
"dependence on external providers", on "software running on standard servers".

Today the network is larger: about 285 million 5G users in June 2026 (Q1 FY27 results), and 10 live network slices on
the in-house 5G SA core, sold as "SLA-based 5G experiences" (RCR Wireless, Aug 2025).

So the time that decides how many procedures meet a failing network function is the record-to-anomaly-check and
alert-to-workflow path, at its busiest moment. This kit runs that one lane on Apache Iggy, beside ATOM: SDRs in,
keyed by NF instance; health alerts out to the workflow engine. It times both hops every second and counts the
procedures a late alert costs. We have not seen how ATOM's ingest is built today or what the conductor fabric runs
on; the kit measures the lane on its own.

## Run it

Needs Docker and Python 3.10+. Defaults: 90 AMF, SMF and UPF instances in 10 circles (3 per pool), 20,000 SDRs a
second of ~600 B with a 3× signalling storm from second 30 for a minute, 5,000 new procedures a second, and an NF
incident every 30 s (one busy instance fails 25% of procedures, slowly, for 12 s). ATOM's 50 million records a
second is the whole network; set the rate to the slice you want to test.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python anomaly.py &                    # the anomaly check: score per NF instance, alerts out
python workflow.py &                   # the workflow engine: drains flagged instances, counts late alerts
python vprobes.py &                    # SDRs, the signalling storm and NF incidents
python ran_flood.py --at 20            # RAN counters and logs next door
```

Each prints a line a second, and a line per incident:

```
[vprobes]  60,000 SDRs/s (signalling storm)  failed …%
[anomaly]  60,000 SDRs/s  SDR-to-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  NFs 90  degraded 1
[workflow] degraded amf-mu-02  major  degraded … s before the flag (detection window)  flag to workflow … ms (lane)  procedures given after the flag 0
[workflow]    90 alerts/s  check-to-workflow p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  NFs degraded 1
[workflow] procedures 5,000/s  steered to a healthy peer …  given after the flag 0
```

`procedures given after the flag` is the business number: new procedures still given to an instance after the
anomaly check had flagged it, because the alert had not reached the workflow engine yet. That count belongs to the
lane alone. Each incident also prints its **detection window** (how long the instance was failing before its score
crossed into major, set by the models and how many records the instance produces) and the **lane** time (anomaly
check to workflow engine). The lane cannot shorten the detection window; it keeps the pipe from adding to it.

**What the numbers mean.** The kit times each record from the moment `vprobes.py` creates it to the moment
`anomaly.py` has applied it, and each alert from the moment the anomaly check sends it to the moment `workflow.py`
has applied it, through Python clients, on whatever machine you run it on. On a shared GitHub runner, with the
broker and four Python processes on one box, two green CI runs at 800 SDRs a second of ~600 B (2.5× for 15 s as the
signalling storm) held SDR-to-check p50 3.6 to 4.5 ms, p99 6.4 to 8.0 ms and p99.9 7.5 to 9.3 ms (median per second
over 62 steady-state seconds each, worst second's p99 8.3 to 10.0 ms, none over the 100 ms budget). Through the
storm (2,000 SDRs a second) the median per-second p99 was 7.7 to 9.6 ms, worst 10.0 ms. Check-to-workflow ran at
about 90 alerts a second (every instance's state every second, plus every change): p50 3.9 to 5.2 ms, p99 5.5 to
6.5 ms, worst second 8.6 ms. In each run four injected incidents reached the workflow engine 1.7 to 4.2 ms after the
flag; of about 35,500 procedures per run, about 414 were steered to a healthy peer and none was given to a flagged
instance before its alert arrived; no healthy instance was flagged. RAN counters ran next door at 2 MB/s for 34 s.
That is the end-to-end lane through this demo code at reduced scale, not the broker alone; the broker's own tail
latency is measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`). The kit's scoring (weights,
thresholds, a 40-record window) is illustrative, so its detection windows of 0.3 to 0.9 s say nothing about yours.

| File | What it is |
|---|---|
| `vprobes.py` | SDRs on `vprobe-sdr`, keyed by NF instance, with a per-instance sequence. `--storm` plays a signalling storm; `--incident-*` injects NF incidents. No subscriber identity in the record. |
| `anomaly.py` | The anomaly check: failure rate and procedure time of the last N records per instance, piecewise scores, EWMA, severity buckets, every 100 ms; each instance's resulting state on `nf-health`, on change and every second. Reads its partitions directly. Times SDR-to-check. |
| `workflow.py` | The workflow engine: applies alerts by sequence, gives new procedures to a healthy peer in the pool, counts procedures given after a flag. Run a second copy with `--name noc` for the NOC view. |
| `ran_flood.py` | RAN counters and logs (~8 KB) on their own topic on the same broker. |
| `kafka_bridge.py` | Copies a probe-record topic from Kafka into Iggy, as a read-only consumer group, for a shadow run on real traffic. Only instance, procedure, result, cause and duration cross over. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Signalling storm:** `python vprobes.py --records-per-s 20000 --storm 3 --storm-at 30 --storm-for 60`.
   Pass: both hops' p99 under 100 ms through the storm, and `procedures given after the flag` at 0.
2. **RAN counters and logs on the same broker:** `python ran_flood.py --mbps 50 --at 40`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart a reader:** stop `workflow.py` and start it again. It replays the alerts, prints "catching up", and
   rebuilds every instance's state before timing live alerts. Pass: no flagged instance gets new procedures again
   by mistake, back under budget within seconds.

## Shadow your own traffic

If your SDRs, or a copy of them, already land on a Kafka topic (your data platform job posts list Kafka, Spark
Streaming, Flink and NiFi):

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-brokers> --kafka-topic <your-probe-record-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the vProbes, the conductor fabric, ATOM and every
existing consumer are untouched. Map your fields in `to_msg()`: the kit needs `k` (NF instance), `seq`, `proc`,
`res`, `cause` and `dur`. No IMSI, MSISDN or IP is copied into the lane. If the records reach you another way (a
NiFi flow, files), replace the Kafka consumer and keep the mapping. In shadow mode the clock starts when the bridge
reads the record.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (in progress; hence the bridge), exactly-once delivery (at-least-once today, which the
per-instance sequence makes safe for this lane; billing records stay where they are), and a long track record for
clustering (Viewstamped Replication shipped in September 2026).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored slice of probe records and their alerts, on
your own servers, beside ATOM. No advantage, no migration.

kranti@laserdata.com · laserdata.com
