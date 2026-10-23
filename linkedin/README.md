# Real-time tracking signals for LinkedIn, on LaserData

Latency-sensitive signals from Tracking API reach real-time processing in milliseconds, and wait in a durable
log through a restart, so none are lost. It runs beside Kafka and Northguard, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.linkedin-kit
- **One slide:** [`LaserData_LinkedIn_RealtimeTracking.pdf`](LaserData_LinkedIn_RealtimeTracking.pdf)

## Why this use case

LinkedIn created Kafka and has since built Northguard and Xinfra to replace it ("Introducing Northguard and
Xinfra", LinkedIn Engineering, Jun 2025). This kit does not propose another log for that estate. It is about one
path that, by LinkedIn's own description, runs outside it.

From "Rebuilding LinkedIn's Tracking Infrastructure" (LinkedIn Engineering, Oct 2026): tracking turns impressions,
clicks, page views and media playback into signals, at hundreds of billions of events a day, with traffic isolated
by category so that bursty media events cannot take capacity reserved for other traffic. And: "The ingestion
service sends latency-sensitive signals directly to real-time processing, while Apache Kafka provides the
asynchronous boundary for near-real-time and offline processing."

A direct path is fast, but it has nowhere to keep a signal while a reader is slow or restarting. This kit puts
the latency-sensitive category on a log of its own that is fast enough to stay on that path: one topic, keyed by
a pseudonymized member ID with a per-member sequence number, read by real-time processing instances partition by
partition. A restarted instance resumes from its stored offset and applies what waited, in order.

What we could not verify: which event types LinkedIn counts as latency-sensitive, their rate and latency target,
what real-time processing is, and how the direct path handles a restarting consumer today (it may already
buffer or retry). The event names, rates and member IDs in the kit are illustrative.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 members, 20,000 latency-sensitive signals a second in client
requests of 1 to 6 signals each, 16 partitions, a 50 ms budget.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0
python tracking_api.py &                               # Tracking API: client requests into the lane
python realtime.py --instance 0 --instances 2 &        # real-time processing, even partitions
python realtime.py --instance 1 --instances 2 --restart-at 30 --restart-for 12   # odd partitions, one restart
python media_burst.py --mbps 50 --at 20 --duration 60  # a media burst next door, on its own topic
```

Each prints a line a second (from the CI run; one line each in your terminal):

```
[tracking-api]   2,002 signals/s in 571 requests  request-to-broker-ack p50 0.8ms  p99 1.3ms
[realtime-1]       952 signals/s  signal-to-processing p50 2.5ms  p99 4.5ms  p99.9 4.7ms  PASS (<50ms)  lost 0  repeats dropped 0
[realtime-1] restart: away 12.0 s, held in the log and applied 12,074 signals, oldest 12.1 s,
             repeats dropped 218, caught up in 2.0 s, lost 0
```

**What the numbers mean.** The kit times each signal from the moment `tracking_api.py` builds its request to
the moment `realtime.py` has applied it, through Python clients, on whatever machine runs it. On a shared GitHub
runner, with the broker and four Python processes on one box, at 2,000 signals a second from 100,000 members,
two real-time processing instances on 16 partitions, one of them restarted for 12 s, and 2 MB/s of media events
next door from second 20, two CI runs held signal-to-processing p50 2.5 to 3.9 ms, p99 4.6 to 7.2 ms and p99.9
4.9 to 7.7 ms (medians per second) over 111 steady-state instance-seconds per run, with every second inside the
50 ms budget (worst second p99 7.4 to 12.0 ms). Median p99 was the same before and during the media burst (4.5
vs 4.6 ms, 7.1 vs 7.2 ms). Requests were acknowledged by the broker at p99 1.8 to 2.8 ms. The restarted
instance found 11,928 to 12,074 signals waiting, dropped 167 to 218 repeats read again from its last stored
offset, was back inside the budget within 2 s, and **lost 0 signals**. That is the end-to-end lane through this
demo code, not the broker alone; the broker's tail is measured with `iggy-bench` in `bench/PLAN.md`, on
dedicated machines.

| File | What it is |
|---|---|
| `tracking_api.py` | Client requests of 1 to 6 signals, shared context once per request, a per-member sequence; one batch per partition every 5 ms; a request is acknowledged only when the broker has its signals. |
| `realtime.py` | One real-time processing instance: owns partitions `p % N == i`, reads them directly, stores its offset once a second, drops repeats and counts gaps by sequence, and can simulate a restart. |
| `media_burst.py` | Render, play, pause and exit events per video on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies an existing Kafka tracking topic into the lane as a read-only consumer group, pseudonymizing member IDs first, for a shadow run on real traffic shape. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Peak hour:** `python tracking_api.py --peak 3 --peak-at 30 --peak-for 120`.
   Pass: signal-to-processing p99 under 50 ms, lost 0.
2. **A media burst next door:** `python media_burst.py --mbps 100` while both run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart a reader:** `--restart-at 30 --restart-for 30` on one instance, or stop it and start it again.
   Pass: everything sent while it was away is applied in order, repeats are dropped, lost stays 0, and it is
   back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <tracking topic> --salt <secret> \
  -X security.protocol=SSL -X ssl.ca.location=...
python realtime.py
```

The bridge reads in its own consumer group, `iggy-shadow`, so Tracking API and every existing consumer are
untouched. Tracking records are Avro: decode them in `to_signal()` with your schema registry client. Member IDs
are hashed with your salt before they enter the lane. Delete the consumer group when the shadow run ends.

## What Apache Iggy 0.9 does not do yet

No Kafka API (a gateway is in progress), so the trial uses the bridge and readers use an Iggy client (Java,
Python, Go, Rust, C#, Node.js). Delivery is at-least-once: readers drop repeats by sequence, as the kit does.
Replicated clusters shipped in September 2026 and are new. LaserData Cloud runs as a managed service on AWS or
GCP, or inside your own AWS or GCP account (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs;
LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored tracking category, beside Kafka and
Northguard. No advantage, no migration.

kranti@laserdata.com · laserdata.com
