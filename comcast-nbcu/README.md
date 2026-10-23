# Real-time stream limits for Peacock, on LaserData

Every session start, heartbeat and stop reaches every playback node's stream-limit view in milliseconds, so
three streams per account hold and a viewer moving from TV to phone gets in, even at kickoff. It runs beside
your current session pipeline, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_ComcastNBCU_StreamLimits.pdf`](LaserData_ComcastNBCU_StreamLimits.pdf)

## Why this use case

Peacock allows three streams at once per account, and its terms since December 2024 say "you may not share your
subscription outside of your household" (PCWorld, 2024). Peacock's own guidance for a stream-limit error, as
quoted from its help pages, is to "Wait up to 10 minutes" and try again, and "a browser or computer crash can
leave a session appearing active" (DocumentaryTube, September 2026). The hardest moments are arrivals: the NFL Wild
Card game, 32 million viewers and "the biggest live-streamed event in U.S. history" (Accenture), "presented us with
a few dimensions to plan for, which most notably was the huge volume of audience coming to the platform at the same
time", and in Paris "Peacock managed up to 60 concurrent single live event streams and up to 300 live events a day"
(Patrick Miceli, CTO, Comcast, September 2024). Peacock runs on a "multiregion, distributed approach" (AWS case
study).

A household's devices do not all reach the same playback node. The limit only holds, and a switch only gets in, if
the stop one node just saw is already in the view of the node deciding the next start. This kit runs that one lane
on Apache Iggy, prints its latency every second, and judges every start decision against the account's real
sessions. Peacock has not published how it enforces the limit today; the kit is a lane beside it, not a claim
about it.

## Run it

Needs Docker and Python 3.10+. Defaults: 3 playback nodes sharing 12,000 accounts with 4 devices each; one
live-event block every 60 s (compressed), 40% of accounts tuning in within 2 s of kickoff; half the households run
three streams and then switch one device for another 0.5 to 4 s later, a fifth try a fourth device, the rest watch
on one or two; a heartbeat every 10 s, a stream released after 3 missed heartbeats; 0.5% of session events
delivered twice.

```
docker compose up -d                                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                            # apache-iggy 0.9.0
python stream_limit.py --name pb-1 --index 0 &             # three playback nodes, each with its
python stream_limit.py --name pb-2 --index 1 &             # own stream-limit view, each reading
python stream_limit.py --name pb-3 --index 2               # every account's sessions
```

Each prints a line a second, and a judgement when a block's decisions are settled:

```
[pb-1] starts 785/s (granted …, refused …)  from other nodes 1,056/s  session-event-to-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  active streams …  repeats dropped …  gaps 0
[limit] block … judged: start decisions …  granted …  refused … (fourth devices refused …)  device switches let in …  viewers wrongly blocked … (…%, switches …)  streams over the limit … (…%)  released by lease …
```

A decision is judged once every event published before it has had time to arrive: a refusal while the account
really had fewer than three streams is a viewer wrongly blocked; a grant while it already had three is a stream over
the limit.

**What the numbers mean.** The kit times each session event from the moment one `stream_limit.py` publishes it to
the moment another has applied it to its view, through Python clients, on whatever machine you run it on. CI result
on a shared 2-vCPU runner, reduced scale (3 nodes, 3,000 accounts, 20-second blocks, a heartbeat every 5 s; ranges
over two green runs, [run 39892378880](https://github.com/psinglap/laserdata-kits/actions/runs/39892378880) and
[run 39892623573](https://github.com/psinglap/laserdata-kits/actions/runs/39892623573)): about 1,000 session events a
second from other nodes per reader and up to 836 start decisions a second at kickoff; session-event-to-view **p50
3.2–5.0 ms, p99 6.5–10.0 ms, p99.9 7.2–11.2 ms** (medians of 66–67 steady-state seconds per node; worst second p99
18.5–53.4 ms), with 264 of 266 steady-state seconds inside the 50 ms budget. In 12 judged blocks of about 3,200
start decisions each, **no viewer was wrongly blocked and no stream went over the limit**; each block refused about
200 fourth devices and let in about 500 device switches at the limit. Each node dropped 450–485 repeated events by
event ID, with no sequence gaps. With 2 MB/s of QoE beacons on the same broker, p99 went from 6.0 to 6.9 ms (run 1)
and 10.4 to 10.0 ms (run 2). A restarted node rebuilt its view from the log in 0.29–0.59 s (6,034–7,637 active
streams from 46,000–62,000 session events) before deciding a stream. The same run with every node applying the
others' events 2 seconds late (`--hold-ms 2000`): **8.2–11.5% of start decisions wrongly blocked a viewer, mostly
device switches (about half of all switches were refused), and 5.1–6.2% let a stream in over the limit**, with only
3–9 of about 200 fourth devices refused per block. That is the end-to-end lane through this demo code, not the
broker alone; the broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

## Three tests that match your peaks

1. **A bigger kickoff.** `--accounts 30000 --kickoff-share 0.7 --ramp-ms 1000` puts more households into the first
   second of a game. Pass: session-event-to-view p99 stays under 50 ms in kickoff seconds, and no viewer is wrongly
   blocked.
2. **QoE beacons next door.** `python qoe_beacons.py --mbps 50` writes player quality-of-experience beacons to their
   own topic on the same broker. Pass: the lane's p99 moves by less than 10%.
3. **Restart a playback node.** Stop one and start it again, as a deploy would. It reads the lane from two blocks
   back, rebuilds every account's running streams, sends any stop its devices made while it was away, then decides
   starts. Pass: it rebuilds in seconds, with no sequence gaps and no stream left running in the view.

And one what-if: `--hold-ms 2000` makes each node apply the others' events only once they are 2 seconds old, as a
consumer stuck in a rebalance or a cross-region hop would.

## Try it on your traffic

`kafka_bridge.py` joins the Kafka (or Amazon MSK) topic your players or playback service write session events to,
as its own consumer group `iggy-shadow`, and writes each start, heartbeat and stop into Apache Iggy, keyed by
account. It only reads, so every producer and every existing consumer is untouched. Map your record in
`to_session()`: the kit needs the account, the device, the session ID and the event type.

```
python kafka_bridge.py --bootstrap <broker> --kafka-topic <playback-session-events> \
  --account-field <account-field> --device-field <device-field> \
  --session-field <session-field> --type-field <event-type-field>
python stream_limit.py --name shadow --read-only
```

Compare the shadow node's active streams per account with your concurrency service. If your session events run on
Kinesis instead, the bridge is a small change. Apache Iggy has official Go, Java, Node.js, Rust, C# and Python SDKs,
so the playback service can write to the lane directly once the shadow run looks right.

## Files

| File | What it is |
|---|---|
| `stream_limit.py` | One playback node: starts granted or refused under the limit, sessions out, everyone's in, decisions judged |
| `qoe_beacons.py` | A heavy neighbour: player QoE beacons on their own topic |
| `kafka_bridge.py` | Shadow your playback session events topic into the lane, read-only |
| `common.py` | Stream, topics, keys, record shape |
| `bench/PLAN.md` | The benchmark plan against Kafka, rules fixed first |
| `docker-compose.yml` | Apache Iggy 0.9.0 |
| `ci.sh` | The CI smoke test on a real broker |

## What Apache Iggy 0.9 has today, and what it doesn't

Replication (Viewstamped Replication clusters) shipped in September 2026; cross-region replication is not built in,
so run a lane per region and mirror it. Delivery is at-least-once; the per-writer sequence makes repeats harmless. A
Kafka-compatible gateway is in progress and not in 0.9, which is why the trial uses `kafka_bridge.py` and why every
existing consumer stays where it is. LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS
or GCP account (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

Contact: kranti@laserdata.com · laserdata.com
