# Real-time task progress for Decagon voice agents, on LaserData

Every step the reasoning layer finishes reaches the voice host speaking to that caller in milliseconds, so each
progress update and follow-up answer is current. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Decagon_TaskProgress.pdf`](LaserData_Decagon_TaskProgress.pdf)

## Why this use case

From Decagon's Voice 3 launch (Oct 1, 2026): Voice 3 uses "a new duplex architecture with two layers running in
parallel. A low-latency conversational model handles the listening and speaking, from answers to progress
updates. A more powerful model handles the reasoning, tool calling, and guardrail enforcement happening behind
the conversation." The agent "gives quick progress updates when something takes a few seconds", and can "narrate
its progress and answer follow-up questions during a long-running task, rather than putting the customer on
hold" ("Introducing Voice 3 and Chord"; "Decagon Dialogues 2026"). Decagon's Agent Platform postings describe a
runtime that coordinates tool calls and communicates back to the user across "real-time systems (e.g., voice
interactions with strict latency requirements) and longer-horizon execution". Its Data Infrastructure posting
describes Kafka, Flink and Airflow for real-time ingestion, ClickHouse and BigQuery for analytics, and "tight
p95/p99 targets" (2026).

Progress updates only help when they are current. Each time the conversational layer speaks about a task, it
needs the newest step the reasoning layer has finished: otherwise it describes a lookup that already returned,
or says it is still working after the result is in. This kit runs that one hop on Apache Iggy, keyed by call,
prints its latency every second, and counts the spoken updates that were out of date.

Decagon has not published how Voice 3's two layers exchange task state. If both run in one process per call, this
lane adds nothing; it is for task execution that runs apart from the voice host. The shadow run on your events
is how to compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 voice hosts and 20,000 live calls, a task after every 4–12 s of
conversation (`rebook_flight`, `refund_status`, `replace_card`, `dispute_charge`, `reschedule_delivery`) with
steps of 0.4–2.5 s each (about 6,000 steps a second), and a spoken update every 1–3 s while a task runs. All
illustrative; set them in `common.py` and the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python voice_layer.py &              # 8 voice hosts, fed by the task lane
python reasoning_layer.py            # 20,000 live calls running tasks
```

They print a line a second:

```
[reasoning]  6,000 task events/s (steady)  tasks started …  results ready …  tasks running …
[voice]      6,000 task events/s  event-to-voice p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
             updates spoken …, out of date … (still working after the result …) (at 1000 ms it would be …)
```

"Out of date" counts spoken updates that came after the reasoning layer had published a newer step of the same
task, before that step reached the voice host. "Still working after the result" is the subset where the newer step
was the result. "At 1000 ms it would be …" judges the same spoken moments against a lane 1 second behind. Set
`--compare-ms`, `--speak-gap` and `--budget-ms`.

**What the numbers mean.** The kit times each step from the moment `reasoning_layer.py` creates it to the moment `voice_layer.py` has applied it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (6,000 live calls, about 1,770 steps a second, over 4 voice hosts each reading its own 4 partitions directly), three green runs held event-to-voice p50 4.5–5.5 ms, p99 8.1–10.0 ms and p99.9 8.9–11.5 ms (all 62 steady-state seconds inside 50 ms in each run). With 2 MB/s of conversation logs on the same broker, the lane's median p99 moved by 0.1–0.2 ms. Of 74,189–74,345 updates spoken per run, 176–193 (0.24–0.26%) were out of date on this lane, 69–95 of them saying the agent was still working after the result was in; a lane 1 second behind would have got 36,594–36,980 (about half) wrong at the same moments, so this lane had 0.5% of that. That is the end-to-end hop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The tasks in the kit are simulated; your tool calls and models add their own time. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39873378539, https://github.com/psinglap/laserdata-kits/actions/runs/39873601377, https://github.com/psinglap/laserdata-kits/actions/runs/39873910697

| File | What it is |
|---|---|
| `reasoning_layer.py` | The reasoning layer behind every live call. Runs tasks step by step and publishes each step to `task-events`, keyed by call, with the task's resulting state and the call's sequence. |
| `voice_layer.py` | The voice hosts. Each reads only its own calls' partitions, keeps every call's newest step, speaks an update every 1–3 s while a task runs, and counts updates that were out of date. Rebuilds every live call's task from the log after a restart. Prints the lane's p99. |
| `conversation_log_flood.py` | Transcripts, tool traces and summaries on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your task or tool-call events from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **An outage on the customer's side:** `python reasoning_layer.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: event-to-voice p99 under 50 ms, and under 1% of the updates a 1 s lane would get wrong are out of date.
2. **The conversation log next door:** `python conversation_log_flood.py --mbps 50` while both run.
   Pass: the task lane's p99 moves by less than 10%.
3. **Restart the voice hosts mid-task:** stop `voice_layer.py` while tasks run and start it again; each host
   replays its partitions, rebuilds every live call's task, and speaks again only for steps from the last 10
   seconds. Pass: it catches up within seconds, no step is applied twice, and every running task speaks its
   current step on the next update.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-bootstrap-brokers> --kafka-topic <agent-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the agent runtime, producers and every existing consumer (Flink, the ClickHouse sink, QA)
are untouched. Map your record in `to_step()`: the kit needs the call or conversation id, the task, whether it is
running or done, and what the caller can be told.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every existing
producer and consumer stays on Kafka. Delivery is at-least-once (steps carry the resulting state and the call's
sequence, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. There
is no cross-region replication in 0.9: run one small lane per region, next to the voice hosts. It is one binary,
which also suits agent deployments inside a customer's cloud or on-prem.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored task-events topic in one region,
alongside Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
