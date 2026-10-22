# Real-time customer memory for Sierra, on LaserData

Every fact a customer gives reaches the memory store every channel's agent reads, in milliseconds, before their
next message on any channel. It runs beside your eventing pipelines, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Sierra_CustomerMemory.pdf`](LaserData_Sierra_CustomerMemory.pdf)

## Why this use case

From Sierra's blog: AI agents are "limited by their lack of memory, forgetting almost everything they learned the
moment the conversation ends"; Agent Data Platform gives them memory across sessions, channels and systems, and
"every conversation enriches your agent's memory" (Introducing Agent Data Platform, Nov 2025). Agents "shift between
modes without making you start over or repeat yourself" (Next gen multimodal agents, Sep 2026). From Sierra's job
posts: "A customer can start by talking, switch to typing on a crowded train, send a photo of a broken part ... The
agent remembers everything, whichever way the customer talks" (Channels and Experiences); the Context Engine team
runs "real-time eventing infrastructure, streaming and batch ETL pipelines, and our Iceberg-based data lakehouse",
and owns "long-term agent memory" (Context Engine); the platform "handles hundreds of millions of calls a year"
(Payments Infrastructure).

Memory reaches the next channel in two hops: the turn has to reach the memory builder, and the customer's updated
memory has to reach the store the next channel's agent reads. Customers switch mid-conversation: an agent texts a
link during a call and the customer opens it two seconds later, with the call still open. A memory written when the
conversation closes, or by a job every few minutes, is not there yet, and the agent asks again. This kit runs that
one lane on Apache Iggy, prints both hops' latency every second, and counts the channel switches that found the
store behind the customer's latest fact.

Sierra has not published how Agent Data Platform builds memory (per turn or at conversation end, streaming or
batch) or which streaming platform it runs; this kit is about the per-turn path.

## Run it

Needs Docker and Python 3.10+. Defaults: 12 agent runtimes (4 each on voice, chat and SMS) and 5,000 turns a second
(hundreds of millions of calls a year, plus chat and SMS and peaks, is a few thousand turns a second; illustrative),
20% of turns adding a fact, and 3% of turns followed by a switch to another channel, half of them 1–5 s later while
the conversation they leave is still open (`--fact-share`, `--switch-rate`, `--quick-share`, `common.GAP_S`; set
them to yours).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python memory.py &                   # the memory builder, fed by the turns lane
python agents.py                     # 12 agent runtimes: turns out, memory checked at every channel switch
```

They print a line a second:

```
[memory]  5,000 turns/s  turn-to-memory p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  facts …  memory updates sent …
[agents]  5,000 turns/s (steady)  facts …  channel switches …, with memory …, would ask again …;
          quick switches …, fact from the conversation still open …  memory-to-store n … p50 …ms p99 …ms
```

"Would ask again" counts channel switches by customers who already gave a fact, where the store was not yet current
to that fact. "Fact from the conversation still open" counts quick switches (1–5 s) whose latest fact came from the
conversation they left: a memory written only when a conversation ends would miss those.

**What the numbers mean.** The kit times each turn from the moment `agents.py` creates it to the moment `memory.py` has applied it, and each memory update from `memory.py` to the memory store, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (about 1,750 turns/s from 14,962 customers over 6 agent runtimes), three CI runs held turn-to-memory p50 3.0 to 6.4 ms, p99 5.7 to 12.3 ms and p99.9 6.4 to 13.8 ms (medians per second; 185 of 185 steady-state seconds inside 50 ms), and memory-to-store p50 2.7 to 4.1 ms and p99 5.0 to 7.7 ms. With 2 MB/s of transcripts on the same broker, the lane's p99 was 5.7 to 12.3 ms against 5.9 to 12.6 ms before it started. Of 1,395, 1,362 and 1,370 channel switches by customers who had already given a fact, 0 found the store behind that fact. Of the 1,068 to 1,126 quick switches per run, 96.8 to 97.3% carried their latest fact from the conversation still open: in the kit's model a customer stays on a channel for about 30 turns, so nearly every quick switch does, and a memory written only when a conversation ends would have missed them. Your real conversation lengths and switch gaps decide the real number. That is the end-to-end lane through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The
memory extraction in the kit is a stand-in that takes no time; your models add their own.

| File | What it is |
|---|---|
| `agents.py` | Agent runtimes on voice, chat and SMS. Every turn publishes the customer, channel, turn sequence, turn text and any new fact to `turns`, keyed by customer. Plays the memory store too (a direct reader of `customer-memory`), and on each customer's first message on a new channel checks the store is current to their latest fact. |
| `memory.py` | The memory builder. Extracts facts per turn (`extract()` is where your extraction goes), reads every turns partition directly (no consumer group), keeps each customer's latest facts, and publishes the whole updated memory with the turn it is current to. Prints the lane's p99. |
| `transcript_flood.py` | Full transcripts and call metadata on their own topic, for the isolation test. |
| `stream_bridge.py` | Copies your conversation events from Kinesis or Kafka into Iggy, read-only, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Disruption surge:** `python agents.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: turn-to-memory p99 under 50 ms, and no channel switch finds the store behind the customer's latest fact.
2. **The transcript firehose next door:** `python transcript_flood.py --mbps 50` while both run.
   Pass: the turns lane's p99 moves by less than 10%.
3. **Restart the memory builder:** stop `memory.py` and start it again; it replays the turns topic, rebuilds every
   customer's memory, and still publishes updates for turns from the last 30 seconds.
   Pass: it catches up within seconds, no fact is applied twice, and fact counts match the run before.

## Shadow your own traffic

```
pip install boto3                    # or confluent-kafka
python stream_bridge.py kinesis --stream-name <conversation-events> --region <region> \
  --customer-field <customer-id-field> --channel-field <channel-field>
python stream_bridge.py kafka --bootstrap <brokers> --topic <conversation-events> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
```

The bridge only reads; producers and every existing pipeline are untouched. The Kinesis reader starts at the latest
record and writes no checkpoints; on a busy stream, give the trial its own enhanced fan-out consumer so it does not
share your readers' limits. Map your record in `to_turn()`: the kit needs the customer ID, the channel, and the fact
if your events carry one (otherwise put your extraction in `memory.extract()`). Run it inside your AWS account, next
to your stream, so no customer data leaves it.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, and there is no Kinesis API, so the trial uses the bridge,
and streaming ETL, the lakehouse and Insights stay on your current stack. Delivery is at-least-once (memory updates
carry the whole memory and its turn sequence, so repeats are harmless). Replicated clusters (Viewstamped
Replication) shipped in September 2026. Storage is local disk per node, not object storage: that keeps the tail in
milliseconds, and it is why this is a small lane, not the transcript pipeline.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored conversation-events stream, alongside your
eventing stack. No advantage, no migration.

kranti@laserdata.com · laserdata.com
