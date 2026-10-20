# Real-time safety verdicts for Character.AI, on LaserData

Every chat turn reaches the trust and safety classifiers, and every verdict reaches every chat server, in
milliseconds, before that conversation's next turn. It runs beside WarpStream, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/WKNPGw1NxVkcj4p9oPgrJ8
- **One slide:** [`LaserData_CharacterAI_SafetyVerdicts.pdf`](LaserData_CharacterAI_SafetyVerdicts.pdf)

## Why this use case

From Character.AI's own case study with WarpStream: the team captures "the full prompt" sent to the LLM in near
real time, at 5 GiB/s, and the pipeline feeds "real-time trust and safety classifiers", prompt and model A/B tests,
replay-on-failure debugging and a feature store ("Boring by design", WarpStream, 2026). From Character.AI's blog:
classifiers check the model's responses and user inputs, violating content "will be blocked from their
conversation with the Character", and self-harm references get a pop-up pointing to the National Suicide
Prevention Lifeline (Teen safety, Dec 2024).

A verdict from a stream classifier is a loop with two hops: the turn has to reach the classifier, and the
classifier's verdict has to reach whichever chat server builds the conversation's next prompt. A swipe or a quick
follow-up can come less than a second after the turn that was flagged. If the verdict is still in flight, the next
prompt carries the blocked content, or the reply goes out without the pop-up. This kit runs that one loop on
Apache Iggy, prints both hops' latency every second, and counts the next turns built before their verdict.

Character.AI has not published which classifiers run inline in serving and which run off the stream; this kit is
about the ones that run off the stream.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 chat servers and 10,000 turns a second (about half the 20,000
queries a second Character.AI published in 2024), 1% of turns flagged, and illustrative turn gaps: typed replies
3–30 s apart, swipes and regenerations 0.3–3 s apart (`--swipe-share`, `common.next_gap`; set them to yours).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python classifiers.py &              # the trust and safety classifiers, fed by the turns lane
python chat_servers.py               # 8 chat servers: turns out, verdicts in
```

They print a line a second:

```
[classifiers]  10,000 turns/s  turn-to-classifier p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  verdicts sent … (block …, lifeline …)
[chat]  10,000 turns/s (steady)  flagged …  next turns after a flag …, served before the verdict … (at 900 ms it would be …)
        verdict-to-chat-servers n … p50 …ms p99 …ms
```

"At 900 ms it would be …" counts the same next turns that came less than 900 ms after their flagged turn: what a
lane with a 900 ms end-to-end p99 would have let through. 900 ms is WarpStream's documented end-to-end p99 with
default settings on object storage; its low-latency configuration documents under 400 ms. Set `--compare-ms`.

**What the numbers mean.** The kit times each turn from the moment `chat_servers.py` creates it to the moment `classifiers.py` has classified it, and each verdict from `classifiers.py` to every chat server, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (about 3,000 turns/s over 36,134 conversations and 4 chat servers), it held turn-to-classifier p50 11.8 ms, p99 22.7 ms and p99.9 25.2 ms (56 of 56 steady-state seconds inside 50 ms), and verdict-to-chat-servers p50 2.9 ms and p99 5.2 ms. With 2 MB/s of full prompts on the same broker, the lane's p99 stayed at 22.8 ms. Of 1,729 next turns that came right after a flagged turn, 0 were built before their verdict arrived; at a 900 ms lane, with the same turn gaps, 143 (8.3%) would have been. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The
classifier in the kit is a stand-in that takes no time; your models add their own.

| File | What it is |
|---|---|
| `chat_servers.py` | A fleet of chat servers. Every turn publishes the user's message and the reply, with the conversation's turn sequence, to `turns`, keyed by conversation. Each server reads `safety-verdicts` on its own connection, and before building a conversation's next turn checks it has the verdict for that conversation's flagged turn. |
| `classifiers.py` | The trust and safety classifiers. Classifies each turn (`classify()` is where your models go), reads every turns partition directly (no consumer group), keeps a strike count per conversation, and publishes BLOCK and LIFELINE verdicts that name the turn they cover. Prints the lane's p99. |
| `prompt_firehose.py` | Full prompts (templates, character definitions, memory) on their own topic, for the isolation test. |
| `warpstream_bridge.py` | Copies your turns or prompts topic from WarpStream (Kafka protocol) into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Evening surge:** `python chat_servers.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: turn-to-classifier p99 under 50 ms, and no next turn built before its verdict.
2. **The full-prompt firehose next door:** `python prompt_firehose.py --mbps 50` while both run.
   Pass: the turns lane's p99 moves by less than 10%.
3. **Restart the classifiers:** stop `classifiers.py` and start it again; it replays the turns topic, rebuilds
   every conversation's strike count, and still sends verdicts for turns from the last 30 seconds.
   Pass: it catches up within seconds, no turn is counted twice, and strike counts match the run before.

## Shadow your own traffic

```
pip install confluent-kafka
python warpstream_bridge.py --bootstrap <agents-endpoint> --kafka-topic <turns-topic> \
  --conversation-field <conversation-id-field> --text-field <turn-text-field> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the tokenizer, producers and every existing pipeline are untouched. Map your record in
`to_turn()`: the kit needs the conversation ID and the turn text (not the whole assembled prompt). Run it inside
your VPC, next to your Agents, so no user data leaves your domain.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and Bento pipelines, the
warehouse and the feature store stay on WarpStream. Delivery is at-least-once (verdicts name their turn, so repeats
are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is local disk per
node, not object storage: that keeps the tail in milliseconds, and it is why this is a small lane, not the 5 GiB/s
capture pipeline.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored turns topic, alongside WarpStream.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
