# Real-time provider failover for Vapi voice agents, on LaserData

Every speech-to-text, LLM and voice outcome reaches the health evaluator, and every degraded verdict reaches
every call worker, in milliseconds, before the next turn. It runs beside MSK, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/9CTsZe7yBKf5jieZ9KR2pF
- **One slide:** [`LaserData_Vapi_ProviderHealth.pdf`](LaserData_Vapi_ProviderHealth.pdf)

## Why this use case

From Vapi's reliability post: every external provider is treated as a possible failure point, with automatic
failover (the LLM chain runs OpenAI, then Azure OpenAI, then AWS Bedrock) so "a provider outage never becomes a
Vapi outage"; circuit breakers in the voice pipeline; a campaign can go from 10 to 10,000 concurrent calls in
minutes ("How We Achieved 99.99% Reliability At Vapi", Aug 2025). From Vapi's docs: transcriber failover "typically
occurs within milliseconds of detecting a failure", and a voice fallback can bring "a brief pause". From Vapi
Monitoring: monitors count "provider failures across STT, LLM, and TTS, failover and degraded mode activations"
(Apr 2026). Vapi's 2026 infrastructure postings describe Kafka, with multi-region MSK and a cluster per regional cell.

Per-call failover means each call meets the failure itself first: a failed or slow request in the middle of a
turn. A shared health signal is a loop with two hops: each provider outcome has to reach the evaluator that owns
that provider and region, and its DEGRADED verdict has to reach every call worker, because every worker has calls
on that provider. This kit runs that one loop on Apache Iggy, prints both hops' latency every second, and counts
the turns sent to a provider after it had already been marked degraded.

Vapi has not published whether its circuit breakers already share state across calls and workers; the shadow run
on your provider events is how to compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 call workers and 20,000 live calls, a turn every 3–9 s per call
(about 3,300 turns and 10,000 provider outcomes a second), two providers per stage (`stt-a`/`stt-b`,
`llm-a`/`llm-b`, `tts-a`/`tts-b`) in three regions (`us`, `eu`, `au`), and a planted incident every 10 s: one
provider in one region failing 60% of its requests for 5 s. All illustrative; set them in `common.py` and the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python health.py &                   # the evaluator, fed by the outcomes lane
python call_workers.py               # 8 call workers: outcomes out, verdicts in
```

They print a line a second, and one per incident:

```
[health]  10,000 outcomes/s  outcome-to-evaluator p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  verdicts sent … (degraded …, ok …)
[calls]   3,333 turns/s (steady)  turns on a failing provider …, after its verdict was out … (at 1000 ms it would be …)
          verdict-to-workers n … p50 …ms p99 …ms
[calls] incident llm:llm-a:eu: onset to verdict … ms, verdict to all 8 workers … ms, turns that hit it first …
```

"At 1000 ms it would be …" counts the turns that, after a DEGRADED verdict was published, still wanted that
provider within 1 second: what a lane with a 1 s end-to-end delay would have sent into the failing provider.
Set `--compare-ms`. "Onset to verdict" is detection, set by the evaluator's rule (`decide()` in `health.py`:
20 outcomes, 25% failed, in a 1 s window), not by the broker.

**What the numbers mean.** The kit times each outcome from the moment `call_workers.py` creates it to the moment `health.py` has counted it, and each verdict from `health.py` to every call worker, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (about 3,000 outcomes/s from 6,000 live calls over 4 call workers), five green runs held outcome-to-evaluator p50 2.9–6.5 ms, p99 5.3–12.1 ms and p99.9 5.9–13.3 ms (every steady-state second inside 50 ms, 61–62 per run), and verdict-to-workers p50 2.0–3.2 ms and p99 2.3–3.5 ms. The evaluator reads the 16 outcome partitions directly, on two connections; an earlier version read them through a consumer group, which visits one partition per poll, and sat at p50 12 ms and p99 23–38 ms. With 2 MB/s of call logs on the same broker, the lane's p99 moved by less than 0.6 ms. The 6–10 planted incidents per run were each marked DEGRADED a median 388–448 ms after onset (the evaluator's rule) and reached all four call workers a median 2.2–4.3 ms later; in two of the five runs one verdict reached a worker 1.4–2.7 s late. Of 1,382–1,653 turns per run that a lane 1 s behind would still have sent into the failing provider, 0–4 (at most 0.2%) went out on this lane. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The providers in the kit are simulated; your real calls add their own time. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39680690278, https://github.com/psinglap/laserdata-kits/actions/runs/39680690348, https://github.com/psinglap/laserdata-kits/actions/runs/39680730121

| File | What it is |
|---|---|
| `call_workers.py` | A fleet of call workers. Every turn makes one STT, one LLM and one voice call and publishes each outcome to `provider-outcomes`, keyed by stage:provider:region. Each worker reads `provider-health` on its own connection and moves new turns to the fallback while a primary is DEGRADED (a few still probe it). Plants the provider incidents. |
| `health.py` | The evaluator. Keeps a 1 s window per key (`decide()` is where your rule goes), publishes DEGRADED and OK with the resulting state, and rebuilds every key's state from the log after a restart. Prints the lane's p99. |
| `call_log_flood.py` | Turn logs and end-of-call reports on their own topic, for the isolation test. |
| `msk_bridge.py` | Copies your provider call events from MSK into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Campaign surge:** `python call_workers.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: outcome-to-evaluator p99 under 50 ms, and under 1% of the turns a 1 s lane would let through go to a
   provider after its verdict.
2. **The call-log firehose next door:** `python call_log_flood.py --mbps 50` while both run.
   Pass: the outcomes lane's p99 moves by less than 10%.
3. **Restart the evaluator mid-incident:** stop `health.py` while a provider is degraded and start it again; it
   replays the outcomes topic, rebuilds every key's state, and only publishes verdicts for outcomes from the last
   10 seconds. Pass: it catches up within seconds, no outcome is counted twice, and the degraded provider stays
   degraded until its probes recover.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-bootstrap-brokers> --kafka-topic <provider-events-topic> --region eu \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the call workers, producers and every existing consumer are untouched. Map your record in
`to_outcome()`: the kit needs the stage, the provider, the region and whether the request succeeded. Run one
bridge per regional cell, inside that region.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every existing
producer and consumer stays on MSK. Delivery is at-least-once (verdicts carry the state and outcomes carry a
sequence, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. There
is no cross-region replication in 0.9: run one small lane per regional cell.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored provider-events topic in one region,
alongside MSK. No advantage, no migration.

kranti@laserdata.com · laserdata.com
