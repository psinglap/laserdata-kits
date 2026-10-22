# Real-time cluster commands for Middleware OpsAI, on LaserData

Every kubectl command OpsAI sends reaches the agent in that cluster in milliseconds, its reply comes back matched by
request ID, and an Auto Fix that misses its deadline never runs late. It runs beside Pulsar, so nothing else has to
move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Middleware_OpsAICommands.pdf`](LaserData_Middleware_OpsAICommands.pdf)

## Why this use case

From Middleware's own code on GitHub: the OpsAI agent in a customer cluster subscribes to Pulsar over its WebSocket
API on `mcp-<token>-<sha1(cluster)>`, runs each `mcp-k8s` request with kubectl, and posts the reply to
`<topic>-unsubscribe`, where OpsAI matches it by request ID (middleware-labs/synthetics-agent, mw-agent `opsai`,
mcp-kubernetes). The fixes of the last months are all properties of that lane:

- "one slow kubectl stalled every request queued behind it, so a burst of callers all timed out waiting on work that
  had not even started"; commands now run off the receive loop, 8 at a time (synthetics-agent #84, Aug 2026);
- the agent honours the caller's `timeout_seconds` "so our timeout cannot outlast the window it is still listening
  in", and always replies, because an unset result was read as "an empty success" (#84);
- request IDs from `UnixMilli()` collided, so a caller "blocked until its timeout even though a valid response had
  come back"; a wrong topic name "is silent ... the caller simply times out" (mcp-kubernetes, 2026);
- a ping/pong keepalive "to detect dead connections and prevent idle timeouts at intermediaries (Cloudflare: 100s,
  Azure LB: 240s)" (#82, May 2026), and an expiration on requests (#83).

OpsAI chains these commands (pods, events, logs, then in Auto Fix mode a change and a rollout check), so the lane's
round trip is paid at every step. And an agent that bounds a command from when it *starts* will still start a
request that waited in a queue, or in a backlog while its connection was down, after OpsAI gave up on it. For an
Auto Fix, that is a change nobody is watching. This kit runs that one lane on Apache Iggy, with the deadline checked
by the agent before anything runs, and prints the round trip and the command funnel every second.

Middleware has not published OpsAI's command volume or the timeouts it sends; the defaults below are illustrative.

## Run it

Needs Docker and Python 3.10+. Defaults: 2,000 clusters, 300 investigations open at once (four triage reads in
parallel, a log read, and in 30% of them an Auto Fix write and a `rollout status` check), 5% of log reads taking
seconds, 15 s timeouts for reads and 30 s for writes (clamped 5–300 s as your agent clamps them), and a 50 ms budget
for the lane's round trip. kubectl is simulated: each command sleeps for a typical time (`common.py`).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python agents.py &                   # the agent in every cluster
python opsai.py                      # 300 investigations over 2,000 clusters
```

They print a line a second:

```
[opsai]  … replies/s  request-to-agent p50 …ms p99 …ms  reply-to-opsai p50 …ms p99 …ms
         lane round trip p50 …ms p99 …ms p99.9 …ms  PASS (<50ms)
[opsai]  commands sent …  answered in time …  stopped at deadline …  expired, not run …
         answered after OpsAI gave up …  duplicates dropped …  never answered 0  investigations finished …, to on-call …
[agents] … requests/s  run …  expired, not run … (writes …)  finished after deadline 0  held in the log …
[agents] totals: … a deadline-blind agent would have run … stale commands, … of them writes
```

"A deadline-blind agent would have run" counts the requests the agent refused because their deadline had passed, at
the moment it decided: an agent that checks only how long a command runs would have started each of them.

**What the numbers mean.** The kit times each command from the moment `opsai.py` creates the request to the moment `agents.py` reads it, and each reply from `agents.py` to `opsai.py`, through Python clients, on whatever machine you run it on; kubectl's own time (simulated) is left out, and requests that waited in the log while their agents were away are counted on their own. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (400 clusters, 80 investigations open, about 92 to 95 replies a second, 7,122 to 7,431 commands per run), three CI runs held request-to-agent p50 3.6 to 3.7 ms and p99 6.2 to 6.4 ms, reply-to-OpsAI p50 3.1 to 3.4 ms and p99 5.9 to 6.2 ms, and the lane round trip p50 6.8 to 7.0 ms, p99 10.7 to 11.1 ms and p99.9 10.7 to 11.3 ms (medians per second; 189 of 189 steady-state seconds inside 50 ms). Most of that is the demo's own pacing: 1 ms polls and 2 ms send batches on each side. With 2 MB/s of customer logs on the same broker, the round trip p99 was 10.5 to 11.0 ms against 11.1 to 11.2 ms before it started. One partition's agents were cut off for 30 s in each run: 54 to 73 requests had passed their deadline when they were read and were answered `expired` without running (1 or 2 of them Auto Fix writes), no command finished after its deadline, every request was answered, and the only replies that reached OpsAI after it gave up were those `expired` ones; 8 to 20 duplicate replies were dropped. That is the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `opsai.py` | OpsAI's side. Investigations send kubectl requests keyed by cluster, each with a unique ID and a deadline, wait for the reply until that deadline and then hand the incident to on-call. Reads `kubectl-replies` directly from every partition and matches by ID; prints the round trip and the funnel. |
| `agents.py` | The agent in every cluster. Reads `kubectl-requests` directly, checks the deadline before it starts and after it waits for one of 8 slots per cluster, runs for the time left, replies `ok`, `timeout` or `expired`, and on start rebuilds "already answered" from the reply log. `run_kubectl()` is where a real executor goes. |
| `log_burst.py` | A customer log burst on its own topic, for the isolation test. |
| `pulsar_bridge.py` | Copies your MCP request topics from Pulsar into Iggy with its own subscription, read-only, and times today's request-to-reply on Pulsar. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Agents that drop off and come back:** `python agents.py --offline-partition 15 --offline-at 60 --offline-for 30`.
   Pass: every request that waited past its deadline is answered `expired` and not run, the rest run on reconnect,
   and `finished after deadline` stays 0.
2. **An incident storm and slow logs:** `python opsai.py --storm-x 3 --storm-at 60 --storm-for 120 --slow-share 0.2`.
   Pass: round trip p99 under 50 ms; reads queued behind slow logs are answered in time or refused, never run late.
3. **A customer log burst next door:** `python log_burst.py --mbps 50` while both run.
   Pass: the lane's round trip p99 moves by less than 10%.

## Shadow your own traffic

```
pip install pulsar-client
python pulsar_bridge.py --service-url pulsar+ssl://<cluster>:6651 \
  --topics-pattern 'persistent://public/default/mcp-.*' --token "$PULSAR_TOKEN"
python agents.py
```

The bridge only reads, with its own subscription (`iggy-shadow`); the agents' subscription, OpsAI's reply reader and
the unsubscribe endpoint are untouched. It writes each `mcp-k8s` request into the lane keyed by its topic (one per
cluster) with its deadline, and prints today's request-to-reply time on Pulsar as a baseline. The kit's agents only
simulate kubectl, so a shadow run never touches a cluster. Delete the `iggy-shadow` subscription when it ends.

## What Apache Iggy 0.9 doesn't have yet

Iggy does not speak Pulsar's protocol or its WebSocket API, so the agent cannot point at it unchanged; the trial uses
the bridge, and a production agent would use Iggy's Go SDK. Iggy serves TCP, QUIC, HTTP and (since 0.6) WebSocket;
agents that can reach only port 443 need a path through your edge, so check the Go SDK's transports first. A
Kafka-compatible gateway is in progress and not in 0.9. Delivery is at-least-once (replies are matched by ID and
repeats dropped). Replicated clusters (Viewstamped Replication) shipped in September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on a mirror of the OpsAI command topics, beside Pulsar.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
