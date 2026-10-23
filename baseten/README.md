# Real-time KV cache events for Baseten, on LaserData

Every block a model replica stores or evicts reaches every KV cache-aware router in milliseconds, before the next
request on that prefix, and a new router rebuilds its index from the log. It runs beside your event plane, so
nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.baseten-kit
- **One slide:** [`LaserData_Baseten_KVCacheEvents.pdf`](LaserData_Baseten_KVCacheEvents.pdf)

## Why this lane

Baseten runs NVIDIA Dynamo's KV cache-aware routing in production and "often see[s] 2x faster TTFT"; on shadowed
traffic P95 and P99 fell 48–49%, with an 89% hit rate across four replicas on ~50k-token code prompts
([Baseten blog, Oct 2025](https://www.baseten.co/blog/how-baseten-achieved-2x-faster-inference-with-nvidia-dynamo/)).
The router is only as good as its view of which replica holds which KV blocks, and that view is built from KV
events: blocks stored after a prefill, blocks evicted under memory pressure. Dynamo's documentation says the
fire-and-forget transports (NATS Core, ZMQ PUB/SUB) are lossy, that a missed event leaves "a stale, incorrect
view of the worker's KV cache state", and that the durable mode keeps prefix blocks in JetStream with one-hour
retention so new routers can restore state
([router operations](https://docs.nvidia.com/dynamo/v1.0.2/components/router/router-operations.md),
[KV event replay](https://docs.nvidia.com/dynamo/v1.2.0/components/router/kv-event-replay-dynamo-vs-v-llm)).

A request routed to a replica for blocks it already evicted pays the whole missing prefill again. This kit
measures exactly that: stale routes and prefill tokens computed again, at the lane's real latency and with a
view one second behind, judged against the same cache state.

Baseten has not published which event plane it runs, how many routers it runs, or any stale-route figures. The
kit's deployment, sessions, block size and rates are illustrative.

## What is in the kit

| File | What it does |
|---|---|
| `serving.py` | One deployment: replicas with LRU KV caches publish `stored` and `removed` events to `kv-events`, keyed by deployment, with each replica's own sequence; 4 KV cache-aware routers each read the lane directly on their own connection, keep their own index and route long-context sessions by prefix overlap against load; a fifth router joins at 30 s and rebuilds from the log. Prints event-to-router p50/p99/p99.9 with PASS/FAIL, hit rate, stale routes and tokens prefilled again (live and 1 s behind), gaps, and the late router's rebuild. |
| `telemetry_flood.py` | A heavy neighbour: per-request inference telemetry on its own topic, same broker. |
| `zmq_bridge.py` | Shadow your engines' KV events (vLLM/SGLang ZMQ publishers) into the lane, read-only, and count events the PUB/SUB hop dropped. |
| `common.py` | Stream, topics, partitions (one per deployment), one-hour retention. |
| `bench/PLAN.md` | The benchmark plan against NATS JetStream, rules first. |
| `docker-compose.yml` | Apache Iggy 0.9.0. |
| `ci.sh` | The CI test on a real broker. |

## Run it

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python serving.py                    # replicas publish, routers read and route
python telemetry_flood.py --mbps 20  # optional, in a second terminal
```

Defaults: 16 replicas with 26,000 KV blocks each, 4 routers, 6,000 sessions on 60 apps, 600 requests a second. The
kit's blocks are 512 tokens, coarser than an engine's, so one Python process can play a whole deployment.

## CI result

**What the numbers mean.** The kit times each KV event from the moment `serving.py`'s replica makes the change
(including up to one 5 ms request tick before the batch is sent) to the moment each router has applied it to
its index, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the
broker and all scripts on one box, at reduced scale (8 replicas x 22,000 blocks, 4 routers, 3,000 sessions,
300 requests/s, about 600 KV events/s read by every router), three green runs on the final code held
event-to-routers p50 2.35–2.42 ms, p99 3.99–4.50 ms and p99.9 4.91–5.54 ms (medians of the steady-state
seconds; 63–64 of 64 seconds inside the 20 ms budget). The one second over budget, in one run, was the second
in which the fifth router replayed the log: it shares one Python process with the routers being timed. With
2 MB/s of inference telemetry on the same broker, the lane's p99 moved by about 0.5 ms (3.42–3.96 ms before,
3.93–4.44 ms with it). Gaps stayed 0 in every run.

The business numbers: at a 74.8–75.6% hit rate of prompt tokens, routers made 1–3 stale routes in the
steady state (1,536–43,008 tokens prefilled again); a view one second behind, scoring the same requests
against the same cache state, would have made 494–535 (18.3–19.7 million tokens), so the lane kept
99.4–99.8% of them away. The router started at 30 s replayed 14,325–14,348 events in 0.40–0.46 s and its
index was identical to a router that never stopped (176,000 block copies at the same offset).

All of this is end-to-end through Python on a shared runner, at reduced scale; it is not a broker benchmark.
The broker's own tail latency is measured separately on dedicated machines ([`bench/PLAN.md`](bench/PLAN.md)).
Runs: [39875743736](https://github.com/psinglap/laserdata-kits/actions/runs/39875743736),
[39875956826](https://github.com/psinglap/laserdata-kits/actions/runs/39875956826),
[39876142721](https://github.com/psinglap/laserdata-kits/actions/runs/39876142721).

## Then try it on your traffic

```
pip install pyzmq msgpack
python zmq_bridge.py --deployment <deployment> \
  --endpoint tcp://<replica-0>:5557 --endpoint tcp://<replica-1>:5557 ...
```

The bridge is one more ZMQ subscriber per replica: engines, the router and current subscribers are untouched.
Decoding follows vLLM's published KV event format; check it against your engine version. For TensorRT-LLM,
send the same lane message from the loop that already reads its executor's KV cache events.

## What Apache Iggy 0.9 has today, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026.
- Delivery: at-least-once; events carry the replica's sequence, so repeats change nothing.
- No Dynamo event-plane plugin for Apache Iggy exists yet, and Iggy 0.9 speaks neither the NATS nor the Kafka
  protocol (a Kafka-compatible gateway is in progress). Production use means a small publisher in the engines'
  event loop and a reader in the router.
- No built-in snapshot store: the log replays from any offset or timestamp; snapshots can be messages with an offset.

Contact: kranti@laserdata.com · laserdata.com
