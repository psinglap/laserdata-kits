# Real-time KV cache events for Baseten, on LaserData

Every block a model replica stores or evicts reaches every KV cache-aware router in milliseconds, before the next
request on that prefix, and a new router rebuilds its index from the log. It runs beside your event plane, so
nothing else has to move.

**Apache Iggy, built by LaserData.** One-slide pitch: [`LaserData_Baseten_KVCacheEvents.pdf`](LaserData_Baseten_KVCacheEvents.pdf).
Write-up with the setup script: @@WRITEUP@@

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

@@CI@@

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
