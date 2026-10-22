# Benchmark plan: the KV cache event lane

**Question:** at production request rates, with a heavy inference telemetry stream on the same brokers, does
every KV cache event (blocks stored, blocks removed) from every model replica reach every KV cache-aware router
within 10 ms at p99, in order and with no event missing, on Apache Iggy and on the same lane in NATS JetStream
(the durable KV event mode Dynamo's router supports)?

Rules are fixed before any result exists. If JetStream wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~150 B removal · 0.2–4 KB store (block hashes of one prefill) | One event per stored or evicted set of blocks, as engines publish them. |
| Rate | 5K → 50K → 200K events/s across deployments | One event per prefill and per eviction; long-context code traffic at 50k input tokens per request. |
| Partitions | 1 per model deployment, 64 deployments | A deployment's routers read one partition; per-replica order holds. |
| Readers | 4 → 32 routers per deployment, each reading its deployment's partition | Every router needs every event; fan-out has to hold as routers scale out. |
| Durability | Iggy `replicated` × 3 · JetStream R3 file storage | A router that restarts rebuilds from the log, so the log cannot lose events. |
| Retention | 1 hour on both | Dynamo's durable KV event mode keeps prefix blocks for one hour. |
| Peak | 3× for 10 min | A model launch or a large customer's batch of agents. |
| Neighbour | 300 MB/s of per-request inference telemetry on a separate topic, same brokers | The telemetry pipeline must not move the lane's tail. |
| Rebuild | Start a router at minute 10 with 1 hour of events retained | Time from start to an index identical to a router that never stopped. |
| Failover | Kill a broker at minute 15 | Time until routers receive again, events missing (must be 0), and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family, same region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` and the kit's `serving.py`; JetStream by `nats bench` and the same workload shape
through a JetStream build of the kit's reader. The kit runs alongside at each rate, so the report shows stale
routes and prefill tokens computed again next to p99.

**Pass line:** event-to-router p99 under 10 ms at 50K events/s with 32 routers per deployment, replicated,
through the 3× peak and with the neighbour running; zero missing events; a new router's index identical to a
running router's within 5 s of start; stale routes under 10% of what a view one second behind would make.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, rebuild time, failover recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
