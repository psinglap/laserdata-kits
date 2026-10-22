# Benchmark plan: the OpsAI command lane

**Question:** with every cluster's OpsAI agent connected, through an incident storm and with a customer log burst on
the same brokers, does every kubectl request reach its agent, and every reply reach OpsAI, within 10 ms at p99 per
hop, does no command run after its deadline, on Apache Iggy and on the same lane in Pulsar?

Rules are fixed before any result exists. If Pulsar wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~300 B request · ~150 B reply | Request ID, cluster, command, read or write, deadline, timestamp; reply ID, status, timings. Command output (stdout) is carried as today; its size is measured separately. |
| Rate | Today's rate → 10× → 100× | OpsAI volume is not public; the mirror sets the first step. |
| Clusters | 2,000 → 20,000 → 100,000 keys | One key per cluster with the agent; partitions or topics per cluster, as in production. |
| Readers | One agent per cluster (Go SDK), one OpsAI reply reader per backend instance | The agent side is the fan-out; each reads only its own cluster's requests. |
| Durability | Iggy `replicated` × 3 · Pulsar with its production settings (persistent topics, same ensemble and quorum) | A request that may change a cluster cannot be lost. |
| Deadlines | 15 s reads, 30 s writes, plus your real `timeout_seconds` | Measures answered in time, stopped at the deadline, expired and not run. |
| Storm | 3× investigations for 10 min, 20% slow log reads | An incident across many customers at once. |
| Disconnects | 5% of agents cut off for 30 s, 2 min and 5 min | Requests held, refused and run on reconnect. |
| Neighbour | 500 MB/s of logs on a separate topic, same brokers | A customer's log burst must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until agents and OpsAI receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift and slow leaks show over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` and the kit, Pulsar by `pulsar-perf` and the same request/reply shape over its
client and WebSocket APIs. The kit runs alongside at each step, so the report shows the command funnel next to p99.

**Pass line:** request-to-agent p99 and reply-to-OpsAI p99 each under 10 ms at 10× today's rate, replicated, through
the storm and with the neighbour running; zero commands finished after their deadline; zero requests never answered.

**Published:** p99 and p99.9 per hop vs rate for both systems, the storm trace, disconnect and failover recovery,
node CPU, node count and monthly cost for the lane, and every config and raw output.
