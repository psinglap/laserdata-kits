# Benchmark plan: the Alpaca partner trade event lane

**Question:** with hundreds of Broker API partners, does every trade event reach its partner's event stream gateway
within 5 ms at p99, does a partner that reconnects resume exactly after its last event_id, and does a slow partner
fall behind in its own log without losing events or moving anyone else's tail, on Apache Iggy and on the same lane
on Redpanda?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~350 B | partner, account ID, sequence, event_id (ULID), event, order ID, symbol, side, type, qty, filled_qty, filled_avg_price, limit price, status, timestamp. |
| Rate | 10K → 20K → 50K → 100K trade events/s | Alpaca has not published event rates; its OMS demo ran 1M fractional orders in under a minute (~17K orders/s), and each order makes several events. |
| Partners | 300 logs, skewed: the top 10 partners carry half the events | Alpaca reports 300+ partner organisations in 40+ countries. |
| Partitions | 1 per partner, 8 for the top 10 ("sharded streaming"), keyed by account ID | Per-account order, as the SSE docs guarantee. |
| Readers | 4 gateways, each serving a quarter of the partners | One event stream per partner, all accounts under its correspondent. |
| Disconnects | each partner every ~10 min, away 0.3–5 s, up to 50 events lost in flight | Load-balancer resets, partner deploys, network blips. |
| Slow partners | 5% of partners at 30% of their rate for 10 min | Alpaca's SSE drops messages for clients that read too slowly. |
| Durability | Iggy `replicated` × 3 · Redpanda `acks=all`, RF 3 | A fill event must never come back missing. |
| Redpanda tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, franz-go | Redpanda on its best latency settings, with the client Alpaca uses. |
| Rebalance | top partner at 10× for 5 min | A robo-advisor's scheduled rebalance across its accounts. |
| Failover | Kill the leader at minute 15 | Time until gateways receive again; resumes through it. |
| Duration | 30 min per step + one full 04:00–20:00 ET session soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family, on GCP in your region | Like for like on Alpaca's cloud; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Redpanda by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside at each rate (with a Go or Rust gateway in place of the Python one), so the report shows resume time, slow
partner lag and gaps next to p99.

**Pass line:** OMS-to-gateway p99 under 5 ms at 50K events/s for live partners, replicated, through the rebalance and
with 5% of partners slow; zero gaps and zero duplicates on every resume outside the failover minute; resume-to-live
p99 under 50 ms; every slow partner back at the head of its log within 30 s of speeding up.

**Published:** p99 and p99.9 vs rate (one line per system), the rebalance trace, failover recovery, resume time,
slow-partner lag, broker CPU, node count and monthly cost for the lane, and every config and raw output.
