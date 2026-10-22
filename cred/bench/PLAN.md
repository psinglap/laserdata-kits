# Benchmark plan: the CRED payment status lane

**Question:** at payment-peak rates, does every payment status and balance event reach every Propeller node within
5 ms at p99, and does a reconnecting device get every missed event replayed from the log, in order, within 50 ms,
on Apache Iggy, and on the same lane on Redis Streams and NATS JetStream (the persistent modes Propeller already
supports)?

Rules are fixed before any result exists. If another system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~250 B | member, sequence, reference, flow, status, amount, reason, bank, coin balance, timestamp. |
| Rate | 5K → 10K → 20K → 50K events/s | CRED has not published Propeller's event rate; the steps bracket a plausible peak and go well past it. |
| Partitions | 16, keyed by member | Each member's events stay in sequence order (PENDING before SUCCESS, BILL_PAID before the coins). |
| Readers | 4 Propeller nodes, each reading every partition | A member's devices can be on any node. |
| Reconnects | 1% of devices per minute, half mid-payment, away 0.3–3 s | Phones changing network or leaving the app while the bank responds. |
| Durability | Iggy `replicated` × 3 · Redis Streams with AOF `everysec` and a replica · JetStream R3 | A SUCCESS must never come back missing. |
| Peak | 5× for 10 min | Shape to be replaced with CRED's own peak. |
| Neighbour | 50 MB/s reconciliation export on a separate topic | A bulk export must not move the lane's tail. |
| Failover | Kill the leader at minute 15 | Time until nodes receive again; resend success through it. |
| Retention | 24 h of events | Memory and disk needed to hold it, per system. |
| Duration | 30 min per step + one 12-hour soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family, AWS ap-south-1 | Like for like; nodes and ₹ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`; Redis and NATS by their own benchmark tools with the same workload shape. The kit
runs alongside at each rate (with a Go Propeller-node reader in place of the Python one), so the report shows
resend time and gaps next to p99.

**Pass line:** backend-to-node p99 under 5 ms at 20K events/s with four nodes, replicated, through the 5× peak and
with the export neighbour running; zero sequence gaps and zero unrecoverable resends outside the failover minute;
resend p99 under 50 ms.

**Published:** p99 and p99.9 vs rate (one line per system), the peak trace, failover recovery, resend time, broker
CPU and memory, node count and monthly cost for the lane, and every config and raw output.
