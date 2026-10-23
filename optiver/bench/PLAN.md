# Benchmark plan: the table change log

**Question:** with Optiver's own tables, rates and process counts, can a process that starts or restarts
rebuild its config and pricing tables from Apache Iggy faster than from a Postgres snapshot, with zero
snapshot queries on the database, while live changes still reach every subscriber node within 5 ms at
p99 on the broker hop? And how does live delivery through the log compare with PG feed itself, the
"additional disk reads and writes" question, measured instead of assumed?

Rules are fixed before any result exists. If PG feed wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Tables | Optiver's own config and pricing tables, mirrored by `pg_bridge.py` from a standby | "relatively small tables that change very frequently, like configuration and pricing data" (Optiver Technology blog, Apr 2026). |
| Change rate | Recorded from a real session, replayed at 1×, 5× and 10× | Optiver has not published rates; the session sets them. |
| Partitions | One per group of tables, keyed by table | Each table's changes stay in commit order, as one decoding slot gives today. |
| Readers | 4, 8 and 16 subscriber nodes, each reading every partition | Thousands of processes sit behind a few nodes; adding nodes must not slow the others. |
| Checkpoints | Every 5 s, every 30 s | Trades checkpoint size against replay length. |
| Restart storms | 50, 200 and 1,000 processes starting at once | A deploy, a host restart, the morning start-up. Time to a full, hash-checked copy, per process. |
| Baseline | The same processes loading the same tables from Postgres the way they do today | "Larger tables make the initial snapshot expensive and slow down new subscribers" (same post). Database CPU and query time recorded. |
| Live comparison | Commit-to-subscriber latency, via PG feed and via the log, same tables, same session | Optiver's concern about a broker's disk I/O, measured on its hardware. |
| Durability | Iggy `replicated` × 3, fsync policy recorded | Like for like; what a restart can rely on. |
| Failover | Kill the Iggy leader during a storm | Time until rejoins complete again. |
| Duration | One full trading session per step | Drift shows over hours. |
| Hardware | 3 Iggy nodes in Optiver's own data centre, on the class of machine PG feed runs on | Nodes, cores and power reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

The broker hop is measured with `iggy-bench`; the kit runs alongside at each step, so the report shows
`tables matching the writer` and `copies matching the writer` next to p99 and rejoin time.

**Pass line:** at the session's 10× rate, a storm of 1,000 processes completes with every copy matching
the database's state hash, zero snapshot queries on Postgres, rejoin p99 below the Postgres baseline's
median, and the subscriber nodes' commit-to-subscriber p99 under 5 ms during the storm.

**Published:** rejoin time and database load vs storm size (log and Postgres baseline), live p99 and
p99.9 via the log and via PG feed, failover recovery, broker CPU and every config and raw output.
