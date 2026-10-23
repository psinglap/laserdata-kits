# Benchmark plan: the Coinbase chain event lane

**Question:** does every BLOCK_ADDED and BLOCK_REMOVED event reach every indexer within 10 ms at p99, in sequence
order per chain, while a backfill replays the log next door and indexers restart, on Apache Iggy and on the same
lane on Kafka? And how much earlier do indexers see new blocks and reorgs than behind a polled event table?

Rules are fixed before any result exists. If the other system wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Event size | ~250 B | chain, sequence_num, type, block tag, hash, height, skipped, timestamp (ChainStorage's BlockchainEvent). |
| Chains | 12 mainnets at their configured block times, then 10x and 100x the event rate | ChainStorage's open-source configs: Solana 400 ms, Arbitrum 300 ms, Base 2 s, Ethereum 12 s and others. |
| Partitions | 4, each chain in one partition | A chain's events stay in sequence order. |
| Readers | 8 → 32 → 128 indexers, each reading every partition | Many internal teams and Coinbase Developer Platform products index the same chains. |
| Reorgs | Recorded rates per chain, then 20x | Removals must reach every indexer as fast as additions. |
| Backfill | 1, 4 and 16 indexers replaying from the first event at full speed | ChainStorage's own test reached ~1,000 blocks/s; a new indexer must not slow the live ones. |
| Restarts | 1 indexer a minute, down 3 s | Resume from the checkpoint, with no gap and no duplicate. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 | A removal must never come back missing. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings. |
| Comparison | The published StreamChainEvents loop (1 s interval with backoff) on the same events | The path the open-source ChainStorage serves today. |
| Failover | Kill the leader at minute 15 | Time until indexers receive again; no gap through it. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 brokers each side, same instance family, us-east-1 | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, with the same workload shape. The kit runs
alongside each step (with a Go or Rust indexer in place of the Python one), so the report shows gaps, broken
chains and tip reads from removed blocks next to p99.

**Pass line:** ChainStorage-to-indexer p99 under 10 ms with 32 indexers, replicated, with 4 backfills running;
zero gaps, duplicates and broken chains outside the failover minute.

**Published:** p99 and p99.9 vs indexers (one line per system), the backfill trace, failover recovery, new-block
and removal delay against the polled loop, broker CPU, node count and monthly cost, and every config and raw
output.
