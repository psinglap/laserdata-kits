# Real-time chain events for Coinbase's indexers, on LaserData

Every block ChainStorage adds, or removes in a reorg, reaches every indexer in milliseconds, and a
restarted indexer resumes from its checkpoint. It runs beside ChainStorage, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Coinbase_ChainEvents.pdf`](LaserData_Coinbase_ChainEvents.pdf)

## Why this use case

From Coinbase's own sources:

- ChainStorage logs every chain "as a strictly ordered series of events marked by additions (+) or
  removals (-)", each with "a monotonically-increasing sequential number", and indexers apply them in
  order to stay on the canonical chain. Ethereum production freshness: "1 to 2s" (Coinbase blog,
  "Part 1: ChainStorage", Nov 2023).
- In the open-source ChainStorage, `StreamChainEvents` polls the DynamoDB event table per client: 10 ms
  after a read that found events, otherwise after a backoff that starts at `streaming_interval` (1 s for
  every chain but Solana) and grows 1.5x per empty read (`internal/server/handler.go`, chain configs).
- The indexing team is building "an industry-leading, ultra-low-latency indexing platform" on "gRPC,
  Kafka, and more" (job post, 2026), and CDP's SQL API states "< 250ms end-to-end from tip of chain".

So this kit runs one lane: every `BLOCK_ADDED` and `BLOCK_REMOVED` event, per chain in sequence order,
to every indexer, with the offset as the indexer's checkpoint. It prints the lane's p99 every second,
and the tip reads an indexer answers from a block a reorg already removed, on the lane and behind a
model of the published polling loop.

## Run it

Needs Docker and Python 3.10+. Defaults: 12 chains at the block times in ChainStorage's configs (about
12 blocks/s), an hour of history in the log, reorgs at 10x an illustrative rate, 8 indexers per process.

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python chainstorage.py &                   # ChainStorage's events for 12 chains
python indexer.py --name idx-1 &           # 8 indexers
python indexer.py --name idx-2 --restart-at 30
```

They print a line a second:

```
[idx-1] 96 events/s to 8 indexers  chain-to-indexer p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
        new blocks: lane p99 …ms, published loop p50 …ms p99 …ms  removals …
        tip reads from a removed block: lane …, published loop …  held in the log …  gaps 0  breaks 0  dups 0
[chainstorage] 12 events/s  BLOCK_ADDED …  BLOCK_REMOVED …  reorgs …
```

For a fresh run, `docker compose down` first: `chainstorage.py` numbers each chain's events from 1.

**What the numbers mean.** The kit times each event from the moment `chainstorage.py` records it to the
moment an indexer has applied it, through Python clients, on whatever machine runs it. On a shared
2-vCPU CI runner with the broker and every script on one box (12 chains, two processes of two indexers,
the second started 20 s in, a backfill replaying the log at 5,000 events/s next door), three green runs
held steady-state medians of p50 3.9–4.4 ms and p99 6.1–7.0 ms, with every steady-state second inside the
50 ms budget (worst single second p99 9.7–40.8 ms). Reorgs at 20x gave 37–55 removals per run: an indexer
on the lane answered 0.8–1.1 tip reads per removal from the removed block (at 200 reads/s per chain),
against 103–173 behind the modelled published loop. New blocks: 6–7 ms p99 on the lane, against a modelled
0.42–0.47 s median and 1.3–1.4 s p99 for the loop. Each late process rebuilt every chain from about 45,000
events in 2.0 s; a 3 s indexer restart resumed with 38–40 events held in the log; zero gaps, duplicates and
broken chains. That is the end-to-end lane through demo code, not the broker alone; the broker's tail is
measured with `iggy-bench` in `bench/PLAN.md`.

The "published loop" is a model of the open-source `StreamChainEvents` loop with ChainStorage's own
constants, run on the same events (`common.PollModel`). It is not a measurement of Coinbase's production
systems, which may already run differently.

| File | What it is |
|---|---|
| `chainstorage.py` | ChainStorage's event stream for 12 chains: `BLOCK_ADDED` and `BLOCK_REMOVED` with a per-chain `sequence_num`, reorgs included, plus history in the log. |
| `indexer.py` | Indexers, each on its own connection reading every partition directly: canonical chain per chain, checks for gaps and broken chains, restart from checkpoint, the removed-block and new-block numbers. |
| `backfill.py` | A new indexer replaying the whole log from the first event, for the isolation test. |
| `chainstorage_bridge.py` | One more `StreamChainEvents` client (via `grpcurl`, read-only) that copies a real ChainStorage's events into the lane, for a shadow run. |
| `common.py` | Chain settings from ChainStorage's configs, the canonical-chain rules and the model of the published loop. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Reorg-heavy hour:** `python chainstorage.py --reorgs 50`. Pass: p99 under 50 ms, `breaks` and
   `gaps` stay 0.
2. **Backfill at full speed:** `python backfill.py --rate 50000`. Pass: the live indexers' p99 moves by
   less than 10%, every pass rebuilds all 12 chains with no gap.
3. **Restarts and many indexers:** start more `indexer.py --indexers 8 --restart-at 20` processes while
   the first run. Pass: restarts resume with events held in the log, zero gaps and duplicates.

## Shadow your own ChainStorage

```
python chainstorage_bridge.py --chain base-mainnet --target <chainstorage-base-mainnet:9090> \
  -H "x-client-id: iggy-shadow"
```

It starts after the last sequence number already on the lane and never writes to ChainStorage.

## What Apache Iggy 0.9 doesn't have yet

No Kafka API (a gateway is in progress), so indexers read with the Iggy SDK (Go, Rust, Java, Python and
others). Clustering (Viewstamped Replication) shipped in September 2026 and is new. Delivery is
at-least-once; `sequence_num` makes it safe. We propose one lane, not the platform.

Contact: kranti@laserdata.com · laserdata.com
