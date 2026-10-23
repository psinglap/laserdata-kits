# Real-time mark prices for CoinDCX's liquidation engine, on LaserData

Every futures mark price and every position change reaches every liquidation worker in milliseconds, so each
second's mark batch is checked against every open position long before the next one lands. It runs beside your
feed handler and order path, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.coindcx-kit
- **One slide:** [`LaserData_CoinDCX_MarkPrices.pdf`](LaserData_CoinDCX_MarkPrices.pdf)

## Why this use case

From CoinDCX's own Futures API docs and public price feed. Each futures mark price carries `bmST`, "the timestamp
at which Third-Party exchange sent this event", and a CoinDCX receive time, `cmRT`. In twelve snapshots of the
public futures prices endpoint on 22 October 2026, the marks for about 500 instruments shared one `bmST` on the
whole second, a new batch arrived every second, and `cmRT` was 142–198 ms after `bmST`. There are 505 active
USDT-margined instruments.

The docs also give the rules each batch is checked against: an isolated position "will be liquidated when the
margin available in your position goes below" its tiered maintenance margin (60K USDT needs 50K × 1.5% + 10K × 2%
= 950 USDT); cross positions "will get liquidated if the ratio becomes greater than equal to 1"; and `liquidate`
orders are "created by the system".

So every second, about 500 marks land at once, already 150–200 ms after the partner sent them, and every open
position has to be re-checked against them. The part CoinDCX controls is the time from the batch arriving to the
last liquidation worker acting on it. In a fast market, every 100 ms there lets the price run further past the
liquidation price before the order fills: first the trader's remaining margin, then, past the bankruptcy price,
the insurance fund. This kit runs that lane on Apache Iggy and prints both every second.

## Run it

Needs Docker and Python 3.10+. Defaults: 505 instruments with a mark batch every second, 300,000 open positions,
2,000 position changes a second, a quarter of accounts on cross margin.

```
docker compose up -d                                              # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                                   # apache-iggy 0.9.0
python exchange.py --crash-at 60 &                                # feed handler, position book, order path
python liquidator.py --name liquidator-1 --shard 0 --shards 2 &   # a worker: every mark, half the accounts
python liquidator.py --name liquidator-2 --shard 1 --shards 2     # the other half
```

They print a line a second:

```
[liquidator-1]   505 marks/s  1,000 position changes/s  lane p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
                 mark batch checked in …ms  liquidation orders … (mark-to-order p99 …ms)  positions …
[exchange]  2,000 position changes/s  mark batches 1 (crash)  liquidations filled …  stale …  mark-to-fill p99 …ms
            past liquidation price … USDT  past bankruptcy … USDT
            a worker 0.5s behind: past liquidation … USDT  past bankruptcy … USDT (… fills)
```

**What the numbers mean.** A mark is timed from the moment its batch arrives at the feed handler (one timestamp
per batch, taken before encoding) to the moment a worker has read it; a position change from the moment the
position book writes it. Both go through Python clients, on whatever machine you run it on. On a shared 2-vCPU
GitHub runner with the broker and all four scripts on one box (505 instruments, 40,000 open positions, 600
position changes a second, a 5% crash over 15 s, two workers, 2 MB/s of trade export next door), the medians of
the steady-state seconds over five green runs were lane p50 5.5–12.0 ms, p99 11.4–20.6 ms and p99.9
16.3–31.4 ms on each worker; the worst single second had a p99 of 24.4–41.3 ms, and every steady-state second
(62–63 per worker per run) was inside the 50 ms budget. Each whole mark batch, cross-margin accounts included,
was checked within 30–69 ms of arriving. About 7,000 liquidations were filled per run (4,900–5,300 of them in
the 15 crash seconds, 0–5 on a position that had changed since the worker saw it). In that simulated crash, fills
landed 60,800–66,700 USDT past liquidation prices and 6,800–8,900 USDT past bankruptcy prices; a worker running
0.5 s behind would have filled the same orders 17–20% further past liquidation and 39–44% further past bankruptcy.
That is the end-to-end lane through this demo code, not the broker alone, and the USDT figures depend on the kit's
price model, not CoinDCX's books. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`,
on dedicated machines.

| File | What it is |
|---|---|
| `exchange.py` | The feed handler (a mark batch for every instrument on the whole second, to `mark-prices`, keyed by instrument, resulting value and sequence), the position book (opens, adds, reduces, closes; isolated and cross; 2x–25x; to `positions`, keyed by account) and the order path (fills each `liquidate` order at the last price, and again 0.5 s later for comparison). `--crash-at` drags the market down. |
| `liquidator.py` | A liquidation worker: reads every mark partition and its shard of position partitions directly, rebuilds from the log on start, checks isolated positions with per-instrument heaps of liquidation prices and cross accounts once per batch (margin ratio ≥ 1), sends orders from a separate task. |
| `common.py` | Topics, the partition function, and the margin math from your docs: tiered maintenance margin, liquidation and bankruptcy prices. |
| `trade_export.py` | A heavy, latency-tolerant stream (fills, fees, funding and ledger records for the lakehouse) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the topic your risk services read marks from today into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Assumptions of the kit, not CoinDCX figures: the open-position count, the change rate, the share of cross
accounts, the leverage mix, the price model and the 0.5 s comparison. Your docs publish the mark and the
liquidation price but not which price triggers liquidation; the kit uses the mark, as most futures venues do.
Change the flags to match what you see.

## Three tests

1. **A crash:** `python exchange.py --crash-at 60 --crash-for 120 --crash-pct 8 --rate 6000`.
   Pass: lane p99 under 50 ms on every worker, and every mark batch checked within 100 ms through the crash.
2. **Trade export at full volume:** `python trade_export.py --mbps 30` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart a worker:** stop one `liquidator.py` and start it again; it rebuilds every mark and its positions
   from the log before it acts. Pass: rebuilt within seconds, and no breached position missed on the first check.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <mark-price-topic> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; the feed handler and every
existing consumer are untouched. Map your record shape in `to_mark()`: the kit needs `k` (instrument, e.g.
`B-BTC_USDT`), `seq` (`bmST` works), `mp`, and `b` and `n` (batch and instruments per batch). Position changes
bridge the same way.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; the workers use the
Apache Iggy SDK (Rust, Go, Python, Java, C#, Node). Delivery is at-least-once; the per-instrument and per-position
sequence makes replays and duplicates safe, and the order path drops orders for a position that has since
changed. Replicated clusters shipped in September 2026, which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored mark feed and position stream, beside your
feed handler and liquidation engine. No advantage, no migration.

kranti@laserdata.com · laserdata.com
