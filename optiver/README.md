# A change log for config and pricing tables at Optiver, on LaserData

Every committed change to a config or pricing table lands once in Apache Iggy, in order per table, with
checkpoints in the same log. A process that starts or restarts rebuilds its tables from the log, checked
row for row against the database, without a fresh Postgres snapshot. It runs beside PG feed, so the
write path and the live feed stay as they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Optiver_TableChangeLog.pdf`](LaserData_Optiver_TableChangeLog.pdf)

## Why this use case

From Optiver's own posts and a profile written with Optiver's engineering leaders:

- Some databases "hold relatively small tables that change very frequently, like configuration and
  pricing data. These updates sit directly in the path of automated systems, and there can be thousands
  of processes that need to stay in sync with them." PG feed "reads decoded changes from Postgres through
  a single logical decoding path and forwards them to clients that subscribe to specific tables." It
  "does not maintain its own copy of the database": a client fetches the table from Postgres, then follows
  the stream (Optiver Technology blog, "Pushing Postgres beyond storage", Apr 2026).
- The same post names the limits: "Larger tables make the initial snapshot expensive and slow down new
  subscribers", and each decoding slot is "a single stream of ordered changes", "a real scaling boundary".
- "Optiver is a big user of Kafka, Postgres, and Databricks." PG feed carries "high-fanout,
  latency-sensitive messages to clients like pricing and configuration data"; Kafka here "may involve
  additional disk reads and writes, which imply unwanted latency" (The Pragmatic Engineer, Aug 2026).

Optiver kept a broker off this path on purpose, and this kit does not ask you to undo that. It takes the
side the post calls expensive: the snapshot every new or restarted subscriber needs. The log holds every
committed change and a recent checkpoint of every table, so a process rebuilds its tables by reading one
checkpoint and a few seconds of changes. Postgres serves no snapshot query for it, and PG feed's
decoding path carries nothing extra. The kit also times live delivery through the log, so the disk I/O
question gets your numbers rather than ours.

## Run it

Needs Docker and Python 3.10+. Defaults (illustrative, Optiver has not published its rates): 32 config
and pricing tables of 2,000 rows on 16 partitions, 5,000 row changes a second in small transactions,
2,000 simulated processes per subscriber node, each on 3 tables, and a storm of 200 processes starting
at once every 30 s.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python pg_writer.py &                # commits to config and pricing tables, each change written once
python checkpoint.py &               # a checkpoint of every table, every 5 s, into the log
python subscriber.py &               # a subscriber node: live copy of every table, hash-checked
python rejoin.py                     # storms of processes that start at once and rebuild from the log
```

Each prints a line a second:

```
[sub-1]  5,000 changes/s  commit-to-subscriber p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)
         deliveries to 2,000 processes …/s  tables matching the writer 32/32  gaps 0
[rejoin]  200 processes rejoined  rejoin time p50 …ms  p99 …ms  max …ms  rows from the log …
         changes replayed …  copies matching the writer 200/200  gaps 0
[rejoin] so far: … rejoins, … matching, 0 gaps; … rows served from the log, 0 snapshot queries on Postgres
```

`tables matching the writer` and `copies matching the writer` compare each copy with the database's
own state hash (the sum of crc32 over every row, carried on every change), so a missed, repeated or
reordered change shows up at once, not at the next reconciliation.

**What the numbers mean.** Latency is timed from the commit (including its wait for the next write) to
the moment `subscriber.py` has applied the change, through Python clients, on whatever machine you run it
on. On a shared 2-vCPU GitHub runner, with the broker, the writer, the checkpointer, a subscriber node and
the restart storms all on one box, at 1,000 row changes a second over 16 tables of 500 rows, three green
CI runs held a median per-second p50 of 1.8 to 2.2 ms, p99 3.0 to 3.6 ms and p99.9 3.4 to 3.9 ms, with no
second over the 10 ms budget in 69 steady-state seconds; the worst second's p99 was 4.3 to 5.5 ms, with a storm of
40 processes rebuilding from the log every 8 s on the same two CPUs. Each run rebuilt 360 processes,
three tables each (about 1,500 rows), from the latest checkpoint plus the changes after it: per-second
rejoin time p50 10.2 to 15.9 ms, p99 15.3 to 24.3 ms, worst 18.9 to 28.3 ms. All 360 copies matched the
database's state hash in every run, there were no sequence gaps, and Postgres served no snapshot query.
That is the end-to-end lane through this demo code, not the broker alone, and not a comparison with
PG feed: the comparison is the point of `bench/PLAN.md`, on your machines.

| File | What it is |
|---|---|
| `pg_writer.py` | Stands in for Postgres and PG feed's decoding path: small transactions on config and pricing tables, each committed row change written once to its table's partition, with the resulting row, per-table sequence, commit LSN, commit time and state hash. `--burst` for a burst. |
| `checkpoint.py` | Reads the log like any subscriber and writes a checkpoint of each partition's tables every `--every-s`, with the change offset it covers. Not on the live path. |
| `subscriber.py` | A subscriber node: direct partition reads, a live copy of every table, the state-hash check on every change, deliveries to `--procs` simulated processes. |
| `rejoin.py` | Restart storms: `--storm` processes start at once, each loads the latest checkpoint for its tables and replays the changes after it, then checks its copy. |
| `pg_bridge.py` | Shadows your own tables from Postgres on a temporary logical slot (wal2json), best on a standby. Writes nothing to the database; PG feed keeps its own slot. |
| `ci.sh` | The CI run: 16 tables, 1,000 changes/s, one subscriber node, a storm of 40 processes every 8 s; pass only on 95% PASS seconds, every table and every rejoined copy matching, and no gaps. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The morning start-up:** `python rejoin.py --storm 1000 --every-s 60`.
   Pass: every copy matches, rejoin p99 stays in tens of milliseconds, and `subscriber.py` stays PASS.
2. **A change burst:** `python pg_writer.py --burst 10 --burst-at 60 --burst-for 10`.
   Pass: commit-to-subscriber p99 stays under 10 ms, `gaps` stays 0.
3. **More subscriber nodes:** start `subscriber.py --name sub-2` and `--name sub-3`.
   Pass: the first node's p99 moves by less than 10%; every node reports every table matching.
4. **Bigger tables:** `--rows 20000` on the writer, `--every-s 2` on the checkpointer.
   Pass: rejoin time grows with the rows a process loads, while Postgres still serves no snapshot query.
   This is the case the PG feed post calls expensive.

## Shadow your own tables

```
pip install psycopg2-binary
python pg_bridge.py --dsn 'host=<standby> dbname=<db> user=<replication user>' \
  --tables 'pricing.vol_params,config.quote_widths' --key id
python subscriber.py --name shadow
python rejoin.py --names 'pricing.vol_params,config.quote_widths' --tables-each 1
```

The bridge opens a temporary slot, so Postgres drops it when the bridge disconnects and never holds WAL
for it; on Postgres 16+ it can decode on a standby. Its start-up read is not stitched to the slot by
transaction visibility the way PG feed does it, so a change committed during that read may arrive twice;
rows carry their full value, so a repeat is harmless, and the hash check flags any table that drifts.
The latency `subscriber.py` prints then starts at the commit time wal2json reports.

## What Apache Iggy 0.9 has today, and what it doesn't

- Has: per-partition order, many independent readers on direct partition reads, polling from an offset,
  a timestamp or the last message, time-based retention, TCP, QUIC, HTTP and WebSocket, replicated
  durability; clustering is new (Sep 2026).
- Doesn't have: log compaction (hence the checkpoints), transactions across partitions (a transaction
  spanning tables on two partitions is not atomic to a reader), exactly-once delivery (at-least-once;
  the per-table sequence makes repeats harmless), a Kafka API (the gateway is in progress).
- Hosting: LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account
  (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it. For Optiver's own
  data centres, self-hosted Apache Iggy, supported by LaserData.

## Next step

If the numbers are interesting: a 2–4 week evaluation on a few mirrored tables, beside PG feed.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
