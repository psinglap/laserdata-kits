# Real-time ingest freshness for Honeycomb triggers, on LaserData

A receipt for every batch the ingest service enqueues, keyed by dataset, and each Retriever node's progress
per Kafka partition reach a freshness check in milliseconds, so a heartbeat trigger pages when a customer goes
quiet, and holds when data is arriving but Retriever is behind. It runs beside Kafka, so the events topic,
Retriever, SLO processing, Service Maps and Anomaly Detection stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.honeycomb-kit
- **One slide:** [`LaserData_Honeycomb_IngestFreshness.pdf`](LaserData_Honeycomb_IngestFreshness.pdf)

## Why this use case

From Honeycomb's own incident reports and talks:

- The ingest service validates events and writes them to one Kafka topic; "enqueuing the message is necessary
  for Honeycomb to return a successful ingestion status for message batches." Retriever runs one shard per
  partition with two query nodes, and each "stores the Kafka-generated offset as a marker of its progress".
  SLO processing, Service Maps and Anomaly Detection read through consumer groups (Fred Hebert, "Honeycomb
  Incident Report: Kafka Maintenance on May 4 and 7, 2026", June 2026).
- In that cutover, ingestion continued but recent data was not queryable for about 25 minutes (EU) and 1.5
  hours (US); "triggers and SLOs did not behave as usual", and "some alerts would fire due to being
  “heartbeats”". One follow-up is a way to silence alarms during maintenance to avoid spurious missing-data
  alerts (same report).
- In December 2025, a DR-exercise cleanup left partitions leaderless; SLO processing could not see new data
  until metadata topics were repaired, and Retriever refused to process after offsets reset ("Incident Report:
  Exercises, Cleanups, and Evacuations", February 2026).
- "~100 partitions", "50k msgs/sec/partition", "We only typically read the last hour"; Basset runs triggers and
  SLOs on a schedule (Liz Fong-Jones and Terra Field, SREcon23 Americas).

The ingest service knows a batch has arrived the moment it is enqueued; a heartbeat that queries Retriever
knows only once Retriever has applied it. This kit puts both facts on one small lane, on its own broker, and
answers each dataset's heartbeat with fire, hold or ok. It prints the lane's p99 every second and checks
every answer against the truth.

## Run it

Needs Docker and Python 3.10+. Defaults: 40,000 datasets sending a batch a second (40,000 receipts/s), 100
Kafka partitions with two Retriever nodes each reporting progress twice a second, a 60 s heartbeat window, 5
datasets a second going quiet for two minutes, and Retriever behind on 30% of partitions for two minutes,
starting 90 s in.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python freshness.py &                # the freshness service heartbeats ask
python ingest.py                     # 40,000 datasets, verdicts checked
```

They print a line a second:

```
[freshness] 40,400 msgs/s  receipt-to-freshness p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
            datasets 40,000  heartbeats now: held …  firing …  verdicts sent …
[ingest]    40,000 receipts/s  datasets 40,000 (quiet …)  Kafka partitions behind on Retriever 30/100
            heartbeats: silences started …  fired right … (after the window +…s)  missed 0  spurious pages 0
            held while Retriever behind … (wrong 0)
```

`ingest.py` knows every batch it has enqueued and which partitions Retriever has stopped applying, so it
checks every verdict: was a fired heartbeat a real silence, was any silence missed, did any heartbeat page an
owner whose dataset was still sending, and was every held heartbeat a dataset that was sending while
Retriever was behind? Those are the business numbers; a slow lane shows up as spurious pages.

**What the numbers mean.** The kit times each receipt and progress report from the moment `ingest.py`
creates it to the moment `freshness.py` has applied it, through Python clients, on whatever machine you run
it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at about 3,370 messages/s
(3,000 datasets sending a batch a second, plus progress from two Retriever nodes on 100 Kafka partitions),
two green runs held p50 5.2 to 7.9 ms, p99 9.9 to 14.6 ms and p99.9 10.9 to 15.8 ms (median of 64
steady-state seconds each), with no second over the 50 ms budget. 3 MB/s of backfill on the topic next door
moved p99 by about 4%. With a 10 s heartbeat window, 2 datasets a second going quiet and Retriever behind on
30 of 100 Kafka partitions for 20 s, each run fired 124 silences, at most 0.60 to 0.61 s after the window
closed; none was missed; no heartbeat paged a dataset that was sending; and 269 heartbeats of datasets that
were sending while Retriever was behind were held (up to 264 at once), none of them wrongly. That is the
end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `ingest.py` | Datasets sending batches; a receipt per enqueued batch, keyed by dataset, with the Kafka partition and offset, and Retriever progress per partition, both on `freshness-lane`; quiet customers and a Retriever stall on purpose. Checks every verdict against the truth. |
| `freshness.py` | The freshness service: last enqueued and newest queryable time per dataset, rebuilt from the lane on start; answers each heartbeat with fire, hold or ok and publishes changes to `heartbeat-verdicts`. |
| `backfill_flood.py` | A neighbour on the same broker: event batches replayed after a cutover (16 KB records) on `backfill-events`. |
| `kafka_bridge.py` | Writes receipts from your events topic as a read-only consumer group, and progress from other consumer groups' committed offsets. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A cutover:** `python ingest.py --stall-share 1 --stall-at 90 --stall-for 300` (Retriever stops applying
   every partition for five minutes while ingest keeps accepting). Pass: every dataset that keeps sending is
   held, every quiet one still fires, none is missed.
2. **A backfill next door:** `python backfill_flood.py --mbps 100` while both run. Pass: the lane's p99 moves
   by less than 10% and no heartbeat fires on a dataset that is sending. The script prints the rate it
   actually reached; one Python process may not reach 100 MB/s on a laptop.
3. **Restart the freshness service:** stop `freshness.py` and start it again; it replays the retained lane
   (15 minutes) and rebuilds every dataset. Pass: it catches up within seconds and no later verdict is wrong.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <events-topic> \
  --partitions 0-9 --progress-group <slo-consumer-group> -X security.protocol=SSL
```

The bridge only reads, as its own consumer group `iggy-shadow`; the ingest service, Retriever and every
consumer group are untouched. `--progress-group` reads another group's committed offsets without joining it.
Retriever keeps its own offsets, so its progress needs a few lines in Retriever that send
`{kp, node, off}` to `freshness-lane`. Map your record in `to_dataset()`. Run it inside your AWS account,
next to your brokers.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one environment's receipts, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
