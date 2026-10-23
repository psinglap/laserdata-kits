# Real-time crash spike alerts for Measure, on LaserData

Every crash and ANR the ingest worker groups into an issue reaches the spike check in milliseconds, so an issue
that crosses its app's thresholds alerts on Slack and email within a second, not at the next hourly run. It runs
on the Apache Iggy server Measure's self-host stack already ships, as one more topic, so nothing else moves.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.measure-kit
- **One slide:** [`LaserData_Measure_CrashSpikes.pdf`](LaserData_Measure_CrashSpikes.pdf)

## Why this use case

From Measure's own repository and docs (read October 2026):

- **The rule.** "An alert is sent when a single issue occurs at least 100 times in an hour and those occurrences
  amount to at least 0.5% of that hour's sessions. Both thresholds can be changed per app." Once an issue alerts,
  it stays quiet for 7 days ([docs: Alerts](https://measure.sh/docs/alerts)).
- **The schedule.** `backend/alerts/main.go` runs `CreateCrashAndAnrAlerts` on cron `0 * * * *`, every hour, over
  the hour just ended (`errorSpikeTimePeriod = time.Hour`), counting each issue's fatal crashes or ANRs and the
  distinct sessions in ClickHouse.
- **The bus.** Since PR #3314 (April 2026) the ingest service publishes every `IngestBatch` to a message bus and
  the ingest worker processes it: Google Pub/Sub on Cloud, Apache Iggy on self-hosted installs (stream `measure`,
  topic `ingest-batch`, 9 partitions, in `self-host/compose.yml`). Step 7 of the worker, "Bucket Exceptions &
  ANRs", is where each crash and ANR gets its issue fingerprint.

Running the rule once an hour over the clock hour has two effects. A spike that starts at 10:05 reaches Slack at
11:00 at the earliest. And a spike that straddles the top of the hour can be split so neither hour reaches 100,
so it never alerts, although it passed 100 inside an hour.

This kit adds one lane: after step 7, the worker publishes one small record per batch (batch ID, app, session, and
the batch's fatal crashes and ANRs by fingerprint) to `issue-occurrences`, keyed by app. A spike check reads it
and applies the same rule, with the same thresholds and cooldown, over a sliding hour, on every batch. Batch IDs
it has already seen are skipped, as the worker's checkSeen skips a batch it already ingested, so a batch delivered
twice never counts twice. `ingest-batch`, the worker, ClickHouse and the hourly job are untouched; the hourly job
can keep running as a backstop.

Measure has not published its alert volume or how many apps hit the thresholds. The apps, sessions, issues and
spikes in this kit are illustrative; the rule is Measure's.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 apps (a few large, many small), 1,000 ingest batches a second from
400,000 concurrent sessions, background crashes and ANRs spread over 400 issues per app, 1% of records delivered
twice, Measure's thresholds (100, 0.5%, one hour, 7 days). Spike A: 3 crashes a second of one new issue in the
largest app, from minute 5 for 15 minutes. Spike B: 1,800 ANRs of one issue in the second-largest app over 20
minutes, centred on the top of an hour.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python spike_judge.py &              # the spike check, fed by the lane
python ingest_worker.py              # 200 apps: records out; Slack and email read the alerts
```

They print a line a second, and a line when a spike reaches Slack and at each hourly run:

```
[spike-check] … batches/s  batch-to-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  occurrences crash … anr …
        repeats skipped … (… total)  alerts crash … anr … sent …  in cooldown …  apps …  stall gc …ms loop …ms
        slowest batch->broker … ms, broker->check … ms
[ingest-worker] TRUTH spike A tips at batch …, … s after it began: 100 occurrences, … sessions, …%
[slack] ALERT Crash Spike: spike A (crash spike, …): at Slack … ms after the tipping batch; … s after the spike
        began; tip batch matches the truth: yes; 100 occurrences, … sessions, …%
[hourly] run at +… s: alerts ['A']; in its window: spike A … crashes, spike B … ANRs
[ingest-worker] … batches/s  …  spike A: Slack … s, hourly job +… s, after Slack before the hourly job …; spike B: …
```

"TRUTH" is the ingest worker's own record of every distinct batch, run through the same rule: the batch that tips
each issue over both thresholds. The alert must name that exact batch; if a repeat had been counted, it would tip
earlier. "Hourly job" applies Measure's job to the same batches at the top of each hour (cron's clock by default;
`--check-phase-s` for a short run), with its own cooldowns. "After Slack before the hourly job" counts the spike's
crashes that came after Slack had the alert and before the hourly job would have.

**What the numbers mean.** The kit times each record from the moment `ingest_worker.py` publishes it to the
moment `spike_judge.py` has applied it, and each alert from its tipping batch to the Slack and email senders,
through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (50 apps, 1,500 ingest batches a second from 20,000 concurrent sessions, 1% of
records sent twice, and the hour cut to 30 s so the run fits in a minute and a half: a 30 s window and an "hourly"
job every 30 s), three green runs held batch-to-check p50 2.1–4.2 ms, p99 4.1–7.6 ms and p99.9 6.0–10.1 ms (220 of
220 steady-state seconds inside 50 ms). With 2 MB/s of full ingest batches on `ingest-batch`, the lane's p99 went
from 4.0–7.2 ms to 4.1–7.4 ms (medians per second). Spike A (12 crashes a second, starting 2 s after an hourly run)
tipped 8.3 s after it began and reached Slack 3.0–7.9 ms after its tipping batch; the hourly job over the same
batches would have alerted 28.0 s after it began, and 68 of the spike's 168 crashes came in between. Spike B (128
ANRs over 16 s, half on each side of a run) tipped 12.5 s after it began and reached Slack 5.7–7.3 ms after its
tipping batch; the hourly job never alerted it. Both alerts named exactly the batch the ground truth names, with
1,255–1,283 repeated records skipped per run, and no other issue alerted, live or hourly. Spike A's alert came 19.7 s
sooner in a 30 s cycle; the same shape on Measure's hour is about 39 minutes sooner. That is the end-to-end loop through this demo code, not the broker
alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. In
production the ingest worker's own steps (symbolication, ClickHouse inserts) come before step 7 and add their time
before any of this.

| File | What it is |
|---|---|
| `ingest_worker.py` | 200 apps uploading ingest batches; after grouping, one record per batch to `issue-occurrences`, keyed by app, 1% twice; spikes A and B; the ground truth and the hourly job over the same batches; the Slack and email senders reading `spike-alerts`. |
| `spike_judge.py` | The spike check: Measure's rule per app over a sliding window, on every record; repeats skipped by batch ID; 7-day cooldown, restored from `spike-alerts` on restart; on start it reads back the window from the log. Prints the lane's p99. |
| `ingest_flood.py` | Full ingest batches on `ingest-batch`, the topic the ingest service already writes, for the isolation test. |
| `clickhouse_shadow.py` | Read-only shadow from the ClickHouse `events` table, for a trial on your own traffic without changing the worker. |
| `common.py` | Topics, partitioning by app ID, Measure's thresholds, the spike window. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## In the ingest worker

The production path is one publish after step 7, through Measure's own `libs/bus`:

```go
// once, at start-up
issues, err := bus.NewIggyProducer(addr, user, pass, "issue-occurrences", bus.DefaultStreamName, "issue-occurrences")
// after bucketing a batch: its ID, app, session, and fatal crashes and ANRs by fingerprint
err = issues.Publish(ctx, record)
```

One change is needed in `libs/bus`: today an Iggy producer takes one message key for its lifetime
(`WithIggyMessageKey`) and `PublishOrdered` ignores the key on Iggy, so routing each record by its app ID needs a
per-message key (or a partition computed from the app ID, as `common.part()` does). On Measure Cloud the same
record can go to Pub/Sub with the app ID as its ordering key, or to an Iggy topic on LaserData Cloud.

## Three tests

1. **Release day:** `python spike_judge.py --readers 4 &` and `python ingest_worker.py --batches 4000`.
   Pass: batch-to-check p99 under 50 ms, both spikes alert on their tipping batch, nothing else alerts.
2. **Uploads next door:** `python ingest_flood.py --mbps 20` while both run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart the spike check:** stop `spike_judge.py` and start it again. It reads back the last hour from the log
   and its cooldowns from `spike-alerts` before it judges.
   Pass: no spike missed, no issue alerted twice. (This one is not part of CI yet.)

## Shadow your own traffic

```
python clickhouse_shadow.py --clickhouse http://<clickhouse>:8123 --user <reader user> --password <secret>
```

It reads the rows the worker has inserted since its last read, one row per app and session with its fatal
crashes and ANRs (the same predicates as `backend/alerts`), and publishes them to `issue-occurrences`. It only
runs a SELECT; the SDKs, the ingest service, the bus, the worker and the alerts service are untouched. Compare the
spike check's alerts and their times with the `alerts` table. This path adds the read interval and ClickHouse's
insert path to every record, so it shows which spikes the lane catches and how much earlier, not the lane's own
latency.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9; Measure doesn't need it, since the bus already speaks
Iggy. Delivery is at-least-once, so the spike check skips repeated batch IDs, as the worker's checkSeen does.
Replicated clusters (Viewstamped Replication) shipped in September 2026; the self-host stack runs one node. The Go
SDK client cannot reconnect once the server drops its session, which is why `libs/bus` restarts the process
(Sep 2026). Storage is local disk per node, not object storage, so this is one
lane with an hour of state, not the crash history, which ClickHouse keeps.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one self-hosted install, alongside the hourly job. No
advantage, no migration. LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP
account (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

kranti@laserdata.com · laserdata.com
