# Real-time Imbi notifications for AWeber, on LaserData

Every deployment and mention reaches slackbot and email in milliseconds, posted once, and a reader that stops
moving is flagged within seconds. It runs on the Iggy server Imbi already has, beside the ClickHouse sink path,
so nothing else moves.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.aweber-kit
- **One slide:** [`LaserData_AWeber_ImbiNotifications.pdf`](LaserData_AWeber_ImbiNotifications.pdf)

## Why this use case

From Imbi's [ADR 0019](https://github.com/AWeber-Imbi/imbi/blob/main/docs/api/adr/0019-apache-iggy-message-streaming.md)
(Sep 2026, accepted): "Imbi has no durable message stream." Three kinds of work need one: analytics writes to
ClickHouse, metrics, and notifications, "events other Imbi services react to, such as a deployment finishing or a
user being mentioned, fanned out to slackbot, email and whatever comes next". Iggy was chosen because "it runs as
a single binary with local storage, which suits Imbi's deployment size better than a Kafka cluster would". The
ClickHouse writes moved first, through the official ClickHouse sink. The `notifications` stream (topics
`deployment`, `mention`, `digest`) is named for later: "The consumer side for those is a separate decision." The
ADR also notes that "Delivery is at-least-once".

From [apache/iggy #4273](https://github.com/apache/iggy/issues/4273) (Gavin M. Roy, Sep 2026): on
`0.9.0-edge.8`, a consumer group kept a member whose process was gone and held the only partition "for six days
and across a full server restart"; live members polled and got nothing, "indistinguishable from an idle topic,
which is why it went unnoticed for six days". [#4279](https://github.com/apache/iggy/issues/4279) adds: "Group lag
over HTTP would have shown the problem at once."

For analytics rows a silent stop costs a backfill. For notifications it costs trust: a quiet deploy channel looks
like a week without deploys. So this kit builds the notifications stream the ADR names, with the two things that
matter for it: each notification posted once despite at-least-once delivery, and a watchdog that flags a reader
whose position stops moving.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 notifications a second (60% deployments over 2,000 projects and
three environments, 40% mentions over 600 users), a 5× release wave 20 s in, and 1% of notifications published
twice. Imbi's notification volume is not published; these rates are illustrative and well above what #4273's
topics suggest.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python notify.py --reader slackbot & # every deployment and mention
python notify.py --reader email &    # production deploys and mentions
python watchdog.py &                 # lag per reader, STALL and SILENT flags
python imbi_api.py                   # Imbi's services: notifications out, 1% twice
```

They print a line a second:

```
[slackbot]    200 notifications/s  posted 200  publish-to-slackbot p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
              duplicates suppressed 2 (… total)  posted twice 0  missing 0  stall gc 0ms loop 0ms
[watchdog] email lag 0 moving  slackbot lag 0 moving  stalls flagged 0
[imbi-api]    200 notifications/s  deployment 120  mention 80  resent 2
```

**What the numbers mean.** The kit times each notification from the moment `imbi_api.py` publishes it to the
moment `notify.py` has applied it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU
CI runner with the broker and all scripts on one box (200 notifications/s with a 5× release wave for 10 s, 1%
published twice, 2,000 analytics rows/s, about 3.5 MB/s of webhook rows, on the same server from 30 s), two runs
on the final code held publish-to-reader p50 3.5–3.6 ms, p99 6.5–6.6 ms and p99.9 6.8–6.9 ms for both slackbot
and email (medians of the per-second values), with a worst single-second p99 of 7.4 ms; slackbot passed 65 of 65
steady-state seconds inside 50 ms and email 55 of 55 in each run. With the analytics rows on the same server, the
lane's p99 moved by 0.3 ms or less. Each reader skipped 240–247 re-sent notifications per run and posted none
twice; none went missing. When email was stalled for 8 s, the watchdog flagged it 3.2 s after its position
stopped, with 668–679 notifications waiting, and nothing else was flagged; email worked off the 1,614 waiting
notifications in its first second back, from its own position, with nothing missing or posted twice. That is the
end-to-end path through this demo code at reduced scale, not the broker alone. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `imbi_api.py` | Imbi's services as producers: deployments (keyed by project, with environment, entry type and version, as in `operations_log`) and mentions (keyed by user) on the `notifications` stream, each with its key's sequence, one batch per partition per 10 ms tick. 1% are published twice; `--wave-*` sets a release wave. |
| `notify.py` | slackbot or email. Reads every lane partition directly on its own connection from its own position, posts each sequence once (a re-sent copy is skipped, a gap counts as missing), reports its position once a second, and resumes from that position after a restart. `--stall-at/--stall-for` makes it poll and apply nothing for a while. |
| `watchdog.py` | Compares each reader's reported position with the topics' current offsets. Flags STALL when a reader with work waiting has not moved for 3 s, SILENT when it stops reporting, RECOVERED when it moves again. |
| `analytics_flood.py` | Imbi's ClickHouse-bound rows (`events/gateway`, `operations_log/deployments`) on the same server, the way `imbi.common.iggy` publishes them, for the isolation test. |
| `iggy_bridge.py` | Shadow run on real traffic: reads `operations_log/deployments` from Imbi's Iggy server read-only and turns completed deployments into notifications. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A release wave:** `python imbi_api.py --wave-at 20 --wave-x 5 --wave-s 10`.
   Pass: each reader's p99 under 50 ms, nothing missing, nothing posted twice.
2. **Analytics rows next door:** `python analytics_flood.py --rate 2000 --size 2048` while the lane runs.
   Pass: the lane's p99 stays inside its budget.
3. **Stall a reader:** `python notify.py --reader email --stall-at 30 --stall-for 8`.
   Pass: the watchdog flags STALL within a few seconds and RECOVERED after; email comes back from its own
   position with nothing missing and nothing posted twice; slackbot is never flagged.

## Design notes

- A stream per subject, topics as its dimension, as ADR 0019 sets out. Nothing about the analytics streams or the
  ClickHouse sink changes.
- Keyed partitions with an explicit partition id (`crc32(key) % 4`), which the Python SDK supports today, keep one
  project's deploy and rollback in order.
- Readers poll explicit partitions instead of joining a consumer group, so no group membership on the server can
  leave them polling nothing (the #4273 failure; its fix, apache/iggy #4283, merged after the 0.9.0 release).
- The Python SDK in 0.9.0 cannot read a group's stored offset, so each reader reports its own position on the lane
  (`notification_readers/positions`) and the watchdog compares it with `get_topic` offsets.
- A restarted reader rebuilds its sequence table from the log before its last reported position without posting.
  Notifications it posted in its last second before a crash can be posted again; key your Slack or email call on
  the notification id if that matters.

## Shadow your own traffic

```
python iggy_bridge.py --source-url "iggy+tcp://USER:PASS@<imbi iggy>:8090?nodelay=true" --from now
python notify.py --reader slackbot
python watchdog.py
```

The bridge reads `operations_log/deployments` as its own plain consumer, `notifications-shadow`, and holds its
position in memory. It never joins the ClickHouse sink's consumer group or stores an offset there. Rows are
published twice in Imbi (open, then completed), so it notifies once per completed id; check that rule against your
rows.

## Next step

If the numbers are interesting: a 2–4 week evaluation on Imbi's notifications stream, alongside the ClickHouse
sink. No advantage, no migration.

kranti@laserdata.com · laserdata.com
