# Real-time delivery status for Netcore Cloud, on LaserData

Sent, Delivered and Read webhooks each get their own Apache Iggy topic, so Delivered counts reach
campaign reports in milliseconds, even during a Read surge. It runs beside your Pub/Sub topics, so
nothing else moves.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.netcore-cloud-kit
- **One slide:** [`LaserData_NetcoreCloud_DeliveryStatus.pdf`](LaserData_NetcoreCloud_DeliveryStatus.pdf)

## Why this use case

Netcore's own engineering blog describes the workload. In "Behind the Scenes at Netcore: Designing a
System for 435 Million Messages and 10,000+ TPS Events", Vishal Vyas (VP Engineering) describes a
campaign system that sent about 665 million messages in four days (about 200 million in a 13-hour
window), with the delivery engine at about 4,000 TPS and "~10,000 TPS" of inbound status webhooks,
load-tested above 20,000 TPS. Each message comes back as Sent, Delivered and Read events, and each
status type is its own stream on its own Pub/Sub topic: Delivered, the highest-priority signal, gets
dedicated consumer capacity so reporting stays current; Read, high-volume and less time-sensitive, is
isolated; each stream is throttled and batched on its own to protect the database cluster (at least
8,000 writes a second verified). In the post's words: "If a backlog occurs in the "Read" event stream,
it has zero impact on the processing of "Delivered" events."

So this kit is that lane on a durable log. Every status webhook goes to its status's topic, keyed by
message ID. The Delivered writer reads its own topic on its own connections, uncapped. The Read writer
is capped; above the cap, Read webhooks wait on the Read topic, on disk and replicated, and drain at
the cap. Each writer counts a status once per message (providers retry) and writes one row per changed
campaign every 250 ms. The delivery engine, its in-memory queues, Pub/Sub and every reporting consumer
you run today stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: 4,000 outbound messages a second across 200 campaigns, so
about 10,000 status webhooks a second (95% delivered within 0.3–3 s, 40% of those read within a
minute, 2% of webhooks sent twice).

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python reporter.py --status delivered &               # the Delivered writer, uncapped
python reporter.py --status read --max-rate 3000 &    # the Read writer, capped
python webhooks.py --surge-for 120                    # status webhooks; a Read surge 60 s in
```

Each writer prints a line a second, and the Read writer one more while it is held at its cap:

```
[delivered]  3,800 delivered statuses/s  webhook-to-reporter p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  report rows …/s  in report within 1 s 100.00%  retries dropped …  campaigns 200  total …
[read] held at its write cap: took 3,000/s (cap 3,000/s)  backlog … on status-read  oldest waiting … s
[read] back inside its write cap: backlog on status-read cleared … s after it was first held
```

The business numbers: `in report within 1 s`, the share of statuses that were in a campaign report
row within a second of the webhook; `report rows`, the row writes a second the database sees;
`retries dropped`, provider retries counted once; and for Read during a surge, its backlog, its oldest
wait and how long it took to clear.

**What the numbers mean.** The kit times each status webhook from the receiver in `webhooks.py` to
the writer in `reporter.py`, through Python clients, on whatever machine you run it on. On a shared
2-vCPU GitHub runner, with the broker, the receiver and two writers on one box, at 1,000 outbound
messages a second across 40 campaigns (about 2,400 webhooks a second, 5,000 during the surge), five
CI runs held a Delivered p50 of 4.1 to 6.6 ms, p99 of 8.3 to 11.8 ms and p99.9 of 8.6 to 12.5 ms
(medians per second) over 65 steady-state seconds each, with no second over the 100 ms budget (worst
second 18.0 ms), and 100% of Delivered statuses in the report within 1 s. While 2,500 extra Read
webhooks a second arrived for 12 s against a 1,500/s cap, Delivered kept a median per-second p99 of
8.4 to 12.0 ms (8.2 to 11.7 ms otherwise); the Read backlog peaked at 15,992 to 16,281 webhooks on
status-read, its oldest webhook waited 11.3 to 11.4 s, and it cleared 26.0 s after it was first held. About 950
Delivered webhooks a second became 149 to 150 report rows a second, and 18 to 20 provider retries a
second were dropped. That is the end-to-end lane through this demo code, not the broker alone, and it
does not include the providers, the webhook endpoint's own work or the database. The broker's own tail
latency and cost per billion webhooks are measured with `iggy-bench` on dedicated machines
(`bench/PLAN.md`).

| File | What it is |
|---|---|
| `webhooks.py` | The status webhook receiver: Sent, Delivered and Read for every outbound message onto `status-sent`, `status-delivered` and `status-read`, keyed by message ID, with provider retries; `--surge-*` runs a Read surge on the largest campaign. |
| `reporter.py` | One campaign reporting writer per status. Reads its topic's partitions directly, counts each status once per message, writes one row per changed campaign every 250 ms; `--max-rate` caps it. |
| `status_bridge.py` | Copies one of your status topics into its Iggy topic, read-only: a Pub/Sub shadow subscription, or a Kafka consumer group `iggy-shadow`. |
| `common.py` | Topics, partitions, the partition function and retry dedup, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, Read surge and cost per billion webhooks included; rules fixed before results. |

## Three tests

1. **A broadcast opened at once:** `python webhooks.py --surge-for 600` (Read at
   about 5× its normal rate). Pass: Delivered keeps p99 under 100 ms and 100% in the report within
   1 s; the Read backlog grows on `status-read` only and clears at the cap after the surge.
2. **Your load test's rate:** `python webhooks.py --send-rate 8000` (above 20,000 webhooks a second)
   with `reporter.py --status delivered --readers 4`. Pass: Delivered p99 holds, and report rows
   grow with the number of campaigns, not with the number of webhooks.
3. **Restart the Delivered writer:** stop it and start it again. It prints "catching up" while it
   replays from the first retained offset, rebuilds every campaign's count from the log and counts no
   message twice. Pass: back under budget once it reaches the live edge.

## Shadow your own traffic

```
gcloud pubsub subscriptions create iggy-shadow-delivered --topic=<your Delivered topic> --message-retention-duration=1h
pip install google-cloud-pubsub
python status_bridge.py --from pubsub --project <gcp-project> --subscription iggy-shadow-delivered --status delivered
python reporter.py --status delivered                # compare its counts with your campaign report
```

A Pub/Sub topic delivers every message to every subscription, so the shadow subscription leaves the
webhook endpoint and every reporting consumer untouched. If a status stream lives in Kafka, use
`--from kafka --bootstrap <brokers> --topic <topic>` (`pip install confluent-kafka`); it joins as its
own consumer group, `iggy-shadow`. Map your record shape in `to_msg()`: the kit needs the message ID
and the campaign.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored status webhooks for a set of
campaigns, alongside Pub/Sub, priced per billion webhooks. No advantage, no migration.

kranti@laserdata.com · laserdata.com
