# Ditto CDC Data Bridges to every backend system, on LaserData

Every Big Peer document change reaches every downstream system in milliseconds, in per-document order, even when
a landing wave of devices reconnects at once. It runs beside your Kafka bridges and webhooks, so nothing in Big
Peer has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Ditto_CDCDataBridge.pdf`](LaserData_Ditto_CDCDataBridge.pdf)

## Why this use case

From Ditto's own documentation: Big Peer's Change Data Capture publishes each Data Bridge (one collection and a
DQL filter) to a Kafka topic, or to a webhook. "CDC guarantees event ordering at the individual document level"
and not across documents; consumers deduplicate by `txnId` and `_id`, since "no two events for a given document
will share the same txnId"; several consumer groups read one bridge independently; the default is 12 partitions,
and changing the count "is disruptive". A new bridge begins "from the time of creation", with no history, and
when the CDC producer falls behind it sends `requeryRequired`, after which the consumer re-queries the dataset
over the HTTP API. On self-hosted Ditto Server, the Operator runs Strimzi Kafka for Big Peer's transaction log.

This kit is an Apache Iggy Data Bridge lane with exactly those semantics: Ditto's event shape (`documentChanged`
upsert, remove, snapshot; `requeryRequired`), keyed by `_id` on 12 partitions, read by several downstream mirrors,
each at its own offset. Each mirror checks every change chain (an upsert's `oldValue` must be the version it
holds), drops repeats by txnId, and a mirror added later rebuilds the whole collection from the log instead of a
full requery. The traffic is a crew-app collection, `cabin_orders` (one document per flight and seat), with a
landing wave: crew devices reconnect at the gate and flush what they edited offline. The collection, rates and
wave are our illustration; Ditto publishes no CDC volume or latency figures.

## Run it

Needs Docker and Python 3.10+. Defaults: 200,000 documents, 2,000 changes a second, 0.5% of events delivered
twice, 2% removes.

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python big_peer.py &                       # one Data Bridge: snapshot, then live changes
python mirror.py --name ops-dashboard &    # a downstream system
python mirror.py --name catering           # another, at its own offset
```

They print a line a second:

```
[big-peer]  2,000 changes/s (steady)  txnId …  inserts …  removes …  re-sent …  tick late max …ms  send max …ms
[ops-dashboard]  2,000 changes/s  change-to-mirror p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  repeats dropped …
                 removes …  chains broken 0 (total 0)  requery 0  mirror … docs  read pass max …ms
```

**What the numbers mean.** The kit times each change from the moment `big_peer.py` creates the CDC event to the
moment a `mirror.py` process has applied it, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU GitHub runner with the broker and all five scripts on one box (30,000 documents, 1,000 changes/s, a
3× landing wave to 3,000/s for 15 s, two mirrors from the start, a third added at second 40, 2 MB/s of bulk export
next door), the medians of the steady-state seconds over three green runs were p50 4.3–6.7 ms, p99 7.9–12.5 ms
and p99.9 14.9–17.7 ms on each mirror. All 59 steady-state seconds per mirror were inside the 100 ms budget; the
worst second's p99 was 20.3–34.3 ms. No mirror broke a change chain; each dropped about 490 repeated events by
txnId; every mirror saw the `requeryRequired`. The mirror added at second 40 rebuilt the collection from 88,000–
92,000 events in the log in 0.8–1.4 s and was then inside the budget every second. That is the end-to-end lane
through this demo code, not the broker alone. One Python reader tops out near 4,000 changes/s on that runner;
past it, latency climbs because the reader falls behind, not the broker. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `big_peer.py` | Plays one Data Bridge: a snapshot of the collection, then live changes as Ditto CDC events, keyed by `_id`, with rising txnIds. `--wave` plays a landing wave on the seats of flights that just reached the gate; `--dup-share` re-sends events; `--requery-at` sends `requeryRequired`. |
| `mirror.py` | A downstream system: reads every partition directly at its own offset, rebuilds from the log on start, applies by txnId per `_id`, checks change chains, counts repeats, removes and requery signals. |
| `bulk_export.py` | A heavy, latency-tolerant stream (historical export to analytics) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies one of your Data Bridge topics into Iggy as `<prefix>-iggy-shadow` over mTLS, read-only, for a shadow run on real changes. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A landing wave:** `python big_peer.py --wave 5 --wave-at 60 --wave-for 120 --landed 50`.
   Pass: change-to-mirror p99 under 100 ms on every mirror through the wave, and zero chains broken.
2. **Bulk export at full volume:** `python bulk_export.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Add a downstream system:** start another `mirror.py` an hour in.
   Pass: it rebuilds every document from the log, with no requery, and is inside the budget the second after.

## Shadow your own Data Bridge

```
pip install confluent-kafka
openssl pkcs12 -in ca.p12 -nokeys -out ca.pem -passin pass:<cluster certificate password>
python kafka_bridge.py --endpoint <host>:443 --topic <bridge topic> --group-prefix <consumer group prefix> \
  --ca-pem ca.pem --user-p12 user.p12 --user-password <user certificate password>
```

The values come from the portal's "Kafka consumer info" or, on Ditto Server, from the `BigPeerDataBridge`
resource's `status.kafka`. The bridge reads as its own consumer group and never commits offsets; the Data Bridge
and every consumer you run today are untouched. Events are copied unchanged, keyed by `_id`; a `requeryRequired`
goes to every partition so every mirror sees it.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; mirrors use the Apache
Iggy SDK. Delivery is at-least-once; txnId per `_id` makes repeats safe, as it already does for Ditto CDC.
Replicated clusters shipped in September 2026, which is why the evaluation kills a leader on purpose. A native
Data Bridge type that writes to Iggy from Big Peer itself would be Ditto's to build; the kit bridges from Kafka.

LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure,
run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored Data Bridge, beside Big Peer and your Kafka
bridges. No advantage, no migration.

kranti@laserdata.com · laserdata.com
