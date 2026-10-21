"""Shadow your enriched ad events from Kafka into the Apache Iggy frequency-cap lane, read-only.

Runs as its own Kafka consumer group ("iggy-shadow") on the topic your ads event publisher writes enriched ad
events to, so the publisher, every producer and every existing consumer (frequency capping, metrics,
sessionizer, billing, measurement partners) are untouched. It keeps impressions only, drops a repeat of an
event ID it has already seen, keeps a running count per (profile, campaign, cap window) and publishes the
resulting total with a sequence, keyed by profile, the same shape ad_server.py writes. Start ad_server.py
processes with --rate 0 --live-profiles 0 to read the lane only, and compare their counts with your
frequency-capping store.

Map your record in to_impression(): the kit needs the profile (or whatever you cap on), the campaign or line
item, the event type and the event ID. The defaults read JSON with fields profile_id, campaign_id,
event_type and event_id. In a shadow run only the lane is timed: the clock starts when the bridge reads the
record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SSL -X ssl.ca.location=...
"""
import argparse
import asyncio
import json
from collections import OrderedDict, defaultdict

from apache_iggy import Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, FCAP, PARTITIONS, STREAM, cap_window, connect, ensure_topics, msg, part


def to_impression(value: bytes, a):
    d = json.loads(value)
    if str(d.get(a.type_field, "")).lower() != a.impression_type:
        return None
    return str(d[a.profile_field]), str(d[a.campaign_field]), str(d[a.id_field])


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, FCAP)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq, n, seen = defaultdict(int), defaultdict(int), OrderedDict()
    P = PARTITIONS[FCAP]
    while True:
        w = cap_window(a.window_s)
        batches = defaultdict(list)
        for rec in kafka.consume(num_messages=1000, timeout=0.001):
            if rec.error():
                continue
            try:
                imp = to_impression(rec.value(), a)
            except (KeyError, ValueError):
                continue                    # not a record this mapping understands
            if imp is None:
                continue                    # not an impression
            p, c, eid = imp
            if eid in seen:
                continue                    # a repeat of an event already published: count it once
            seen[eid] = None
            if len(seen) > a.dedup_ids:
                seen.popitem(last=False)
            seq[p] += 1
            n[(p, c, w)] += 1
            kind = "L" if c in a.live_campaign else "V"
            batches[part(p, P)].append(SendMessage(msg(s="bridge", p=p, c=c, w=w, seq=seq[p], n=n[(p, c, w)], k=kind)))
        # One batch per partition, all sent at once, so a profile's impressions stay in order.
        if batches:
            await asyncio.gather(*(iggy.send_messages(STREAM, FCAP, Partitioning.partition_id(q), ms)
                                   for q, ms in batches.items()))
        if len(n) > 2_000_000:
            for k in [k for k in n if k[-1] < w]:
                del n[k]
        await asyncio.sleep(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True, help="a broker of your Kafka cluster")
    ap.add_argument("--kafka-topic", default="ads-enriched-events", help="your enriched ad events topic")
    ap.add_argument("--profile-field", default="profile_id")
    ap.add_argument("--campaign-field", default="campaign_id")
    ap.add_argument("--type-field", default="event_type")
    ap.add_argument("--impression-type", default="impression")
    ap.add_argument("--id-field", default="event_id")
    ap.add_argument("--live-campaign", action="append", default=[], help="a live-event sponsor campaign ID (repeatable)")
    ap.add_argument("--dedup-ids", type=int, default=5_000_000, help="recent event IDs remembered for de-duplication")
    ap.add_argument("--window-s", type=float, default=86_400, help="cap window in seconds; a day by default")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(ap.parse_args()))
