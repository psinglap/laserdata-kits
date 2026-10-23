"""Shadow your live game moments from Kafka into Apache Iggy, read-only.

Fubo's data stack streams on Apache Kafka and Google Pub/Sub (Fubo Data Engineering posting, 2026). Fubo has not
published which topics carry key-play detections, clip-ready events or replay reviews, so this bridge assumes only
that they land on one or more Kafka topics you name. It reads them as one more consumer group (`iggy-shadow`), so
producers and existing consumers are untouched, and writes each game's moments to its partition once per
--tick-ms, batched per partition. Point one game_alerts.py at Iggy and compare its pushes with what Game Alerts
sends today. If these events live on Pub/Sub instead, read them from a new subscription in pump() with
google-cloud-pubsub; the rest stays the same.

Map your records in to_moment(): the kit expects, per record, a game id, the kind (pitch, key, clip, review), the
play id for key, clip and review, and the resulting score. A per-game sequence is added here; the created time is
when the bridge received the record, so the latency game_alerts.py prints starts at the bridge, not at the
ballpark, the moment model or the clipper.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, connect, ensure_topic, pack, partition_of

seq = defaultdict(int)


def to_moment(value: bytes) -> tuple[str, bytes] | None:
    # Replace with your record layout. Example inputs:
    # {"gameId": 260907, "type": "key", "playId": 41, "team": "T14", "label": "go-ahead HR", "score": [3, 2], "frame": "T7"}
    # {"gameId": 260907, "type": "clip", "playId": 41, "clipId": "G260907-41", "lengthS": 24, "score": [3, 2]}
    try:
        r = json.loads(value)
        g, kind = f"G{int(r['gameId'])}", str(r["type"])
        body = {"f": str(r.get("frame", "")), "sc": r.get("score") or [0, 0]}
        if kind == "key":
            body.update(t=str(r["team"]), e=str(r["label"]), runs=int(r.get("runs", 0)))
        elif kind == "clip":
            body.update(c=str(r["clipId"]), len=int(r.get("lengthS", 0)))
        elif kind == "review":
            body.update(v="overturned" if r.get("overturned") else "upheld")
        elif kind != "pitch":
            return None
        play = int(r.get("playId", 0))
    except (ValueError, KeyError, TypeError):
        return None
    seq[g] += 1
    return g, pack(g, seq[g], kind, play, body)


async def main(a):
    from confluent_kafka import Consumer as KafkaConsumer

    iggy = await connect(a.url)
    await ensure_topic(iggy, LANE, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    pending = defaultdict(list)
    k = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                       "auto.offset.reset": "latest", "enable.auto.commit": True})
    topics = a.topics.split(",")
    k.subscribe(topics)
    print(f"[bridge] {', '.join(topics)} on {a.bootstrap} (group iggy-shadow) -> Iggy {STREAM}/{LANE}", flush=True)

    async def pump():
        loop = asyncio.get_running_loop()
        while True:
            recs = await loop.run_in_executor(None, lambda: k.consume(500, 0.05))
            for r in recs:
                if r.error() is None:
                    m = to_moment(r.value())
                    if m:
                        pending[partition_of(m[0])].append(SendMessage(m[1]))

    async def write(pid):
        while True:
            if pending[pid]:
                batch, pending[pid] = pending[pid], []
                await writers[pid].send_messages(STREAM, LANE, pid, batch)   # one batch per partition per tick
            await asyncio.sleep(a.tick_ms / 1000)

    await asyncio.gather(pump(), *(write(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topics", required=True, help="comma-separated topics your key plays, clips and reviews land on")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
