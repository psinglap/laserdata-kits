"""Shadow your existing score or points topic from Kafka into Apache Iggy, read-only.

Reads the topic your scoring pipeline already produces to, as one more consumer group
(`iggy-shadow`), so producers and existing consumers are untouched. Writes each match's balls to
its partition once per --tick-ms. Point one points_node.py at Iggy and compare it with what your
points service sees today.

Map your records in to_ball(): the kit expects, per ball, a match id, the over, the event and
the resulting fantasy points of the players it touched. A per-match sequence is added here; the
created time is when the bridge received the record, so the latency points_node.py prints starts
at the bridge, not at the venue.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, connect, ensure_topic, pack, partition_of

seq = defaultdict(int)
slot = {}                                   # your match id -> the kit's M001, M002, … so points_node.py holds teams for it


def to_ball(value: bytes) -> tuple[str, bytes] | None:
    # Replace with your record layout. Example input:
    # {"matchId": "IPL-2026-41", "over": "17.3", "event": "SIX", "playerPoints": {"7": 62, "17": 14}}
    # with players keyed by their slot in the two playing XIs (0-10 one side, 11-21 the other).
    try:
        r = json.loads(value)
        m = slot.setdefault(str(r["matchId"]), f"M{len(slot) + 1:03d}")
        pts = [[int(p), v] for p, v in r["playerPoints"].items()]
    except (ValueError, KeyError, TypeError):
        return None
    seq[m] += 1
    return m, pack(m, seq[m] + 1, str(r.get("over", "")), str(r.get("event", "")), pts)   # seq 1 is the line-up


async def main(a):
    from confluent_kafka import Consumer as KafkaConsumer

    iggy = await connect(a.url)
    await ensure_topic(iggy, LANE, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    pending = defaultdict(list)
    k = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                       "auto.offset.reset": "latest", "enable.auto.commit": True})
    k.subscribe([a.topic])
    print(f"[bridge] {a.topic} on {a.bootstrap} (group iggy-shadow) -> Iggy {STREAM}/{LANE}", flush=True)

    async def pump():
        loop = asyncio.get_running_loop()
        while True:
            recs = await loop.run_in_executor(None, lambda: k.consume(500, 0.05))
            for r in recs:
                if r.error() is None:
                    b = to_ball(r.value())
                    if b:
                        pending[partition_of(b[0])].append(SendMessage(b[1]))

    async def write(pid):
        while True:
            if pending[pid]:
                batch, pending[pid] = pending[pid], []
                await writers[pid].send_messages(STREAM, LANE, pid, batch)
            await asyncio.sleep(a.tick_ms / 1000)

    await asyncio.gather(pump(), *(write(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topic", required=True, help="the topic your ball-by-ball scores or points are produced to")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
