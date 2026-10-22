"""Shadow your existing score topic from Kafka into Apache Iggy, read-only.

Reads the topic your score updates are already produced to, as one more consumer group (`iggy-shadow`), so
producers and existing consumers are untouched. Writes each match's updates to its partition once per
--tick-ms, batched per partition. Point one scorecard_node.py at Iggy and compare its keys and scores with
what scorecardPoll answers today.

Map your records in to_update(): the kit expects, per update, a match id, the sport, what happened and the
resulting score of both sides. A per-match key is added here when your record has none; the created time is
when the bridge received the record, so the latency scorecard_node.py prints starts at the bridge, not at the
venue or the data provider.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, connect, ensure_topic, pack, partition_of

key = defaultdict(int)


def to_update(value: bytes) -> tuple[str, bytes] | None:
    # Replace with your record layout. Example input:
    # {"matchId": 120001, "sport": "cricket", "event": "17.3 SIX", "score": [[168, 4, 105], [0, 0, 0]], "key": 418}
    try:
        r = json.loads(value)
        m = f"FC{int(r['matchId'])}"
        k = int(r["key"]) if "key" in r else key[m] + 1
    except (ValueError, KeyError, TypeError):
        return None
    key[m] = k
    return m, pack(m, str(r.get("sport", "")), k, str(r.get("event", "")), r.get("score"))


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
                    u = to_update(r.value())
                    if u:
                        pending[partition_of(u[0])].append(SendMessage(u[1]))

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
    p.add_argument("--topic", required=True, help="the topic your live score updates are produced to")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
