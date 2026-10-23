"""Shadow one state's account and wallet events from Kafka (Confluent Cloud) into the wallet lane at nj-1, read-only.

Reads a topic that already carries account and wallet events as one more consumer group, `iggy-shadow`, so the
producers and every existing consumer are untouched. Keeps only events accepted in --state, writes each account's
events to its partition once per --tick-ms, batched per partition. Run site_mirror.py and standby.py as usual and
compare the replica's balances with your system of record.

Map your records in to_event(): the kit expects, per record, the account, what happened, the amount, and the
account's balance and open bets after it, plus the state it was accepted in. A per-account sequence number is
counted here when the record has none. The accepted time is when the bridge received the record, so the latency
standby.py prints starts at the bridge, not at the bet slip.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import SendMessage

from common import LANE, PARTITIONS, PRIMARY, PRIMARY_URL, STATE, STREAM, connect, ensure_topic, pack, partition_of

seq = defaultdict(int)
skipped = [0]


def to_event(key: bytes | None, value: bytes, state: str) -> tuple[str, bytes] | None:
    # Replace with your record layout (decode Avro or protobuf here if that is what the topic carries).
    # Example (JSON for the demo):
    # {"accountId": "…", "state": "NJ", "type": "bet.placed", "betId": "…", "amountCents": 2500,
    #  "balanceCents": 18250, "openBets": 3}
    try:
        r = json.loads(value)
        acct = (key.decode() if key else None) or str(r["accountId"])
        st = str(r["state"]).upper()
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        return None
    if st != state:
        skipped[0] += 1                                      # another state's event: it stays in that state
        return None
    s = int(r["seq"]) if "seq" in r else seq[acct] + 1
    seq[acct] = s
    return acct, pack(acct, s, str(r.get("type", "")), str(r.get("betId", "")), int(r.get("amountCents", 0)),
                      int(r.get("balanceCents", 0)), int(r.get("openBets", 0)), PRIMARY, state=st)


async def main(a):
    from confluent_kafka import Consumer as KafkaConsumer

    iggy = await connect(a.url)
    await ensure_topic(iggy, LANE, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    pending = defaultdict(list)
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
            "auto.offset.reset": "latest", "enable.auto.commit": True}
    for kv in a.X:                                       # e.g. -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN
        k_, v_ = kv.split("=", 1)
        conf[k_] = v_
    k = KafkaConsumer(conf)
    k.subscribe([a.topic])
    print(f"[bridge] {a.topic} on {a.bootstrap} (group iggy-shadow), {a.state} only -> Iggy {STREAM}/{LANE} at "
          f"{PRIMARY}", flush=True)

    async def pump():
        loop = asyncio.get_running_loop()
        while True:
            recs = await loop.run_in_executor(None, lambda: k.consume(500, 0.05))
            for r in recs:
                if r.error() is None:
                    e = to_event(r.key(), r.value(), a.state)
                    if e:
                        pending[partition_of(e[0])].append(SendMessage(e[1]))

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
    p.add_argument("--topic", required=True, help="the topic carrying account and wallet events")
    p.add_argument("--state", default=STATE)
    p.add_argument("-X", action="append", default=[], help="extra librdkafka setting, key=value (SASL for Confluent Cloud)")
    p.add_argument("--tick-ms", type=float, default=2)
    p.add_argument("--url", default=PRIMARY_URL)
    asyncio.run(main(p.parse_args()))
