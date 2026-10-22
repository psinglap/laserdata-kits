"""Shadow your domain verdicts into the Apache Iggy verdict lane, read-only.

Reads verdicts from where your Fraud Detection Solution already writes them and copies each one to the lane,
keyed by domain, with the resulting verdict and a per-domain sequence (the same shape verdicts.py writes).
It never writes back and never sits in the lookup path: resolvers, the SMS link scanner and the database
refresh stay exactly as they are.

Two sources:
  * --source file (default): tail a JSON-lines file your scorer or policy engine appends to (one verdict per
    line), e.g. an export of partner API scores and SMS link scan hits. Start at its end, or with
    --from-start to replay it.
  * --source kafka: a topic your verdicts already ride, read through its own consumer group "iggy-shadow"
    (a new group gets its own copy of every message; your consumers and producers are untouched).
    Needs: pip install confluent-kafka.
Map your fields with --domain-field, --verdict-field and --score-field, or edit to_record(). A verdict value of
block, blocked, malicious, phishing, fraud, true or 1 counts as block; anything else as clean.
Point resolver.py at Iggy with --replay-s 0 and compare its counts with your resolvers' block logs. In a
shadow run the clock starts when the bridge reads the record, so only the lane is timed.
"""
import argparse
import asyncio
import json
import time

from apache_iggy import SendMessage

from common import BLOCK, CLEAN, DEFAULT_URL, PARTITIONS, STREAM, VERDICTS, connect, ensure_topic, msg, part

BLOCK_WORDS = {"block", "blocked", "malicious", "phishing", "fraud", "true", "1"}


def to_record(line: bytes, a):
    d = json.loads(line)
    k = str(d[a.domain_field]).strip().lower().rstrip(".")
    v = BLOCK if str(d.get(a.verdict_field, "")).strip().lower() in BLOCK_WORDS else CLEAN
    sc = float(d.get(a.score_field) or (1.0 if v == BLOCK else 0.0))
    return k, v, sc, str(d.get(a.source_field) or "api")


async def file_lines(a):
    with open(a.path, "rb") as f:
        if not a.from_start:
            f.seek(0, 2)
        while True:
            line = f.readline()
            if line.strip():
                yield line
            elif not line:
                yield None                       # idle: let the caller send what it has read
                await asyncio.sleep(0.005)


async def kafka_lines(a):
    from confluent_kafka import Consumer as KafkaConsumer
    k = KafkaConsumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "enable.auto.commit": True,
                       "auto.offset.reset": "latest"})
    k.subscribe([a.topic])
    loop = asyncio.get_running_loop()
    try:
        while True:
            m = await loop.run_in_executor(None, k.poll, 0.2)
            yield m.value() if m is not None and not m.error() else None
    finally:
        k.close()


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, VERDICTS)
    writers = [await connect(a.url) for _ in range(PARTITIONS[VERDICTS])]
    state, pending = {}, [[] for _ in writers]          # domain -> [verdict, seq]; per-partition batches
    n, flips, bad, last = 0, 0, 0, time.monotonic()
    print(f"[bridge] {a.source} -> Iggy {STREAM}/{VERDICTS}", flush=True)

    async def flush():
        sends = [writers[p].send_messages(STREAM, VERDICTS, p, b) for p, b in enumerate(pending) if b]
        if sends:
            await asyncio.gather(*sends)
        for b in pending:
            b.clear()

    lines = file_lines(a) if a.source == "file" else kafka_lines(a)
    flushed = time.monotonic()
    async for line in lines:
        if line is None:
            if any(pending):
                await flush()
                flushed = time.monotonic()
            continue
        try:
            k, v, sc, src = to_record(line, a)
        except (ValueError, KeyError, TypeError):
            bad += 1
            continue
        s = state.setdefault(k, [None, 0])
        flips += s[0] is not None and s[0] != v
        s[0], s[1] = v, s[1] + 1
        pending[part(k)].append(SendMessage(msg(k, s[1], v, sc, src)))
        n += 1
        if time.monotonic() - flushed >= a.flush_ms / 1000 or sum(map(len, pending)) >= 1000:
            await flush()                                # one batch per partition every few ms
            flushed = time.monotonic()
        if time.monotonic() - last >= 1:
            print(f"[bridge] {n:,} verdicts/s copied, {flips:,} flips, {bad:,} unreadable, {len(state):,} domains",
                  flush=True)
            n, flips, bad, last = 0, 0, 0, time.monotonic()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--source", choices=("file", "kafka"), default="file")
    p.add_argument("--path", default="verdicts.jsonl", help="file source: the JSON-lines file to tail")
    p.add_argument("--from-start", action="store_true", help="file source: replay the file from the start")
    p.add_argument("--bootstrap", default="localhost:9092", help="kafka source: bootstrap servers")
    p.add_argument("--topic", default="domain-verdicts", help="kafka source: the topic your verdicts ride")
    p.add_argument("--domain-field", default="domain")
    p.add_argument("--verdict-field", default="verdict")
    p.add_argument("--score-field", default="score")
    p.add_argument("--source-field", default="source")
    p.add_argument("--flush-ms", type=float, default=5, help="send what has been read at least this often")
    asyncio.run(main(p.parse_args()))
