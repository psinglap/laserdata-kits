"""Shadow your live commentary into Apache Iggy, read-only.

Cricbuzz has not published how its commentary moves between services, so this bridge assumes nothing about it:
it polls the commentary JSON your own commentary service already answers for each live match (an internal URL
you pass), every --every-ms, and writes each new line, and each line whose text or result changed, to the lane
as a ball or an edit, batched per partition. Nothing upstream is touched: no producer changes, no new consumer
group on anything you run. Point one commentary_node.py and match_alerts.py at Iggy and compare their feeds and
alerts with what your apps show.

Map your response in to_lines(): the kit expects, per line, a line id (or the ball number), over.ball, the event,
the text and the resulting score. The created time is when the bridge saw the line, so the latency the readers
print starts at the bridge, not at the desk. If your desk already writes to a Kafka topic, read that topic as one
more consumer group instead (the same pattern, with confluent-kafka) and drop the polling.
Needs: pip install apache-iggy==0.9.0 (standard library only for HTTP)
"""
import argparse
import asyncio
import json
import urllib.request
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, connect, ensure_topic, pack, partition_of

seq = defaultdict(int)                               # match -> last sequence written
seen = defaultdict(dict)                             # match -> {line id: (version, event, text)}


def to_lines(match: str, body: bytes) -> list:
    # Replace with your response layout. Example input:
    # {"score": [0, 168, 4, 105], "lines": [{"id": 214, "over": "17.3", "event": "SIX", "text": "..."}]}
    try:
        r = json.loads(body)
        score = r["score"]
        out = []
        for ln in r["lines"]:
            lid, ev, tx = int(ln["id"]), str(ln["event"]), str(ln["text"])
            held = seen[match].get(lid)
            if held is None:
                seen[match][lid] = (1, ev, tx)
                kind, ver = "ball", 1
            elif held[1:] != (ev, tx):
                seen[match][lid] = (held[0] + 1, ev, tx)
                kind, ver = "edit", held[0] + 1
            else:
                continue                             # unchanged since the last poll
            seq[match] += 1
            out.append(pack(match, seq[match], kind, lid, ver, str(ln.get("over", "")), ev, tx, score))
        return out
    except (ValueError, KeyError, TypeError):
        return []


async def main(a):
    iggy = await connect(a.url)
    await ensure_topic(iggy, LANE, PARTITIONS)
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    matches = a.matches.split(",")
    loop = asyncio.get_running_loop()
    print(f"[bridge] {len(matches)} live matches from {a.source} every {a.every_ms:g} ms -> Iggy {STREAM}/{LANE}", flush=True)

    def get(m):
        with urllib.request.urlopen(a.source.format(match=m), timeout=2) as resp:
            return resp.read()

    async def poll():
        while True:
            bodies = await asyncio.gather(*(loop.run_in_executor(None, get, m) for m in matches), return_exceptions=True)
            per = defaultdict(list)
            for m, b in zip(matches, bodies):
                if isinstance(b, bytes):
                    per[partition_of(m)] += [SendMessage(x) for x in to_lines(m, b)]
            await asyncio.gather(*(writers[p].send_messages(STREAM, LANE, p, batch) for p, batch in per.items() if batch))
            await asyncio.sleep(a.every_ms / 1000)

    await poll()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, help="internal commentary URL with {match}, e.g. http://commentary.internal/v1/{match}")
    p.add_argument("--matches", required=True, help="comma-separated live match ids")
    p.add_argument("--every-ms", type=float, default=500)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
