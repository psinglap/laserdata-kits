"""The trust and safety classifiers, fed by the turns lane.

Reads the turns topic on its own connection and consumer group. Each record is one turn of one conversation
(the user's message and the character's reply, with the conversation's turn sequence), so a replayed or
repeated turn is recognised by its sequence and changes nothing.

For every turn it runs classify() (a stand-in for your input and output classifiers) and, when a turn is
flagged, publishes a verdict on safety-verdicts, which every chat server reads (chat_servers.py):
  * BLOCK: the content is taken out of the conversation, so the next prompt must not carry it;
  * LIFELINE: a self-harm reference, so the next reply shows the Lifeline pop-up.
Each verdict carries the conversation, the turn it covers and the conversation's running strike count
(the resulting value, never only "+1"), so a restarted classifier rebuilds every count from the log.

Prints turn-to-classifier p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the verdicts.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import BLOCK, DEFAULT_URL, LIFELINE, PARTITIONS, STREAM, TURNS, VERDICTS, Window, ensure_topics, load, msg


def classify(text: str) -> str | None:
    """Stands in for your trust and safety models. Here a flagged turn carries a tag the kit planted; put
    your classifier call here (in-process, or a batched call to your model servers)."""
    if text.startswith(f"[{LIFELINE}]"):
        return LIFELINE
    if text.startswith(f"[{BLOCK}]"):
        return BLOCK
    return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), TURNS, VERDICTS)
    turns = await (await own_client()).consumer_group(
        f"{a.name}-{TURNS}-{os.getpid()}", STREAM, TURNS, polling_strategy=PollingStrategy.First(),
        batch_length=5000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    out = await (await own_client()).producer(STREAM, VERDICTS, topic_partitions_count=PARTITIONS[VERDICTS])
    lat, stats = Window(), defaultdict(float)
    seq = {}                                # conversation -> last turn sequence applied
    strikes = defaultdict(int)              # conversation -> flagged turns so far
    pending = []                            # verdicts waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays turns from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    async def on_turn(m):
        d = load(m.payload())
        now = time.time_ns()
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest turn seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        c = d["c"]
        if d["seq"] <= seq.get(c, 0):
            stats["dropped"] += 1           # older or repeated turn: already classified
            return
        seq[c] = d["seq"]
        stats["turns"] += 1
        verdict = classify(d["text"])
        if verdict is None:
            return
        strikes[c] += 1
        stats[verdict] += 1
        if age > a.replay_verdict_s * 1000:
            return                          # replayed from long ago: count the strike, the next turn has long gone
        pending.append(SendMessage(msg(c=c, t=d["seq"], action=verdict, strikes=strikes[c])))

    async def send_verdicts():
        # Verdicts are small and come in bursts; one batch every 2 ms keeps them off a send-per-verdict loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of turns arrived inside the budget; partitions interleave,
                # so one fresh turn alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier turns this second, "
                      f"strikes rebuilt for {len(strikes):,} conversations", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} turns/s  turn-to-classifier p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  verdicts sent {int(stats['sent']):,} "
                  f"(block {int(stats[BLOCK]):,}, lifeline {int(stats[LIFELINE]):,})  "
                  f"conversations with 3+ strikes {sum(1 for v in strikes.values() if v >= 3):,}", flush=True)
            stats.clear()

    await asyncio.gather(turns.consume_messages(on_turn, asyncio.Event()), send_verdicts(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="classifiers")
    p.add_argument("--budget-ms", type=int, default=50, help="turn-to-classifier p99 budget for the lane")
    p.add_argument("--replay-verdict-s", type=float, default=30,
                   help="after a restart, still publish verdicts for turns younger than this")
    asyncio.run(main(p.parse_args()))
