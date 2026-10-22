"""The Soundbox announcement service, fed by the payment confirmations lane.

Reads every partition of payment-confirmations directly, on --readers connections of its own, tracking its own
position and storing it on the broker at most once a second (no consumer group: a group reader visits one
partition per poll, so with 16 partitions a payment waits for its partition's turn). Each record is one status
event for one Soundbox, with the time the customer paid (vt), the device's event sequence and its resulting
state (today's collections and payment count), so:
  * a repeated confirmation (a status callback delivered twice) has a sequence already applied: it changes
    nothing, and the speaker says the amount once;
  * a confirmation older than common.FRESH_S since the customer paid is not announced: the total is updated
    quietly and the passbook gets it. The confirmation step already keeps late confirmations off this lane;
    this check holds even if it did not.

Announcements (stand-ins for yours; put your own in rules()): UPI and card payments received, with the amount,
the device's language and the day's running total and count. They go to soundbox-announcements, which the
device push (to the Soundbox over your IoT path), the business app and merchant support read; payments.py
plays them. Sent in small batches from their own task, never from the read loop.

Prints confirmation-to-announcer p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ANNOUNCE, ANNOUNCE_KINDS, CARD, DEFAULT_URL, FRESH_S, LANE, PARTITIONS, STREAM, UPI, Stalls,
                    Window, ensure_topics, load, msg)


def rules(d: dict, now_ns: int):
    """Returns the announcement kind for a status event the merchant should hear now, or None."""
    if d["k"] not in ANNOUNCE_KINDS:
        return None
    if now_ns - d["vt"] > FRESH_S * 1e9:
        return None                         # the customer paid too long ago: total and passbook only
    return d["k"]


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, ANNOUNCE)
    out = await (await own_client()).producer(STREAM, ANNOUNCE, topic_partitions_count=PARTITIONS[ANNOUNCE])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seq = {}                                # Soundbox -> last status sequence applied
    pending = []                            # announcements waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays confirmations from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_event(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - d["ns"] / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest event seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        k = d["d"]
        if d["seq"] <= seq.get(k, 0):
            stats["repeats"] += 1           # delivered twice or replayed: already applied, said once
            return
        seq[k] = d["seq"]
        stats["events"] += 1
        kind = rules(d, now)
        if kind is None:
            if d["k"] in ANNOUNCE_KINDS:
                stats["quiet"] += 1         # paid too long ago: total updated, passbook only
            return
        stats[kind] += 1
        stats["paise"] += d["amt"]
        if age > a.replay_announce_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody is at the counter
        pending.append(SendMessage(msg(d=k, seq=d["seq"], kind=kind, txn=d["txn"], amt=d["amt"], tot=d["tot"],
                                       cnt=d["cnt"], lang=d["lang"], vt=d["vt"], src_ns=d["ns"])))

    async def read(pids):
        # Each partition in turn. Resume from the broker's stored offset once (from the start of the log the
        # first time), then read on from our own position, storing the offset at most once a second: an offset
        # write on every poll stalls the lane. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-p{p}") for p in pids}
        nxt, committed = {}, {}
        while True:
            more = False
            for p in pids:
                commit = time.monotonic() - committed.get(p, 0) >= a.commit_s
                ms = await c.poll_messages(STREAM, LANE, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=commit,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.Next())
                if ms:
                    if commit:
                        committed[p] = time.monotonic()
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_event(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_announcements():
        # Announcements are small and come in bursts; one batch every 2 ms keeps sends out of the read loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        last = time.monotonic()
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            took, last = now - last, now          # the real length of this "second", for an honest rate
            if not caught_up[0]:
                # caught up once a whole second of events arrived inside the budget; partitions interleave,
                # so one fresh event alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier confirmations this "
                      f"second, state rebuilt for {len(seq):,} Soundboxes", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {round(r['n'] / took):>7,} confirmations/s  confirmation-to-announcer p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  announced {int(stats['sent']):,} "
                  f"(UPI {int(stats[UPI]):,}, card {int(stats[CARD]):,}, Rs {stats['paise'] / 100:,.0f})  "
                  f"late, passbook only {int(stats['quiet']):,}  repeats dropped {int(stats['repeats']):,}  "
                  f"{stalls.read()}  report every {took:.2f} s  slowest confirmation->broker {stats['to_broker']:.0f} ms, "
                  f"broker->announcer {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[LANE]))
    await asyncio.gather(send_announcements(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="announcer")
    p.add_argument("--budget-ms", type=int, default=50, help="confirmation-to-announcer p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the payment-confirmations partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--commit-s", type=float, default=1, help="store each partition's offset at most this often")
    p.add_argument("--replay-announce-s", type=float, default=10,
                   help="after a restart, still announce confirmations younger than this")
    asyncio.run(main(p.parse_args()))
