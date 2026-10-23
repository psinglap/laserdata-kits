"""Plays one Ditto Big Peer Data Bridge: every change to a collection, as a Ditto CDC event, onto the lane.

The collection is cabin_orders from a crew app: one document per flight and seat (meal choice, special
assistance, service status), edited on crew devices that sync with each other in the air and with Big Peer when
they reconnect. Each change goes to the cabin-orders topic keyed by document _id, in Ditto's CDC event shape:
  * documentChanged / upsert with oldValue and newValue (an insert has oldValue null);
  * documentChanged / remove with the full document as value;
  * documentChanged / snapshot for the starting state (and, in Ditto, during recovery and under load);
  * requeryRequired (--requery-at), which Ditto sends when the CDC producer falls behind.
txnId rises with every transaction, so it rises with every change to a given document, as Ditto's does.

--wave plays a landing wave: aircraft reach the gate, crew devices reconnect and every edit made offline in the
air reaches Big Peer at once, concentrated on the seats of the flights that just landed. --dup-share re-sends a
share of events with the same txnId, as an at-least-once path or two bridges running side by side would.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import CHANGES, DEFAULT_URL, PARTITIONS, STREAM, changed, connect, doc_key, dump, ensure_topics, part_of

MEALS = ("chicken", "pasta", "vegetarian", "kosher", "none")
STATUS = ("requested", "confirmed", "served", "declined")
ASSIST = (None, None, None, "wheelchair", "infant", "unaccompanied minor", "allergy: nuts")


def doc_id(i: int) -> str:
    flight, s = divmod(i, 36)                       # 36 tracked seats per flight in this collection
    return f"FL{flight:05d}-{1 + s // 6}{'ABCDEF'[s % 6]}"


async def main(a):
    admin = await connect(a.url)
    await ensure_topics(admin, CHANGES)
    lane = await connect(a.url)
    rng = random.Random(11)
    ids = [doc_id(i) for i in range(a.docs)]
    flights = sorted({i.split("-")[0] for i in ids})
    by_flight = defaultdict(list)
    for i in ids:
        by_flight[i.split("-")[0]].append(i)
    docs = {}                                       # _id -> current document, or None once removed
    txn = [0]
    st = defaultdict(int)
    resend = []                                     # events to deliver a second time on the next tick

    def new_doc(i, v):
        f, seat = i.split("-")
        return {"_id": i, "_version": v, "flight": f, "seat": seat, "meal": rng.choice(MEALS),
                "assist": rng.choice(ASSIST), "status": "requested"}

    def next_txn():
        txn[0] += 1
        return txn[0]

    def one_change(i, ns):
        cur = docs.get(i)
        if cur is None:                             # a removed order placed again: an insert
            new = new_doc(i, 1)
            docs[i] = new
            st["inserts"] += 1
            return changed(next_txn(), "upsert", ns, old=None, new=new)
        if rng.random() < a.remove_share:
            docs[i] = None
            st["removes"] += 1
            return changed(next_txn(), "remove", ns, value=cur)
        new = dict(cur, _version=cur["_version"] + 1)
        r = rng.random()
        if r < 0.5:
            new["status"] = rng.choice(STATUS)
        elif r < 0.8:
            new["meal"] = rng.choice(MEALS)
        else:
            new["assist"] = rng.choice(ASSIST)
        docs[i] = new
        return changed(next_txn(), "upsert", ns, old=cur, new=new)

    async def send(events):
        out = defaultdict(list)                     # one batch per partition: a document's events stay in order
        for e in events:
            key = doc_key((e["change"].get("newValue") or e["change"].get("value"))["_id"])
            out[part_of(key)].append(SendMessage(dump(e)))
        await asyncio.gather(*(lane.send_messages(STREAM, CHANGES, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    async def snapshot():
        # The bridge's starting state, as snapshot events (not timed as live changes).
        for k in range(0, len(ids), 5000):
            ns = time.time_ns()
            batch = []
            for i in ids[k:k + 5000]:
                docs[i] = new_doc(i, 1)
                batch.append(changed(next_txn(), "snapshot", ns, value=docs[i]))
            await send(batch)
        print(f"[big-peer] snapshot: {len(ids):,} {a.collection} documents, txnId 1..{txn[0]:,}", flush=True)

    async def run():
        await snapshot()
        gc.freeze()                                 # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed, landed, requeried = 0.0, [], False
        while True:
            t = time.monotonic() - t0
            wave = a.wave_at and a.wave_at <= t < a.wave_at + a.wave_for
            if wave and not landed:
                landed = rng.sample(flights, min(a.landed, len(flights)))   # these aircraft just reached the gate
            elif not wave:
                landed = []
            owed += a.rate * (a.wave if wave else 1) / 100
            ns = time.time_ns()
            events = list(resend)
            resend.clear()
            for _ in range(int(owed)):
                if wave and rng.random() < 1 - 1 / a.wave:
                    i = rng.choice(by_flight[rng.choice(landed)])           # an edit made offline in the air
                else:
                    i = rng.choice(ids)
                e = one_change(i, ns)
                events.append(e)
                if rng.random() < a.dup_share:
                    resend.append(e)                # the same txnId again, a moment later
                    st["resent"] += 1
            st["changes"] += int(owed)
            owed -= int(owed)
            t_send = time.monotonic()
            if events:
                await send(events)
            if a.requery_at and not requeried and t >= a.requery_at:
                requeried = True                    # the CDC producer fell behind: every partition gets the signal
                r = dump({"txnId": txn[0], "type": "requeryRequired", "ns": time.time_ns()})
                await asyncio.gather(*(lane.send_messages(STREAM, CHANGES, Partitioning.partition_id(p),
                                                          [SendMessage(r)]) for p in range(PARTITIONS[CHANGES])))
                print(f"[big-peer] requeryRequired sent at txnId {txn[0]:,}", flush=True)
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))    # how late this tick started
            st["wave"] = int(bool(wave))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[big-peer] {st['changes']:>6,} changes/s ({'landing wave' if st['wave'] else 'steady'})  "
                  f"txnId {txn[0]:,}  inserts {st['inserts']:,}  removes {st['removes']:,}  "
                  f"re-sent {st['resent']:,}  tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms",
                  flush=True)
            st.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--collection", default="cabin_orders", help="label only; the kit's collection")
    p.add_argument("--docs", type=int, default=200_000, help="documents in the collection")
    p.add_argument("--rate", type=float, default=2_000, help="document changes per second")
    p.add_argument("--remove-share", type=float, default=0.02, help="share of changes that remove the document")
    p.add_argument("--dup-share", type=float, default=0.005, help="share of events delivered a second time")
    p.add_argument("--wave", type=float, default=1.0, help="change-rate multiplier during a landing wave")
    p.add_argument("--wave-at", type=float, default=0, help="seconds after start")
    p.add_argument("--wave-for", type=float, default=60)
    p.add_argument("--landed", type=int, default=20, help="flights reconnecting in the wave")
    p.add_argument("--requery-at", type=float, default=0, help="send requeryRequired this many seconds in (0 = never)")
    asyncio.run(main(p.parse_args()))
