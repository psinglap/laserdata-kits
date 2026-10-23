"""The source of truth: every vehicle on every rooftop, as the DMS holds it, and the DMS's answer to test-drive bookings.

Each vehicle change (a deal started in desking and the unit going to Pending Sale, a deal delivered or falling
through, a reprice, a unit arriving from In Transit, going to or back from In Service, wholesaled, new miles on a
demo) goes to the vehicle-changes topic, keyed by rooftop:VIN, with a per-vehicle sequence number and the resulting
state (status, price, miles), never only a delta.

Agents' test-drive bookings arrive on the test-drive-requests topic. A booking is confirmed if the vehicle is still
In Stock, or declined because it is no longer available. A decline means an agent offered a shopper a vehicle that
had already gone to Pending Sale, Sold or Wholesale: the cost of a stale vehicle context.

--reprice-run models a dealer group's repricing pass: for --reprice-for seconds the change rate is multiplied and
most of the extra changes are new prices on the group's rooftops.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (AVAILABLE, BOOKINGS, DEFAULT_URL, GONE, OUTCOMES, PARTITIONS, STREAM, VEHICLES, key, load, msg,
                    part_of, vin)


async def main(a):
    async def own_client():                 # one connection per writer and reader, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    rng = random.Random(a.seed)
    admin = await own_client()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=200))
    lane = await (await own_client()).producer(STREAM, VEHICLES, topic_partitions_count=PARTITIONS, mode=cfg)  # replicated by default
    outcome_p = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=8, mode=cfg)
    if await admin.get_topic(STREAM, BOOKINGS) is None:     # agent_context.py may not have created it yet
        try:
            await admin.create_topic(STREAM, BOOKINGS, 8)
        except RuntimeError:
            pass                                             # created by agent_context.py in the meantime
    bookings = await (await own_client()).consumer_group(
        "dms-scheduling", STREAM, BOOKINGS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))

    status, price, miles = {}, {}, {}
    seq, stats = defaultdict(int), defaultdict(int)
    slots = []                              # slots[rooftop][i] = the key of a vehicle on that rooftop's lot
    outcomes = []                           # booking outcomes queued for the next tick, sent in one batch

    def new_vehicle(r, st):
        k = key(r, vin(rng))
        status[k] = st
        price[k] = rng.randrange(18_000, 72_000, 100)        # USD, illustrative
        miles[k] = rng.choice((5, 12, 30)) if rng.random() < 0.6 else rng.randrange(8_000, 90_000)
        return k

    def change(k, kind, snap=False):
        seq[k] += 1
        return k, SendMessage(msg(k=k, seq=seq[k], status=status[k], price=price[k], miles=miles[k], kind=kind, snap=snap))

    for r in range(a.rooftops):
        slots.append([new_vehicle(r, rng.choices(("In Stock", "Pending Sale", "In Transit", "In Service"),
                                                 (70, 10, 15, 5))[0]) for _ in range(a.vehicles)])

    async def send(changes):
        # One batch per partition, all sent at once: same per-vehicle order, about one round trip per tick.
        batches = defaultdict(list)
        for k, m in changes:
            batches[part_of(k)].append(m)
        await asyncio.gather(*(lane.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    async def on_booking(m):
        b = load(m.payload())
        k = b["k"]
        if status.get(k) == AVAILABLE:
            stats["booked"] += 1
            if b["price"] != price[k]:
                stats["price_moved"] += 1   # repriced between the agent's answer and the booking
            outcome = "booked"
        else:
            stats["declined"] += 1          # offered as available, already Pending Sale, Sold or Wholesale
            outcome = "declined_no_longer_available"
        outcomes.append(SendMessage(msg(booking=b["booking"], k=k, outcome=outcome)))

    async def snapshot():
        # The starting state of every vehicle, like a CDC snapshot (snap=true, not timed as a live change).
        keys = [k for lot in slots for k in lot]
        for i in range(0, len(keys), 5000):
            await send([change(k, "snapshot", snap=True) for k in keys[i:i + 5000]])
        print(f"[dms] snapshot: {len(keys):,} vehicles on {a.rooftops:,} rooftops", flush=True)

    def reprice(k):
        price[k] = max(5_000, int(round(price[k] * rng.choice((0.97, 0.98, 0.99, 1.01, 1.02)), -1)))
        return change(k, "repriced")

    def one_change(r, i):
        k = slots[r][i]
        st, x = status[k], rng.random()
        if st == AVAILABLE:
            if x < 0.40:
                return reprice(k)
            if x < 0.62:
                status[k] = "Pending Sale"                   # a deal started in desking
                return change(k, "deal_started")
            if x < 0.68:
                status[k] = "Wholesale"
                return change(k, "wholesaled")
            if x < 0.76:
                status[k] = "In Service"                     # back to the shop for recon or a fix
                return change(k, "to_service")
            miles[k] += rng.randrange(3, 60)                 # a demo drive
            return change(k, "miles")
        if st == "Pending Sale":
            status[k] = "Sold" if x < 0.6 else AVAILABLE     # delivered, or the deal fell through
            return change(k, "delivered" if x < 0.6 else "deal_unwound")
        status[k] = AVAILABLE                                # arrived from In Transit, or back from In Service
        return change(k, "arrived" if st == "In Transit" else "from_service")

    def replace_gone(r, i, out):
        # A vehicle that left the lot frees its slot: the next unit for this rooftop is ordered (In Transit).
        if status[slots[r][i]] in GONE:
            k = new_vehicle(r, "In Transit")
            slots[r][i] = k
            out.append(change(k, "ordered"))

    async def churn():
        async with lane, outcome_p:
            await snapshot()
            gc.freeze()                     # keep start-up tables out of the collector's way
            t0 = due = time.monotonic()
            group = range(min(a.group_rooftops, a.rooftops))
            while True:
                t = time.monotonic() - t0
                run = a.reprice_at and a.reprice_at <= t < a.reprice_at + a.reprice_for
                n = max(1, int(a.rate / 100))
                extra = int(a.rate * (a.reprice_run - 1) / 100) if run else 0
                changes = []
                for _ in range(n):
                    r, i = rng.randrange(a.rooftops), rng.randrange(a.vehicles)
                    changes.append(one_change(r, i))
                    replace_gone(r, i, changes)
                for _ in range(extra):      # the group's repricing pass: new prices on its In Stock units
                    r, i = rng.choice(group), rng.randrange(a.vehicles)
                    k = slots[r][i]
                    if status[k] == AVAILABLE:
                        changes.append(reprice(k))
                out, outcomes[:] = list(outcomes), []
                sends = [send(changes)]
                if out:
                    sends.append(outcome_p.send_with_partitioning(out, Partitioning.balanced()))
                await asyncio.gather(*sends)
                stats["changes"] += len(changes)
                due += 0.01                 # scheduled from its due time, so a slow tick doesn't drift the rest
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            total = stats["booked"] + stats["declined"]
            rate = 100 * stats["declined"] / total if total else 0
            print(f"[dms] vehicle changes {stats['changes']:,}  test drives booked {stats['booked']:,}  "
                  f"declined as no longer available {stats['declined']:,}  ({rate:.3f}%)  "
                  f"booked at a moved price {stats['price_moved']:,}  (this second)", flush=True)
            stats.clear()

    await asyncio.gather(churn(), bookings.consume_messages(on_booking, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rooftops", type=int, default=3000, help="dealership rooftops on the platform")
    p.add_argument("--vehicles", type=int, default=150, help="vehicles per rooftop (new and used)")
    p.add_argument("--rate", type=int, default=300, help="vehicle changes per second, all rooftops")
    p.add_argument("--reprice-run", type=float, default=1.0, help="rate multiplier during a group repricing pass")
    p.add_argument("--reprice-at", type=float, default=0, help="seconds after start")
    p.add_argument("--reprice-for", type=float, default=120)
    p.add_argument("--group-rooftops", type=int, default=60, help="rooftops in the dealer group that reprices")
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
