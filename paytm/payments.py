"""Device merchants taking payments, the confirmation step, and the readers of every announcement.

Plays --devices Soundbox merchants in one process. Customers pay each merchant (UPI QR mostly, some card taps),
one status event every --pay-s seconds per device on average, each with the time the customer paid (vt), the
device's event sequence and its resulting state: today's collections and payment count after this payment, as
the screen Soundboxes show them. The confirmation step routes each event by how long ago the customer paid
(common.route()):
  * fresh confirmations go on payment-confirmations, the lane the announcement service reads;
  * confirmations older than common.FRESH_S go on late-confirmations, which the passbook reads at its own pace.

Two things from the field are played on purpose:
  * a repeated callback: --dup-share of confirmations are delivered a second time, with the same sequence, as
    at-least-once status delivery does;
  * a bank recovery: for --held-share of the devices, the customers' bank is down from the start and their
    payments stay pending; at --recover-at it comes back and each held device's pending payments (--held-min
    minutes of them) are confirmed over --flush-s seconds.

--readers services read soundbox-announcements (the device push that sends each announcement to its Soundbox,
the business app, merchant support), each on its own connection, reading the announcements partition directly
from its own position. The device push reader checks every announcement against the truth this process holds:
each fresh payment was announced once, on time, with the right running total; no payment was announced twice;
no payment the customer made minutes ago was announced as if it just happened; no device's total went backwards.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ANNOUNCE, ANNOUNCE_KINDS, CARD, DEFAULT_URL, FAILED, FRESH_S, LANE, LANGS, LATE, PARTITIONS,
                    PENDING, STREAM, UPI, Stalls, Window, device, ensure_topics, load, msg, part, route)

READERS = ["device-push", "business-app", "merchant-support", "risk-monitor", "partner-webhooks"]
EDGE_S = 5                          # payments within 5 s of FRESH_S may land either side; they are not judged


class Soundbox:
    __slots__ = ("s", "seq", "tot", "cnt", "lang", "card", "nxt", "held")

    def __init__(self, s):
        self.s, self.seq, self.tot, self.cnt = s, 0, 0, 0
        self.lang = random.choice(LANGS)
        self.card = random.random() < 0.2           # a share of devices also take card taps (illustrative)
        self.nxt, self.held = 0.0, False

    def step(self, settled=False):
        """The device's next status event and its resulting state (illustrative mix; yours come from the
        payment status stream). settled: a pending payment the bank has now confirmed."""
        r = random.random()
        if settled or r < 0.94:
            k = CARD if self.card and random.random() < 0.15 else UPI
        elif r < 0.98:
            k = PENDING
        else:
            k = FAILED
        amt = 0
        if k in ANNOUNCE_KINDS:
            amt = int(min(500_000, random.lognormvariate(5.3, 1.1)) * 100)    # paise; mostly small-ticket
            self.tot += amt
            self.cnt += 1
        self.seq += 1
        return dict(d=self.s, seq=self.seq, k=k, txn=f"T{self.s[4:]}{self.seq:07d}", amt=amt, tot=self.tot,
                    cnt=self.cnt, lang=self.lang)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LANE, LATE, ANNOUNCE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    lane = await (await own_client()).producer(STREAM, LANE, topic_partitions_count=PARTITIONS[LANE], mode=cfg)
    late = await (await own_client()).producer(STREAM, LATE, topic_partitions_count=PARTITIONS[LATE],
                                               mode=DirectProducerConfig(batch_length=5000,
                                                                         linger_time=timedelta(milliseconds=2)))
    start_ns = time.time_ns()
    fleet = [Soundbox(device(i)) for i in range(a.devices)]
    pid = [part(x.s, PARTITIONS[LANE]) for x in fleet]
    held = set(random.sample(range(a.devices), int(a.devices * a.held_share))) if a.recover_at else set()
    now = time.monotonic()
    due = []
    for i, x in enumerate(fleet):
        if i in held:
            x.held = True                   # the customers' bank is down: payments pending since before the run
        else:
            x.nxt = now + random.expovariate(1 / a.pay_s)      # memoryless from the start: a steady rate, no
            due.append((x.nxt, i))                              # surge when first and second payments overlap
    heapq.heapify(due)
    expected = {}                           # (device, seq) -> (routed at ns, total): announcements the device must get
    order = deque()                         # (routed at ns, key), to find announcements that never arrived
    seen = set()                            # (device, seq) already announced
    shown = {}                              # device -> highest sequence its screen shows
    lat, stats, total, stalls = Window(), defaultdict(float), defaultdict(int), Stalls()

    def expect(d, now_ns):
        """Record what the device must announce for a confirmation the confirmation step put on the lane."""
        if d["k"] not in ANNOUNCE_KINDS:
            return
        if (now_ns - d["vt"]) / 1e9 <= FRESH_S - EDGE_S:
            expected[(d["d"], d["seq"])] = (now_ns, d["tot"])
            order.append((now_ns, (d["d"], d["seq"])))
            stats["expected"] += 1

    def on_announcement(m):
        d = load(m.payload())
        if d["src_ns"] <= start_ns:
            return                          # from an earlier run
        now_ns = time.time_ns()
        key = (d["d"], d["seq"])
        if key in seen:
            stats["twice"] += 1             # the merchant would hear one payment twice
            return
        seen.add(key)
        if d["src_ns"] - d["vt"] > FRESH_S * 1e9:
            stats["stale"] += 1             # a payment from minutes ago announced as if it just happened
        if d["seq"] < shown.get(d["d"], 0):
            stats["backwards"] += 1         # the screen's total and count would go backwards
        shown[d["d"]] = max(shown.get(d["d"], 0), d["seq"])
        want = expected.pop(key, None)
        if want is not None:
            ms = (now_ns - d["src_ns"]) / 1e6
            lat.add(ms)
            stats["at_device"] += 1
            stats["on_time"] += ms < a.on_time_ms
            stats["wrong_total"] += d["tot"] != want[1]

    async def read(name):
        # One connection per reader, reading the announcements partition directly from its own position (no
        # consumer group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c = await own_client()
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, ANNOUNCE, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                if name == "device-push":
                    for m in ms:
                        on_announcement(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send(batches, producer):
        # One batch per partition, all sent at once: per-device order is kept, one round trip per tick.
        if batches:
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in batches.items()))

    gc.freeze()                             # keep start-up objects out of the collector's way
    dups = []                               # confirmations delivered a second time next tick

    async def live_loop():
        nonlocal dups
        t0 = tick = last = time.monotonic()
        extra = 0.0
        while True:
            now = time.monotonic()
            x_peak = a.peak if a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1
            if x_peak > 1:                  # the peak: bring extra payments forward, at (peak - 1) x the base rate
                extra += (x_peak - 1) * a.devices / a.pay_s * (now - last)
                while extra >= 1:
                    extra -= 1
                    j = random.randrange(a.devices)
                    if not fleet[j].held:
                        fleet[j].nxt = now
                        heapq.heappush(due, (now, j))
            last = now
            batches = defaultdict(list)
            now_ns = time.time_ns()
            for d in dups:
                batches[part(d["d"], PARTITIONS[LANE])].append(SendMessage(msg(**d)))
                stats["dups"] += 1
            dups = []
            while due and due[0][0] <= now:
                t, i = heapq.heappop(due)
                x = fleet[i]
                if t != x.nxt or x.held:
                    continue                # superseded by a payment the peak brought forward
                d = x.step()
                d["vt"] = now_ns            # live: the customer paid moments ago
                expect(d, now_ns)
                batches[pid[i]].append(SendMessage(msg(**d)))
                if random.random() < a.dup_share:
                    dups.append(d)
                stats["events"] += 1
                x.nxt = t + random.expovariate(1 / a.pay_s)        # from its due time, so a late tick never bunches
                heapq.heappush(due, (x.nxt, i))
            await send(batches, lane)
            stats["peak"] = x_peak > 1
            tick += 0.01
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def bank_recovery():
        """The bank comes back: every held device's pending payments are confirmed within --flush-s seconds,
        oldest first, and the last one's callback arrives twice. The confirmation step routes each by how long
        ago the customer paid: the last minute's to the lane (announced), the rest to the passbook."""
        if not held:
            return
        await asyncio.sleep(a.recover_at)
        stats["recovery"] = 1
        back_at = time.time_ns()
        held_ns = int(a.held_min * 60e9)
        n_each = max(1, int(a.held_min * 60 / a.pay_s))
        queue = list(held)
        random.shuffle(queue)
        per_tick = max(1, int(len(queue) / (a.flush_s * 100)))
        while queue:
            now_ns = time.time_ns()
            lane_b, late_b = defaultdict(list), defaultdict(list)
            for i in queue[:per_tick]:
                x = fleet[i]
                for j in range(n_each):
                    d = x.step(settled=True)
                    d["vt"] = back_at - held_ns + int((j + 1) * held_ns / (n_each + 1))
                    events = [d] + ([dict(d)] if j == n_each - 1 else [])     # the last callback twice
                    for e in events:
                        if route(e["k"], e["vt"], now_ns) == LANE:
                            if e is d:
                                expect(e, now_ns)
                            lane_b[pid[i]].append(SendMessage(msg(**e)))
                            stats["settled_live"] += 1
                        else:
                            late_b[part(x.s, PARTITIONS[LATE])].append(SendMessage(msg(**e)))
                            stats["late"] += 1
                            total["late"] += 1
            await asyncio.gather(send(lane_b, lane), send(late_b, late))
            for i in queue[:per_tick]:      # bank back: new payments follow the settled ones, in sequence order
                x = fleet[i]
                x.held, x.nxt = False, time.monotonic() + random.expovariate(1 / a.pay_s)
                heapq.heappush(due, (x.nxt, i))
                total["back"] += 1
            del queue[:per_tick]
            await asyncio.sleep(0.01)
        stats["recovery"] = 0
        print(f"[payments] bank recovery done: {len(held):,} devices' pending payments settled, {total['late']:,} "
              f"late confirmations to '{LATE}' in total", flush=True)

    async def report():
        print(f"[payments] {a.devices:,} Soundbox merchants, a payment every {a.pay_s:g} s each, {a.dup_share:.1%} "
              f"delivered twice, {len(held):,} held by a bank outage of {a.held_min:g} min recovering at "
              f"{a.recover_at:g} s, {a.readers} announcement readers, fresh window {FRESH_S} s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            cutoff = time.time_ns() - int(a.missing_s * 1e9)
            while order and order[0][0] < cutoff:
                _, key = order.popleft()
                if expected.pop(key, None) is not None:
                    stats["missing"] += 1   # never announced
            phase = "recovery" if stats["recovery"] else "peak" if stats["peak"] else "steady"
            print(f"[payments] {int(stats['events'] + stats['settled_live']):>6,} confirmations/s ({phase})  "
                  f"devices taking payments {a.devices - len(held) + total['back']:,}  announcements expected "
                  f"{int(stats['expected']):,}  at the device {int(stats['at_device']):,}, on time (<{a.on_time_ms:g} ms) "
                  f"{int(stats['on_time']):,}  missing {int(stats['missing']):,}  announced twice {int(stats['twice']):,}  "
                  f"stale announced {int(stats['stale']):,}  total backwards {int(stats['backwards']):,}  wrong total "
                  f"{int(stats['wrong_total']):,}  delivered twice {int(stats['dups']):,}  to passbook {int(stats['late']):,} "
                  f"(total {total['late']:,})  confirmation-to-device n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"{stalls.read()}", flush=True)
            for k in ("events", "settled_live", "expected", "at_device", "on_time", "missing", "twice", "stale",
                      "backwards", "wrong_total", "dups", "late"):
                stats[k] = 0

    async with lane, late:
        await asyncio.gather(live_loop(), bank_recovery(), report(), stalls.watch(),
                             *(read(n) for n in READERS[: a.readers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--devices", type=int, default=1_000_000, help="Soundbox merchants")
    p.add_argument("--pay-s", type=float, default=200, help="mean seconds between payments per device")
    p.add_argument("--dup-share", type=float, default=0.01, help="share of confirmations delivered twice")
    p.add_argument("--recover-at", type=float, default=60, help="seconds until the bank recovers (0 = no outage)")
    p.add_argument("--held-share", type=float, default=0.05, help="share of devices whose customers' bank is down")
    p.add_argument("--held-min", type=float, default=15, help="minutes the bank was down")
    p.add_argument("--flush-s", type=float, default=10, help="seconds over which the pending payments settle")
    p.add_argument("--readers", type=int, default=3, help="services reading announcements, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="reader wait between polls when nothing is waiting")
    p.add_argument("--on-time-ms", type=float, default=1000, help="an announcement at the device push within this is on time")
    p.add_argument("--missing-s", type=float, default=5, help="an expected announcement not there after this is missing")
    p.add_argument("--peak", type=float, default=1, help="payment-rate multiplier during the peak (festival evening)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
