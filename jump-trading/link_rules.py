"""The link baseline and alert rules, fed by the link health lane.

Reads every partition of link-samples directly, on --readers connections of its own, tracking its own position
(no consumer group: a group reader visits one partition per poll, so a sample can wait for its partition's turn;
playbook rule found on other kits). Each record is one telemetry sample from one link:
RSSI, SNR and BER, with the link's sample sequence, so a replayed or repeated sample is recognised and changes
nothing.

The rules (stand-ins for yours; put your own in rules()):
  * each link has its own SNR baseline, learned from its clean samples (an EWMA), so a link that always runs at
    33 dB and one that runs at 41 dB are each judged against themselves;
  * DEGRADED: BER above BER_MAX, or SNR more than SNR_DROP_DB below the link's baseline. One alert per episode,
    named by the sequence of the sample that opened it;
  * RECOVERED: CLEAR_SAMPLES clean samples in a row; the alert names the episode it closes and how long it lasted.
Alerts go to link-alerts, which every consumer reads (links.py plays the network console, on-call and the status
board). Alerts are sent in small batches from their own task, never from inside the read loop.

Prints sample-to-rules p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, and the alerts.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict
from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import (ALERTS, BASELINE_ALPHA, BER_MAX, CLEAR_SAMPLES, DEFAULT_URL, DEGRADED, PARTITIONS, RECOVERED,
                    SAMPLES, SNR_DROP_DB, STREAM, Stalls, Window, ensure_topics, load, msg)


class LinkState:
    __slots__ = ("seq", "base", "bad", "ep", "ep_ns", "clean")

    def __init__(self):
        self.seq, self.base, self.bad, self.ep, self.ep_ns, self.clean = 0, None, False, 0, 0, 0


def rules(d: dict, s: LinkState):
    """Applies one sample to the link's state. Returns DEGRADED, RECOVERED or None."""
    breach = d["ber"] > BER_MAX or (s.base is not None and d["snr"] < s.base - SNR_DROP_DB)
    if not s.bad:
        if breach:
            s.bad, s.ep, s.ep_ns, s.clean = True, d["seq"], d["ns"], 0
            return DEGRADED
        s.base = d["snr"] if s.base is None else s.base + BASELINE_ALPHA * (d["snr"] - s.base)
        return None
    if breach:
        s.clean = 0
        return None
    s.clean += 1
    if s.clean >= CLEAR_SAMPLES:
        s.bad = False
        return RECOVERED
    return None


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SAMPLES, ALERTS)
    out = await (await own_client()).producer(STREAM, ALERTS, topic_partitions_count=PARTITIONS[ALERTS])
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    links = defaultdict(LinkState)
    pending = []                            # alerts waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays samples from before it joined
    gc.freeze()                             # keep start-up objects out of the collector's way

    def on_sample(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
            b = m.timestamp() / 1e3                 # broker append time, ms: where the time went
            stats["to_broker"] = max(stats["to_broker"], b - d["ns"] / 1e6)
            stats["from_broker"] = max(stats["from_broker"], now / 1e6 - b)
        else:                               # still replaying: track the oldest sample seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        s = links[d["l"]]
        if d["seq"] <= s.seq:
            stats["dropped"] += 1           # older or repeated sample: already applied
            return
        s.seq = d["seq"]
        stats["samples"] += 1
        kind = rules(d, s)
        if kind is None:
            return
        stats[kind] += 1
        if age > a.replay_alert_s * 1000:
            return                          # replayed from long ago: state rebuilt, nobody needs this alert now
        pending.append(SendMessage(msg(l=d["l"], kind=kind, ep=s.ep, src_ns=d["ns"], snr=d["snr"], ber=d["ber"],
                                       base=round(s.base or 0, 1), lasted_ms=round((d["ns"] - s.ep_ns) / 1e6)
                                       if kind == RECOVERED else 0)))

    async def read(pids):
        # Each partition in turn, from the start of the log (baselines are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, SAMPLES, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_sample(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_alerts():
        # Alerts are small and come in bursts; one batch every 2 ms keeps sends out of the read loop.
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
                # caught up once a whole second of samples arrived inside the budget; partitions interleave,
                # so one fresh sample alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier samples this second, "
                      f"state rebuilt for {len(links):,} links", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} samples/s  sample-to-rules p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  alerts sent {int(stats['sent']):,} "
                  f"(degraded {int(stats[DEGRADED]):,}, recovered {int(stats[RECOVERED]):,})  "
                  f"links degraded now {sum(s.bad for s in links.values()):,}  {stalls.read()}  slowest sample->broker "
                  f"{stats['to_broker']:.0f} ms, broker->rules {stats['from_broker']:.0f} ms", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[SAMPLES]))
    await asyncio.gather(send_alerts(), report(), stalls.watch(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="link-rules")
    p.add_argument("--budget-ms", type=int, default=50, help="sample-to-rules p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the link-samples partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--replay-alert-s", type=float, default=30,
                   help="after a restart, still send alerts for samples younger than this")
    asyncio.run(main(p.parse_args()))
