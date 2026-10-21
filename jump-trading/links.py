"""A network of links reporting RSSI, SNR and BER, and the consumers that read every alert.

Plays --links links in one process. Each link publishes a sample to link-samples every --sample-ms, keyed by link,
with its RSSI, SNR and BER and the link's sample sequence. Now and then a link fades: SNR and RSSI drop and BER
climbs for a while, then the link comes back. Fade durations are illustrative: a share are short bursts
(80-400 ms), the rest 3-90 s (set them to yours in fade_seconds()). --storm brings extra fades
forward for a while, the way weather crosses many links at once.

--consumers readers play the services that act on link-alerts (network console, on-call, status board), each on
its own connection, reading the alerts partition directly from its own position.

The business number: of the degraded alerts, how many reached the network console while that link was still
degraded, so the alert describes the network as it is. Printed every second, with fade-to-consumers p50/p99 (the
whole loop: the first bad sample, the rules, the alert, every consumer), and how many of the same alerts would
still have found the link degraded had they taken --compare-ms (default 150 ms, the p99 Jump has published for
its telemetry pipeline).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERTS, DEFAULT_URL, DEGRADED, PARTITIONS, RECOVERED, SAMPLES, STREAM, Stalls, Window,
                    ensure_topics, link, load, msg, nominal, part)

CONSUMERS = ["network-console", "on-call", "status-board", "capacity-planning", "reports"]


def fade_seconds(short_share: float, long_max_s: float):
    """How long a fade lasts (illustrative; set it to yours): short bursts, or longer weather fades."""
    if random.random() < short_share:
        return random.uniform(0.08, 0.4)
    return random.uniform(min(3, long_max_s / 2), long_max_s)


class Link:
    __slots__ = ("l", "seq", "snr0", "rssi0", "fade_end", "ep", "depth", "nxt")

    def __init__(self, l, now, gap):
        self.l, self.seq = l, 0
        self.snr0, self.rssi0 = nominal(l)
        self.fade_end = 0.0                 # monotonic time the current fade ends (in the past: no fade)
        self.ep = 0                         # sequence of the first sample of the current fade
        self.depth = 0.0                    # dB lost in the current fade
        self.nxt = now + random.expovariate(1 / gap)    # when the next fade starts

    def fading(self, now):
        return now < self.fade_end


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SAMPLES, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, SAMPLES, topic_partitions_count=PARTITIONS[SAMPLES],
                                                   mode=cfg)
    start_ns = time.time_ns()
    now = time.monotonic()
    period = a.sample_ms / 1000
    links = [Link(link(i), now, a.fade_every_s) for i in range(a.links)]
    by_name = {x.l: x for x in links}
    pid = [part(x.l, PARTITIONS[SAMPLES]) for x in links]
    due = [(now + random.uniform(0, period), i) for i in range(a.links)]
    heapq.heapify(due)
    fades = {}                              # (link, first-sample seq) -> fade length in s
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seen = set()                            # (link, kind, ep) the console already has

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            if d["src_ns"] > start_ns:
                lat.add((time.time_ns() - d["src_ns"]) / 1e6)   # live alerts only, not ones replayed at start-up
            if name != "network-console" or (d["l"], d["kind"], d["ep"]) in seen:
                return                      # repeated alert (at-least-once): already counted
            seen.add((d["l"], d["kind"], d["ep"]))
            stats[d["kind"]] += 1
            if d["kind"] != DEGRADED:
                return
            held = fades.pop((d["l"], d["ep"]), None)
            if held is None:
                return
            stats["fade_alerts"] += 1
            x = by_name[d["l"]]
            if x.ep == d["ep"] and x.fading(time.monotonic()):
                stats["while_degraded"] += 1    # the alert still describes the link as it is
            if held > a.compare_ms / 1000:
                stats["at_compare"] += 1        # an alert this slow would still have found it degraded
            if held < 1:
                stats["short"] += 1
        return on_alert

    async def consume(name):
        # One connection per consumer, reading the alerts partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    gc.freeze()                             # keep start-up objects out of the collector's way

    async def sample_loop():
        t0 = tick = last = time.monotonic()
        extra = 0.0
        async with producer:
            while True:
                now = time.monotonic()
                x_storm = a.storm if a.storm_at <= now - t0 < a.storm_at + a.storm_for else 1
                if x_storm > 1:             # the storm: bring extra fades forward, at (storm - 1) x the base rate
                    extra += (x_storm - 1) * a.links / a.fade_every_s * (now - last)
                    while extra >= 1:
                        extra -= 1
                        j = random.randrange(a.links)
                        if not links[j].fading(now):
                            links[j].nxt = now
                last = now
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    t, i = heapq.heappop(due)
                    heapq.heappush(due, (t + period, i))   # from its due time, so a late tick never bunches samples
                    x = links[i]
                    x.seq += 1
                    if not x.fading(now) and now >= x.nxt:      # a fade starts with this sample
                        held = fade_seconds(a.short_share, a.long_max_s)
                        x.fade_end, x.ep, x.depth = now + held, x.seq, random.uniform(10, 25)
                        x.nxt = x.fade_end + random.expovariate(1 / a.fade_every_s)
                        fades[(x.l, x.seq)] = held
                        stats["fades"] += 1
                    if x.fading(now):
                        snr = x.snr0 - x.depth + random.uniform(-1, 1)
                        rssi = x.rssi0 - x.depth + random.uniform(-1, 1)
                        ber = 10 ** random.uniform(-5, -3)
                    else:
                        snr = x.snr0 + random.uniform(-0.5, 0.5)
                        rssi = x.rssi0 + random.uniform(-0.5, 0.5)
                        ber = 10 ** random.uniform(-11, -8)
                    batches[pid[i]].append(SendMessage(msg(
                        l=x.l, seq=x.seq, rssi=round(rssi, 1), snr=round(snr, 1), ber=float(f"{ber:.2e}"))))
                    stats["samples"] += 1
                # One batch per partition, all sent at once: per-link order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["storm"] = x_storm > 1
                tick += 0.01
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[links] {a.links:,} links, a sample every {a.sample_ms:g} ms each, a fade every {a.fade_every_s:g} s "
              f"per link on average, {a.short_share:.0%} of fades short bursts, {a.consumers} consumers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "storm" if stats["storm"] else "steady"
            print(f"[links] {int(stats['samples']):>6,} samples/s ({phase})  fades started {int(stats['fades']):,}  "
                  f"degraded alerts at the console {int(stats['fade_alerts']):,}, while still degraded "
                  f"{int(stats['while_degraded']):,} (short bursts {int(stats['short']):,}; at {a.compare_ms:g} ms it "
                  f"would be {int(stats['at_compare']):,})  recovered alerts {int(stats[RECOVERED]):,}  "
                  f"fade-to-consumers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}",
                  flush=True)
            for k in ("samples", "fades", "fade_alerts", "while_degraded", "short", "at_compare", DEGRADED, RECOVERED):
                stats[k] = 0

    await asyncio.gather(sample_loop(), report(), stalls.watch(),
                         *(consume(n) for n in CONSUMERS[: a.consumers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--links", type=int, default=1000, help="links reporting telemetry")
    p.add_argument("--sample-ms", type=float, default=20, help="milliseconds between samples per link")
    p.add_argument("--fade-every-s", type=float, default=600, help="mean seconds between fades per link")
    p.add_argument("--short-share", type=float, default=0.3, help="share of fades that are short bursts (80-400 ms)")
    p.add_argument("--long-max-s", type=float, default=90, help="longest weather fade, seconds")
    p.add_argument("--consumers", type=int, default=3, help="services reading link-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="consumer wait between polls when nothing is waiting")
    p.add_argument("--compare-ms", type=float, default=150,
                   help="also count degraded alerts an alert this slow would still have found degraded")
    p.add_argument("--storm", type=float, default=1, help="fade-rate multiplier during the storm")
    p.add_argument("--storm-at", type=float, default=0, help="seconds after start")
    p.add_argument("--storm-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
