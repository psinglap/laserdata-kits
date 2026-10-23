"""One Cribl Stream Worker Node writing a security Route's output to Apache Iggy, once, for every Destination.

Plays the Worker Processes of one Worker Node after the Route's Pipeline has run: each sending host
(a firewall, a domain controller, a DNS server…) is pinned to one Worker Process, as a sender's
connection is, and every Worker Process sends one batch per partition per tick on its own
connection. Every event carries the host, its sourcetype, a per-host sequence number and the time
the Worker sent it, so each Destination can drop repeats, prove nothing is missing and time it.

Run one per Worker Node with a different --node; the hosts don't overlap.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TOPIC, connect, ensure_topic, hosts, msg, part

RAW = {  # one short line per sourcetype, as the SIEM would index it (illustrative)
    "firewall": "action=allow proto=tcp src=10.4.{a}.{b} dst=172.16.{b}.{a} dport=443 bytes={n}",
    "windows_security": "EventCode=4624 LogonType=3 Account=svc-{a} Workstation=WS-{b} Result=Success",
    "dns": "query=host{a}.corp.example qtype=A rcode=NOERROR client=10.8.{a}.{b} ttl={n}",
    "web_proxy": "method=GET status=200 url=https://app{a}.example/{b} bytes={n} user=u{a}{b}",
    "endpoint": "process=svc{a}.exe parent=init{b} action=start sha256_prefix={n:08x}",
    "linux_auth": "sshd[{n}]: Accepted publickey for deploy{a} from 10.2.{a}.{b} port 22",
}


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, TOPIC, a.partitions)
    procs = [await connect(a.url) for _ in range(a.processes)]
    hs = hosts(a.hosts, a.node)
    owner = [i % a.processes for i in range(len(hs))]      # host -> Worker Process
    seq = defaultdict(int)
    owed = 0.0
    stats = defaultdict(int)
    print(f"[worker-{a.node:02d}] {len(hs)} sending hosts on {a.processes} Worker Processes -> "
          f"{STREAM}/{TOPIC} ({a.partitions} partitions)", flush=True)
    gc.freeze()                                           # keep the collector from pausing the sender

    async def send(c, batch):
        await asyncio.gather(*(c.send_messages(STREAM, TOPIC, Partitioning.partition_id(pid), ms)
                               for pid, ms in batch.items()))

    async def run():
        nonlocal owed
        tick = time.monotonic()
        while True:
            rate = a.rate * (a.peak if time.monotonic() - t0 < a.peak_s else 1)
            owed += rate * a.tick_ms / 1000
            k = int(owed)
            owed -= k
            batches = [defaultdict(list) for _ in procs]
            ns = time.time_ns()
            for _ in range(k):
                i = random.randrange(len(hs))
                h, st = hs[i]
                seq[h] += 1
                x, y = random.randrange(256), random.randrange(256)
                raw = RAW[st].format(a=x, b=y, n=random.randrange(1 << 31))
                batches[owner[i]][part(h, a.partitions)].append(
                    SendMessage(msg(ns=ns, host=h, sourcetype=st, seq=seq[h], _raw=raw)))
            stats["events"] += k
            await asyncio.gather(*(send(c, b) for c, b in zip(procs, batches) if b))
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # due time, never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            print(f"[worker-{a.node:02d}] {stats['events']:>7,} events/s written once to {TOPIC}", flush=True)
            stats.clear()

    t0 = time.monotonic()
    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--node", type=int, default=0, help="Worker Node number; hosts are named by it")
    p.add_argument("--processes", type=int, default=4, help="Worker Processes on this node, one connection each")
    p.add_argument("--hosts", type=int, default=2000, help="sending hosts on this node")
    p.add_argument("--rate", type=float, default=7000, help="events a second this node writes")
    p.add_argument("--peak", type=float, default=1.0, help="rate multiplier for the first --peak-s seconds")
    p.add_argument("--peak-s", type=float, default=0)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
