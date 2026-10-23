"""Shadow one chain's events from a running ChainStorage into Apache Iggy, read-only.

ChainStorage serves its event stream over gRPC (StreamChainEvents, coinbase/chainstorage api.proto). This bridge
is one more stream client: it starts after the last sequence number already on the lane (or at --sequence, or
at the latest event), and copies each BlockchainEvent into the chain's partition with its sequence_num, type and
block identifier. It never writes to ChainStorage, so the poller, the streamer and every existing client are
untouched. It uses grpcurl with server reflection, as ChainStorage's own README does, so no generated stubs are
needed.

The events land with the bridge's clock as "ns" (ChainStorage's stream does not carry the time it recorded an
event), so indexer.py measures bridge-to-indexer latency on this path.

Needs: pip install apache-iggy==0.9.0, and grpcurl on the PATH.
"""
import argparse
import asyncio
import json
import time

from apache_iggy import Consumer, PollingStrategy, SendMessage

from common import CHAINS, DEFAULT_URL, EVENTS, STREAM, connect, ensure_topics, load, msg, part_of


async def last_sequence(c, chain: str) -> int:
    """The highest sequence number of this chain already on the lane (a scan of its partition; fine for a trial)."""
    p, last, off = part_of(chain), 0, 0
    who = Consumer.Single("chainstorage-bridge")
    while True:
        ms = await c.poll_messages(STREAM, EVENTS, consumer=who, partition_id=p, count=5000, auto_commit=False,
                                   polling_strategy=PollingStrategy.Offset(off))
        for m in ms:
            e = load(m.payload())
            if e["chain"] == chain:
                last = max(last, e["seq"])
        if len(ms) < 5000:
            return last
        off = ms[-1].offset() + 1


def to_event(chain: str, d: dict) -> dict:
    ev = d.get("event", d)
    b = ev.get("block", {})
    return dict(chain=chain, seq=int(ev["sequenceNum"]), type=ev["type"], height=int(b.get("height", 0)),
                hash=b.get("hash", ""), tag=int(b.get("tag", 0)), skipped=bool(b.get("skipped", False)),
                ts=b.get("timestamp"), ns=time.time_ns())


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy)
    start = a.sequence if a.sequence is not None else await last_sequence(iggy, a.chain)
    req = {"sequence_num": start} if start else {"initial_position_in_stream": "LATEST"}
    cmd = ["grpcurl", "-max-msg-sz", "67108864"] + (["-plaintext"] if a.plaintext else []) + \
          sum((["-H", h] for h in a.header), []) + \
          ["-d", json.dumps(req), a.target, "coinbase.chainstorage.ChainStorage/StreamChainEvents"]
    print(f"[bridge] {a.chain}: streaming from {a.target} after sequence {start or 'LATEST'} into partition "
          f"{part_of(a.chain)}", flush=True)
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
    dec, buf, n, t0 = json.JSONDecoder(), "", 0, time.monotonic()
    while True:
        chunk = await proc.stdout.read(65536)
        if not chunk:
            raise SystemExit(f"[bridge] stream ended (grpcurl exit {await proc.wait()})")
        buf += chunk.decode()
        out = []
        while True:                         # grpcurl prints one JSON object per message
            buf = buf.lstrip()
            try:
                d, i = dec.raw_decode(buf)
            except ValueError:
                break
            buf = buf[i:]
            out.append(SendMessage(msg(**to_event(a.chain, d))))
        if out:
            await iggy.send_messages(STREAM, EVENTS, part_of(a.chain), out)
            n += len(out)
        if time.monotonic() - t0 >= 10:
            print(f"[bridge] {a.chain}: {n:,} events copied", flush=True)
            t0 = time.monotonic()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True, help="ChainStorage gRPC address, host:port")
    p.add_argument("--chain", default="base-mainnet", choices=list(CHAINS), help="the chain this ChainStorage serves")
    p.add_argument("--sequence", type=int, default=None, help="start after this sequence_num (default: what the lane has)")
    p.add_argument("--plaintext", action="store_true", help="no TLS, e.g. a local ChainStorage")
    p.add_argument("-H", "--header", action="append", default=[], help="extra gRPC header, e.g. client id or auth")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
