"""Shadow real order updates into Apache Iggy, read-only, from DhanHQ's Live Order Update socket.

Dhan has not published what carries order updates inside DEXT, so this bridge starts from the one feed that is
public: wss://api-order-update.dhan.co. With a partner ID and secret (UserType PARTNER) it receives updates for
every user connected through that partner platform; with a client ID and access token (UserType SELF), the
client's own orders from every platform. It only listens: it places, modifies and cancels nothing.

Each update is mapped onto the kit's record (k = ClientId, a per-client seq assigned here, OrderNo, LegNo, Status,
quantities, prices) and written to the lane with the kit's partition function, so order_gateway.py reads real
update shapes and rates. ns is when the bridge received the update, so the latency printed is bridge-to-gateway.
If DEXT's updates already travel on an internal bus, a small subscriber on that bus does the same job, from
earlier in the path.

Needs: pip install apache-iggy==0.9.0 websockets
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, UPDATES, connect, ensure_topics, msg, part_of

URL = "wss://api-order-update.dhan.co"
seq = defaultdict(int)


def to_update(d: dict) -> tuple[str, bytes]:
    """Map one Live Order Update 'Data' object onto the lane's record. Adjust here if your fields differ."""
    k = str(d.get("ClientId", ""))
    seq[k] += 1
    leg = int(d.get("LegNo") or 1)
    return k, msg(k=k, seq=seq[k], order_no=str(d.get("OrderNo", "")), leg=leg,
                  entry=str(d.get("AlgoOrdNo") or "") if leg > 1 else "",
                  segment=f"{d.get('Exchange', '')}_{d.get('Segment', '')}", symbol=d.get("Symbol", ""),
                  txn=d.get("TxnType", ""), product=d.get("Product", ""), order_type=d.get("OrderType", ""),
                  status=str(d.get("Status", "")).upper(), qty=int(d.get("Quantity") or 0),
                  traded_qty=int(d.get("TradedQty") or 0), price=float(d.get("Price") or 0),
                  avg_price=float(d.get("AvgTradedPrice") or 0))


async def main(a):
    import websockets

    iggy = await connect(a.url)
    await ensure_topics(iggy, UPDATES)
    if a.partner_id:
        login = {"LoginReq": {"MsgCode": 42, "ClientId": a.partner_id}, "UserType": "PARTNER", "Secret": a.secret}
    else:
        login = {"LoginReq": {"MsgCode": 42, "ClientId": a.client_id, "Token": a.token}, "UserType": "SELF"}
    buf, n, t0 = [], 0, time.monotonic()

    async def flush():
        # One batch per partition every 2 ms, never one awaited send per update.
        nonlocal n, t0
        while True:
            await asyncio.sleep(0.002)
            if buf:
                out = defaultdict(list)
                for k, payload in buf:
                    out[part_of(k)].append(SendMessage(payload))
                n += len(buf)
                buf.clear()
                await asyncio.gather(*(iggy.send_messages(STREAM, UPDATES, Partitioning.partition_id(p), ms)
                                       for p, ms in out.items()))
            if time.monotonic() - t0 >= 10:
                print(f"[ws-bridge] {n / (time.monotonic() - t0):,.1f} order updates/s copied to '{UPDATES}'",
                      flush=True)
                n, t0 = 0, time.monotonic()

    sender = asyncio.create_task(flush())
    async for ws in websockets.connect(URL, ping_interval=20):   # reconnects on its own
        try:
            await ws.send(json.dumps(login))
            async for raw in ws:
                body = json.loads(raw)
                d = body.get("Data") if isinstance(body, dict) else None
                if d and d.get("ClientId"):
                    buf.append(to_update(d))
        except websockets.ConnectionClosed:
            continue
    sender.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--partner-id", help="partner ID (UserType PARTNER: every user on your platform)")
    p.add_argument("--secret", help="partner secret")
    p.add_argument("--client-id", help="Dhan client ID (UserType SELF)")
    p.add_argument("--token", help="access token for --client-id")
    p.add_argument("--url", default=DEFAULT_URL)
    a = p.parse_args()
    if not (a.partner_id and a.secret) and not (a.client_id and a.token):
        p.error("pass --partner-id and --secret, or --client-id and --token")
    asyncio.run(main(a))
