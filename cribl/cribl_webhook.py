"""Shadow a Route's events from Cribl Stream into Apache Iggy through a Webhook Destination.

Cribl Stream has no Apache Iggy Destination today, so this small receiver stands in for one. Add a
Webhook Destination to the Route you want to shadow (a Route or Output Router rule with Final off,
so your existing Destinations keep every event), pointed at http://<this host>:10080/, method POST,
format NDJSON (the default; JSON Array works too), compression on or off. For a shadow trial set
its Backpressure behavior to Drop, not Block: Cribl Stream's docs say a Destination set to Block that
enters backpressure stops the Source for every Destination that receives the same events.

Each request's events are written to one Apache Iggy topic, keyed by `host`, with a per-host
sequence and the receive time, and the request is answered 200 only after Apache Iggy has the
events. If Apache Iggy can't take them, it answers 503, which the Webhook Destination retries (and
holds in its persistent queue, if you enabled one). Run one receiver per Worker Group: the sequence
is per host per receiver.

Then run: python destination.py --dest splunk_lb   (and one per Destination you want to compare).

Needs: pip install aiohttp apache-iggy==0.9.0
Auth: with --token, requests must carry it in Authorization (as the Webhook Destination's Auth token sends it).
"""
import argparse
import asyncio
import gzip
import json
import time
from collections import defaultdict

from aiohttp import web
from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TOPIC, connect, ensure_topic, msg, part


def events(body: bytes):
    if body[:2] == b"\x1f\x8b":                     # gzip, if the server didn't already undo it
        body = gzip.decompress(body)
    text = body.decode().strip()
    if text.startswith("["):                         # JSON Array format
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, STREAM, a.topic, a.partitions)
    seq = defaultdict(int)
    lock = asyncio.Lock()                            # keeps each host's sequence in send order
    stats = defaultdict(int)

    async def receive(request):
        if a.token and a.token not in request.headers.get("Authorization", ""):
            return web.Response(status=401)
        try:
            evs = events(await request.read())
        except (ValueError, OSError):
            stats["bad"] += 1
            return web.Response(status=400, text="expected NDJSON or a JSON array of events")
        now = time.time_ns()
        async with lock:
            batch = defaultdict(list)
            for e in evs:
                h = str(e.get("host") or "unknown")
                seq[h] += 1
                batch[part(h, a.partitions)].append(SendMessage(msg(
                    ns=now, host=h, sourcetype=e.get("sourcetype", ""), seq=seq[h], _time=e.get("_time"),
                    _raw=e.get("_raw", ""))))
            try:
                await asyncio.gather(*(c.send_messages(STREAM, a.topic, Partitioning.partition_id(pid), ms)
                                       for pid, ms in batch.items()))
            except RuntimeError:
                for e in evs:                        # not written: Cribl Stream will send them again
                    seq[str(e.get("host") or "unknown")] -= 1
                stats["refused"] += len(evs)
                return web.Response(status=503, text="Apache Iggy unavailable, retry")
        stats["events"] += len(evs)
        stats["requests"] += 1
        return web.Response(status=200)

    async def report(_app):
        async def loop():
            while True:
                await asyncio.sleep(1)
                if stats:
                    print(f"[cribl-webhook] {stats['requests']:,} requests  {stats['events']:,} events/s written to "
                          f"{STREAM}/{a.topic}  refused {stats['refused']:,}  bad {stats['bad']:,}", flush=True)
                stats.clear()
        t = asyncio.create_task(loop())
        yield
        t.cancel()

    app = web.Application(client_max_size=a.max_mb * 1024 * 1024)
    app.router.add_route("*", "/{tail:.*}", receive)
    app.cleanup_ctx.append(report)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, a.host, a.port).start()
    print(f"[cribl-webhook] listening on http://{a.host}:{a.port}/ -> {STREAM}/{a.topic} ({a.partitions} partitions)",
          flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=10080)
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--partitions", type=int, default=16, help="same as workers.py")
    p.add_argument("--token", default="", help="require this token in the Authorization header")
    p.add_argument("--max-mb", type=int, default=64, help="largest request body accepted (Webhook default is 4 MB)")
    asyncio.run(main(p.parse_args()))
