"""Shadow real metric samples into the Apache Iggy samples lane, read-only, from an OpenTelemetry Collector.

Listens for OTLP/HTTP with JSON encoding on --port and publishes every histogram data point of --metric as one
sample per bucket (le), keyed by shard key, the same shape gateway.py writes. Nothing in the existing path
changes: a collector that already exports to your ingestion endpoint gets a second exporter that points here,
so if this process stops, only the shadow stops.

    exporters:
      otlphttp/iggy-shadow:
        endpoint: http://<this-host>:4319
        encoding: json
    service:
      pipelines:
        metrics:
          exporters: [<your existing exporter>, otlphttp/iggy-shadow]

Mapping: service from the resource's service.name (the shard_label), pod from k8s.pod.name (or
service.instance.id), route from the point's http.route, cluster from --cluster. Cumulative points carry the
counter's value as sent; delta points are added up here into a running value per series, so the aggregator
always sees cumulative counters. The sequence is the point's timestamp in ms, so a collector retry or an older
point arriving late adds nothing. Bucket counts are made cumulative per le, as Prometheus exposes them.
In a shadow run only the lane is timed; the clock starts when the bridge receives the point.
Needs: pip install apache-iggy==0.9.0 (no other dependency). Run aggregator.py with your timeslice.
"""
import argparse
import asyncio
import gzip
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, SAMPLES, STREAM, ensure_topics, msg, part, shard_key


def attrs(kv: list) -> dict:
    out = {}
    for a in kv or []:
        v = a.get("value", {})
        out[a.get("key")] = next(iter(v.values()), "") if v else ""
    return out


def to_samples(body: dict, a, last_seq, running):
    """Yield (partition, sample bytes) for each bucket of each histogram point of the chosen metric."""
    for rm in body.get("resourceMetrics", []):
        res = attrs(rm.get("resource", {}).get("attributes"))
        svc = str(res.get("service.name", "unknown"))
        pod = str(res.get("k8s.pod.name") or res.get("service.instance.id") or "unknown")
        q = part(shard_key(a.cluster, svc), PARTITIONS[SAMPLES])
        for sm in rm.get("scopeMetrics", []):
            for m in sm.get("metrics", []):
                if m.get("name") != a.metric or "histogram" not in m:
                    continue
                delta = m["histogram"].get("aggregationTemporality") in (1, "AGGREGATION_TEMPORALITY_DELTA")
                for dp in m["histogram"].get("dataPoints", []):
                    route = str(attrs(dp.get("attributes")).get("http.route", ""))
                    seq = int(dp.get("timeUnixNano", 0)) // 1_000_000
                    bounds = [f"{b:g}" for b in dp.get("explicitBounds", [])] + ["+Inf"]
                    counts = [int(c) for c in dp.get("bucketCounts", [])]
                    total = 0
                    for le, c in zip(bounds, counts):
                        total += c                   # cumulative per le
                        key = (svc, pod, route, le)
                        if seq <= last_seq.get(key, -1):
                            continue                 # a retry or an older point: already sent
                        last_seq[key] = seq
                        v = total
                        if delta:
                            running[key] += total
                            v = running[key]
                        yield q, msg(c=a.cluster, sv=svc, id=f"{pod}/{route}", le=le, seq=seq, v=v, t=seq)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SAMPLES)
    producer = await iggy.producer(STREAM, SAMPLES, topic_partitions_count=PARTITIONS[SAMPLES])
    last_seq, running = {}, defaultdict(int)

    async def handle(reader, writer):
        try:
            while True:                              # keep-alive: several requests per connection
                line = await reader.readline()
                if not line:
                    break
                method, path = line.decode().split()[:2]
                headers = {}
                while (h := await reader.readline()) not in (b"\r\n", b"\n", b""):
                    k, _, v = h.decode().partition(":")
                    headers[k.strip().lower()] = v.strip()
                raw = await reader.readexactly(int(headers.get("content-length", 0)))
                if method != "POST" or path != "/v1/metrics" or "json" not in headers.get("content-type", ""):
                    code, reply = "415 Unsupported Media Type", b'{"error":"POST /v1/metrics with encoding: json"}'
                else:
                    if headers.get("content-encoding") == "gzip":
                        raw = gzip.decompress(raw)
                    groups = defaultdict(list)
                    for q, b in to_samples(json.loads(raw), a, last_seq, running):
                        groups[q].append(SendMessage(b))
                    # One batch per partition, all sent at once, so a shard key's samples stay in order.
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in groups.items()))
                    code, reply = "200 OK", b"{}"
                writer.write(f"HTTP/1.1 {code}\r\nContent-Type: application/json\r\n"
                             f"Content-Length: {len(reply)}\r\n\r\n".encode() + reply)
                await writer.drain()
        except (ValueError, ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async with producer:
        server = await asyncio.start_server(handle, a.host, a.port)
        print(f"[otlp_tee] OTLP/HTTP JSON on {a.host}:{a.port}/v1/metrics -> '{SAMPLES}' ({a.metric}, "
              f"cluster {a.cluster})", flush=True)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=4319)
    ap.add_argument("--cluster", default="prod-00", help="the cluster these samples belong to")
    ap.add_argument("--metric", default="http.server.request.duration", help="the histogram to shadow")
    ap.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(ap.parse_args()))
