"""Shadow OpsAI's kubectl requests from your Pulsar cluster into the Apache Iggy command lane, read-only.

Attaches to the MCP topics with its own subscription ("iggy-shadow"): mcp-<token>-<sha1(cluster)> carries
OpsAI's requests and mcp-...-unsubscribe the agents' replies. The agents' "subscribe" subscription, OpsAI's
reply reader and the unsubscribe endpoint are untouched, and nothing is ever sent back to Pulsar.

  * Each request is written to kubectl-requests, keyed by its Pulsar topic (one per cluster), with the
    request ID, the command, read or write, and the deadline (publish time + timeout_seconds, default 45).
    Then run agents.py against it: it simulates kubectl (it never runs a command), so you can compare the
    lane's timings and deadline decisions with what your agents did.
  * Each reply is matched to its request by ID, and the bridge prints today's request-to-reply time on
    Pulsar (publish to publish, so kubectl's own time is included) as a baseline.

A Pulsar subscription keeps a backlog while its consumer is away: delete "iggy-shadow"
(pulsar-admin topics unsubscribe) when the shadow run ends.

Needs: pip install pulsar-client apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
import re
import time
from collections import defaultdict

import pulsar
from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, REQUESTS, STREAM, Window, clamp_timeout, ensure_topics, msg, part

WRITE_VERBS = ("apply", "patch", "scale", "rollout restart", "rollout undo", "set ", "delete", "edit",
               "label", "annotate", "cordon", "drain", "create")


def kind(cmd: str) -> str:
    c = cmd.removeprefix("kubectl ").strip()
    return "write" if any(c.startswith(v) for v in WRITE_VERBS) else "read"


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, REQUESTS)
    client = pulsar.Client(a.service_url, **({"authentication": pulsar.AuthenticationToken(a.token)} if a.token else {}))
    consumer = client.subscribe(re.compile(a.topics_pattern), subscription_name="iggy-shadow",
                                consumer_type=pulsar.ConsumerType.Shared,
                                initial_position=pulsar.InitialPosition.Latest,
                                pattern_auto_discovery_period=30)
    asked = {}                               # request ID -> publish ns, to time today's round trip
    baseline, stats = Window(), defaultdict(int)
    mark = time.monotonic()
    while True:
        batch = defaultdict(list)
        for _ in range(a.batch):
            try:
                rec = consumer.receive(timeout_millis=5)
            except pulsar.Timeout:
                break
            consumer.acknowledge(rec)       # acknowledges on the iggy-shadow subscription only
            topic = rec.topic_name().rsplit("/", 1)[-1]
            try:
                d = json.loads(rec.data())
            except ValueError:
                continue
            res = d.get("result") or {}
            pub_ns = rec.publish_timestamp() * 1_000_000
            if topic.endswith("-unsubscribe"):
                t = asked.pop(str(d.get("Id")), None)
                if t is not None:
                    baseline.add((pub_ns - t) / 1e6)
                    stats["replies"] += 1
                continue
            if d.get("Action") != "mcp-k8s" or "command" not in res:
                continue
            rid, cmd = str(d.get("Id")), str(res["command"])
            asked[rid] = pub_ns
            dl = pub_ns + int(clamp_timeout(float(res.get("timeout_seconds") or 0)) * 1e9)
            batch[part(topic, PARTITIONS[REQUESTS])].append(SendMessage(msg(
                ns=pub_ns, id=rid, c=topic, cmd=cmd, k=kind(cmd), dl=dl, sim=[a.sim_s, a.sim_s])))
            stats["requests"] += 1
        if batch:
            await asyncio.gather(*(iggy.send_messages(STREAM, REQUESTS, Partitioning.partition_id(p), ms)
                                   for p, ms in batch.items()))
        if time.monotonic() - mark >= 1:
            b = baseline.flush()
            print(f"[bridge] {stats['requests']:,} requests/s copied into {STREAM}/{REQUESTS}  "
                  f"today's request-to-reply on Pulsar (incl. kubectl) n {b['n']:,} p50 {b['p50']:.0f}ms "
                  f"p99 {b['p99']:.0f}ms", flush=True)
            stats.clear()
            mark = time.monotonic()
            cutoff = time.time_ns() - 600 * 10**9
            for k in [k for k, t in asked.items() if t < cutoff]:
                del asked[k]                # never answered within 10 minutes
        await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--service-url", required=True, help="pulsar+ssl://… of the cluster behind the MCP topics")
    p.add_argument("--topics-pattern", default=r"persistent://public/default/mcp-.*",
                   help="requests and replies (…-unsubscribe)")
    p.add_argument("--token", default=None, help="JWT for token authentication, if you use it")
    p.add_argument("--sim-s", type=float, default=0.2, help="kubectl time agents.py simulates for mirrored requests")
    p.add_argument("--batch", type=int, default=2000)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
