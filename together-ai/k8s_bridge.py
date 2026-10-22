"""Shadow your GPU nodes' health conditions from Kubernetes into the Apache Iggy node-signals lane, read-only.

Watches Node objects (list and watch only; RBAC: get, list, watch on nodes). Nothing is written to the cluster:
no labels, no taints, no cordons. Node Problem Detector, the kubelet and GPU health exporters already set node
conditions; for each node the bridge publishes one sample whenever its conditions change, and again every
--resync-s seconds, with the active signals by name, the same shape fleet.py writes. Point health_checker.py at
Iggy and compare its HOLD and OK decisions and their timing with your own alerts and repair recommendations.

Map your conditions in SIGNAL_OF: condition type -> signal name. The defaults map the upstream Node Problem
Detector and kubelet conditions; add the condition types your GPU detectors set (for example one per Xid, ECC,
fallen-off-bus and InfiniBand check). Run one bridge per cluster. In a shadow run only the lane is timed; the clock
starts when the bridge sees the condition change, not when the detector found it.
Needs: pip install kubernetes apache-iggy==0.9.0 (uses your kubeconfig, or the in-cluster service account).
"""
import argparse
import asyncio
import threading
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from kubernetes import client as k8s, config, watch

from common import DEFAULT_URL, PARTITIONS, SIGNALS, STREAM, ensure_topics, msg, part

SIGNAL_OF = {
    "KernelDeadlock": "NpdKernelDeadlock",
    "ReadonlyFilesystem": "NpdReadonlyFilesystem",
    "FrequentKubeletRestart": "NpdFrequentKubeletRestart",
    "FrequentContainerdRestart": "NpdFrequentContainerdRestart",
    "FrequentUnregisterNetDevice": "NpdFrequentUnregisterNetDevice",
    "MemoryPressure": "KubeNodeMemoryPressure",
    "DiskPressure": "KubeNodeDiskPressure",
    "PIDPressure": "KubeNodePIDPressure",
    # your GPU and fabric detectors' condition types go here, e.g. "GpuFallenOffBus": "DmesgGpuFallenOffBus"
}


def signals(node) -> list:
    out = []
    for c in node.status.conditions or []:
        if c.status == "True" and c.type in SIGNAL_OF:
            out.append({"s": SIGNAL_OF[c.type]})
    return sorted(out, key=lambda s: s["s"])


def watch_nodes(q: "asyncio.Queue", loop, a):
    """Runs in a thread: the Kubernetes client is blocking. Puts (node, signals) on the queue on every change."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config(context=a.context)
    api = k8s.CoreV1Api()
    while True:
        try:
            for ev in watch.Watch().stream(api.list_node, label_selector=a.selector, timeout_seconds=300):
                n = ev["object"]
                loop.call_soon_threadsafe(q.put_nowait, (n.metadata.name, signals(n)))
        except Exception as e:              # a dropped watch is re-listed, as kubectl does
            print(f"[bridge] watch restarted: {e}", flush=True)
            time.sleep(1)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SIGNALS)
    producer = await iggy.producer(STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS])
    q, loop = asyncio.Queue(), asyncio.get_running_loop()
    threading.Thread(target=watch_nodes, args=(q, loop, a), daemon=True).start()
    seq, last, sent_at = defaultdict(int), {}, {}
    async with producer:
        while True:
            groups = defaultdict(list)
            try:
                k, sig = await asyncio.wait_for(q.get(), timeout=0.05)
                last[k] = sig
                pending = [(k, sig)] + [q.get_nowait() for _ in range(q.qsize())]
            except asyncio.TimeoutError:
                pending = []
            now = time.monotonic()
            for k, sig in pending:
                last[k] = sig
            due = {k for k, _ in pending} | {k for k, t in sent_at.items() if now - t >= a.resync_s}
            for k in due:
                seq[k] += 1
                sent_at[k] = now
                groups[part(k, PARTITIONS[SIGNALS])].append(SendMessage(msg(k=k, seq=seq[k], sig=last[k])))
            # One batch per partition, all sent at once, so a node's samples stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--context", default=None, help="kubeconfig context of the GPU cluster")
    p.add_argument("--selector", default="", help="label selector for the GPU nodes, e.g. nvidia.com/gpu.present=true")
    p.add_argument("--resync-s", type=float, default=1.0, help="re-send each node's current signals this often")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
