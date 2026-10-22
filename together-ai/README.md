# Real-time GPU node health for Together AI, on LaserData

Every passive health signal reaches the health checker, and every hold reaches every placement scheduler, in
milliseconds, before the next placement. It runs beside your control plane, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_TogetherAI_NodeHealth.pdf`](LaserData_TogetherAI_NodeHealth.pdf)

## Why this use case

From Together's GPU Clusters posts: passive health checks run continuously on every node and "work like alerts",
catching GPUs fallen off the PCIe bus, thermal throttling, Xid errors and Slurm drains; a repair is detected,
recommended, approved by a person, then run, because "your training checkpoints and inference replicas are too
valuable to risk on automated drains" (July 2026); "a single node failure can invalidate an entire job run"
(March 2026). From Together's docs: each signal maps to a repair action (reboot for an Xid or an uncorrectable ECC
error, migrate for a GPU fallen off the bus or InfiniBand link flapping, quick reprovision for a fatal platform
error), and under Approve a repair waits 30 minutes by default. From Together's 2026 job postings: the compute
infrastructure team is building the Kubernetes-native control plane for the GPU inference fleet, with bin-packing
and rebalancing, and asks for "event-driven health, remediation, and utilization systems" on Kafka, NATS or SQS
rather than "polling or cron-driven scripts"; the fleet is "tens of thousands of GPUs".

In the flow the docs describe, the cordon starts once a repair is approved, so until then a flagged node can still
be picked for new work, and a node whose pods just failed looks like it has free GPUs. A hold is a loop with two hops: each node's sample has to reach the checker that owns
that node, and its HOLD has to reach every placement scheduler. This kit runs that one loop on Apache Iggy, uses
your signal names and repair actions, prints both hops' latency every second, and counts the placements that
landed on a node after it was already on hold. The drain itself stays human-approved.

Together has not published how its passive-check signals travel to the alert, how often they are evaluated, or
whether placement already avoids a node with an open alert; the shadow run on one cluster is how to compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 4,000 GPU nodes, each sending a passive health sample once a second
(4,000 samples/s), 8 placement schedulers placing 100 replicas and jobs a second on the 64 nodes with free GPUs,
and a planted fault every 10 s on one of those nodes, lasting 20 s until it is repaired. A faulted node draws 30%
of placements, because its GPUs look free. All illustrative; set them in `common.py` and the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python health_checker.py &           # the checker, fed by the node-signals lane
python fleet.py                      # node agents out, holds in, 8 placement schedulers
```

They print a line a second, and one per fault:

```
[checker]  4,000 samples/s  sample-to-checker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent … (hold …, ok …)
[fleet]    4,000 samples/s  placements 100  on a faulted node …, after its hold was out … (at 15000 ms it would be …)
           hold-to-schedulers n … p50 …ms p99 …ms
[fleet] incident gpu-node-01734 DmesgGpuFallenOffBus: onset to hold … ms (recommend migrate),
        hold to all 8 schedulers … ms, placements that landed on it first …
```

"At 15000 ms it would be …" counts the placements that, after a HOLD was published and while the node was still
faulted, picked that node within 15 seconds: what a path that polls or evaluates alerts every 15 s would have let
through, judged on the same picks. Set `--compare-ms`. "Onset to hold" is detection, set by the sample interval
(1 s) and the checker's rule (`decide()` in `health_checker.py`: a fault signal holds at once; thermal throttle,
PCIe replays and IB link flapping hold after 3 samples in 5 s), not by the broker.

**What the numbers mean.** The kit times each sample from the moment `fleet.py` creates it to the moment `health_checker.py` has applied it, and each hold from the checker to every scheduler, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (2,000 nodes, 2,000 samples/s, 4 schedulers, 100 placements/s), three green runs held sample-to-checker p50 4.3–9.2 ms, p99 7.6–15.8 ms and p99.9 8.3–17.1 ms (every steady-state second inside 50 ms, 62 of 62 per run), and hold-to-schedulers p50 2.8–3.7 ms and p99 2.8–3.9 ms. The checker reads the 16 signal partitions directly, on two connections. With 2 MB/s of GPU telemetry on the same broker, the lane's p99 moved by less than 1 ms. The six planted faults per run were put on hold a median 590–654 ms after onset (64–898 ms for fault signals, 2.3–2.9 s for condition signals that need three samples) and reached all four schedulers a median 2.7–4.0 ms later (at most 6.0 ms). Of 1,391–1,436 placements per run that a 15 s path would still have put on a faulted node, none landed on this lane; the 106–136 placements per run that did land on a faulted node all came before its hold existed, which only a faster sample interval or a sharper rule shortens. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The nodes and faults in the kit are simulated; your detectors add their own time. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39874215658, https://github.com/psinglap/laserdata-kits/actions/runs/39874467526, https://github.com/psinglap/laserdata-kits/actions/runs/39874677386

| File | What it is |
|---|---|
| `fleet.py` | Node agents and placement schedulers. Every node publishes a passive health sample to `node-signals`, keyed by node, with the active signals by Together's names. Each scheduler reads `node-health` directly on its own connection and re-picks while a node is on HOLD. Plants the faults. |
| `health_checker.py` | The checker. Holds a node at once on a fault signal and after 3 samples in 5 s on a condition signal (`decide()` is where your rule goes), attaches the repair action from Together's docs, clears after 3 clean samples, and rebuilds every node's state from the log after a restart. Prints the lane's p99. |
| `common.py` | Topics, the signal-to-action table from Together's node repair docs, and the node key. |
| `telemetry_flood.py` | DCGM metrics and kernel-log lines on their own topic, for the isolation test. |
| `k8s_bridge.py` | Watches Kubernetes node conditions (list and watch only) and publishes each node's signals into Iggy, for a shadow run on a real cluster. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Fleet scale:** `python health_checker.py --readers 4 &` then `python fleet.py --nodes 8000 --schedulers 16`.
   Pass: sample-to-checker p99 under 50 ms, and under 1% of the placements a 15 s path would let through land on a
   node after its hold.
2. **The telemetry firehose next door:** `python telemetry_flood.py --mbps 50` while both run.
   Pass: the node-signals lane's p99 moves by less than 10%.
3. **Restart the checker mid-fault:** stop `health_checker.py` while a node is on hold and start it again; it
   replays the node-signals topic, rebuilds every node's state, and only publishes decisions for samples from the
   last 10 seconds. Pass: it catches up within seconds, no sample is applied twice, and the held node stays on
   hold until it reports clean again.

## Shadow your own cluster

```
pip install kubernetes
python k8s_bridge.py --context <gpu-cluster> --selector nvidia.com/gpu.present=true
```

The bridge only lists and watches Node objects; it sets no labels, taints or cordons, and the detectors, alerts,
repair flow and scheduler are untouched. Map your condition types to signal names in `SIGNAL_OF`: the defaults
cover the upstream Node Problem Detector and kubelet conditions; add the ones your GPU and fabric detectors set.
Run one bridge per cluster, then compare `health_checker.py`'s holds and their timing with your own alerts and
repair recommendations.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and nothing else changes.
Delivery is at-least-once (decisions carry the state and samples carry a sequence, so repeats are harmless).
Replicated clusters (Viewstamped Replication) shipped in September 2026. There is no cross-region replication in
0.9: run one small lane per data center, next to its control plane. Apache Iggy is a log, not a workflow engine:
durable workflows (your postings name Temporal) keep provisioning and repair.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one cluster's node signals, read-only, beside your
control plane. No advantage, no migration.

kranti@laserdata.com · laserdata.com
