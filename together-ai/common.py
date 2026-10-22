"""Shared settings for the GPU node health lane kit: every passive health sample from every GPU node reaches the
health checker in milliseconds, and its hold and clear decisions reach every placement scheduler before their next
placement, on Apache Iggy."""
import asyncio
import json
import time
import zlib

STREAM = "gpu-fleet"
SIGNALS = "node-signals"       # the lane: one passive health sample per node per interval, keyed by node
HEALTH = "node-health"         # the checker's decisions back to every scheduler: hold (with a repair action) or ok
TELEMETRY = "gpu-telemetry"    # a heavy, latency-tolerant DCGM metrics and kernel-log stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {SIGNALS: 16, HEALTH: 1, TELEMETRY: 32}

HOLD = "hold"                  # no new placements on this node; a repair recommendation waits for approval
OK = "ok"                      # placements may land on it again

# Signal names and repair actions as Together's node repair and health check docs list them
# (docs.together.ai/docs/node-repair, /docs/health-checks). The thresholds below are the kit's, not Together's.
MIGRATE, REPROVISION, REBOOT, WARN = "migrate", "quick-reprovision", "reboot", "warning"
ACTION = {
    "DmesgGpuFallenOffBus": MIGRATE,
    "GpuSmClockThermalThrottle": MIGRATE,
    "GpuPcieReplayRateHigh": MIGRATE,
    "IBRailsDownOrDegraded": MIGRATE,
    "IBLinkFlapping": MIGRATE,
    "NpdCperHardwareErrorFatal": REPROVISION,
    "NpdReadonlyFilesystem": REPROVISION,
    "NpdXfsShutdown": REPROVISION,
    "DmesgXidError": REBOOT,               # Xid 79 (fallen off the bus) migrates instead
    "GpuEccDoubleBitError": REBOOT,
    "GpuRowRemapFailure": REBOOT,
    "NpdKernelDeadlock": REBOOT,
    "KubeNodeMemoryPressure": REBOOT,
    "KubeNodeDiskPressure": WARN,           # warning only: no hold, no repair
}
# Signals that describe a condition rather than a single fault: held only once seen in several samples.
SUSTAINED = {"GpuSmClockThermalThrottle", "GpuPcieReplayRateHigh", "IBLinkFlapping", "KubeNodeMemoryPressure"}
# The faults the fleet plants, in turn (illustrative mix).
PLANTED = ("DmesgXidError", "GpuEccDoubleBitError", "DmesgGpuFallenOffBus", "IBLinkFlapping",
           "GpuSmClockThermalThrottle", "NpdKernelDeadlock", "IBRailsDownOrDegraded", "GpuRowRemapFailure")


def action_for(sig: str, xid: int = 0) -> str:
    return MIGRATE if sig == "DmesgXidError" and xid == 79 else ACTION.get(sig, WARN)


def node_id(i: int) -> str:
    return f"gpu-node-{i:05d}"


def part(k: str, partitions: int) -> int:
    # One partition per node, the same function in every script, so a node's samples stay in order.
    return zlib.crc32(k.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


async def ensure_topics(client, *topics):
    """Create the stream and topics if they are missing, so any script can start first."""
    if await client.get_stream(STREAM) is None:
        try:
            await client.create_stream(STREAM)
        except RuntimeError:
            pass                                    # created by another script in the meantime
    for t in topics:
        if await client.get_topic(STREAM, t) is None:
            try:
                await client.create_topic(STREAM, t, PARTITIONS[t])
            except RuntimeError:
                pass
    while not all([await client.get_topic(STREAM, t) for t in topics]):
        await asyncio.sleep(0.5)


class Window:
    def __init__(self):
        self.samples = []

    def add(self, v):
        self.samples.append(v)

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples = []
        return out
