"""Shared settings for the task progress lane kit: every step of a task the reasoning layer runs (tool call
started, tool call returned, result ready) reaches the voice host serving that call in milliseconds, so what the
conversational layer says next ("still checking later flights", "you're rebooked on the 18:40") is current, on
Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "voice-agents"
TASKS = "task-events"           # the lane: every step of every task, keyed by call, read by the call's voice host
LOGS = "conversation-log"       # a heavy, latency-tolerant conversation log (transcripts, tool traces) on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {TASKS: 16, LOGS: 32}

# Tasks the reasoning layer runs behind a call, as the steps the caller can be told about (illustrative: your
# Agent Operating Procedures and tools decide the real ones). The last step is the result.
TASK_STEPS = {
    "rebook_flight": ("looking up the booking", "checking later flights", "holding a seat on the 18:40",
                      "rebooked on the 18:40"),
    "refund_status": ("finding the order", "checking the refund with payments", "refund issued"),
    "replace_card": ("verifying identity", "freezing the card", "ordering a replacement",
                     "replacement card on its way"),
    "dispute_charge": ("finding the charge", "checking the merchant record", "dispute opened"),
    "reschedule_delivery": ("finding the delivery", "checking open slots", "moved to Tuesday 9:00-11:00"),
}
TASK_NAMES = tuple(TASK_STEPS)
STEP_MS = (400, 2500)           # one tool call or lookup behind the conversation (illustrative; set it to yours)
IDLE_S = (4.0, 12.0)            # conversation between tasks: the caller talks, the agent answers (illustrative)

RUNNING, DONE = "running", "done"


def call_id(i: int) -> str:
    return f"call-{i:07d}"


def part(call: str, partitions: int) -> int:
    # One partition per call, the same function in every script, so a call's task steps stay in order.
    return zlib.crc32(call.encode()) % partitions


def host_of(pid: int, hosts: int) -> int:
    # A call's audio session lives on one voice host; that host reads the call's partition and nothing else.
    return pid % hosts


def step_gap() -> float:
    return random.uniform(*STEP_MS) / 1000


def idle_gap() -> float:
    return random.uniform(*IDLE_S)


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
