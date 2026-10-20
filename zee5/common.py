"""Shared settings for the Continue Watching lane kit: playback heartbeats and resume positions on Apache Iggy."""
import json
import time
import zlib

STREAM = "playback"
HEARTBEATS = "heartbeats"            # the lane: one heartbeat per viewer every --every seconds while a video plays
RESUME_REQ = "resume-requests"       # a viewer opens a title on another device and asks where to resume
RESUME_ANS = "resume-answers"        # the Continue Watching service answers with a position
METADATA = "metadata"                # a busy neighbour: content publishing, for the release-drop test
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
DEVICES = ("android", "tv", "web", "ios")


def viewer(i: int) -> str:
    return f"U{i:09d}"


def part(key: str, partitions: int) -> int:
    # One partition per viewer, the same function for heartbeats, resume requests and answers in every script.
    return zlib.crc32(key.encode()) % partitions


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


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
