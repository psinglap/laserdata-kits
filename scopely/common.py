"""Shared settings for the MONOPOLY GO! race lane kit: every dice roll in a team race reaches the race owner that
holds that race in milliseconds, and its laps, finishes and race ends reach every game server, on Apache Iggy."""
import asyncio
import json
import random
import time
import zlib

STREAM = "monopoly-go-races"
ROLLS = "race-rolls"            # the lane: every dice roll in a race, keyed by race, with the player's race total
UPDATES = "race-updates"        # the race owners' laps, finishes and race ends, keyed by race, to every game server
ANALYTICS = "client-analytics"  # a heavy, latency-tolerant analytics stream on the same broker
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true"   # Nagle off: small writes go out at once
PARTITIONS = {ROLLS: 16, UPDATES: 2, ANALYTICS: 32}

TEAMS = 4                       # a race: your team of 4 against three other teams (Scopely's Tycoon Racers post)
PLAYERS = 4
MULTIPLIERS = (1, 1, 1, 2, 2, 3, 5, 10)   # the dice multiplier a player picks (illustrative mix)


def race(i: int) -> str:
    return f"race-{i:06d}"


def player(race_i: int, team: int, k: int) -> str:
    return f"p{race_i:06d}{team}{k}"


def part(key: str, partitions: int) -> int:
    # One partition per race, the same function in every script, so a race's rolls stay in one order.
    return zlib.crc32(key.encode()) % partitions


def roll_points() -> int:
    """Points from one tap of the dice popper: the sum of two dice times the multiplier (illustrative)."""
    return (random.randint(1, 6) + random.randint(1, 6)) * random.choice(MULTIPLIERS)


def next_gap() -> float:
    """Seconds until a player's next roll while their race is on (illustrative; set it to yours)."""
    return random.uniform(0.5, 4.0)


MEAN_GAP = 2.25


class Race:
    """One race as the log decides it. The race owner (race_owner.py) and the truth check in game_servers.py
    both run exactly this on the rolls in log order, so they agree on every lap, finish and place.

    A roll carries the player's points in this race so far (the resulting value, never only "+points") and the
    player's roll sequence, so a roll the client sent twice (a retry) or a roll replayed from the log changes
    nothing, and a roll for a race that already ended, or from a team that already finished, scores nothing."""
    __slots__ = ("rn", "last", "ptot", "ttot", "laps", "done")

    def __init__(self):
        self.rn = 1                 # race number on this track: 1, 2, 3 ... (a new race starts when one ends)
        self.last = {}              # player -> last roll sequence applied
        self.ptot = {}              # player -> points in this race
        self.ttot = [0] * TEAMS     # team -> points in this race
        self.laps = [0] * TEAMS
        self.done = []              # teams in finishing order

    def apply(self, d: dict, track: int, lap: int):
        """Returns (outcome, events). outcome: 'ok', 'retry' (already applied), 'ended' (race already over),
        'finished' (the team already crossed the line). events: list of (kind, team, value)."""
        p, tm = d["p"], d["tm"]
        if d["seq"] <= self.last.get(p, 0):
            return "retry", ()
        self.last[p] = d["seq"]
        if d["rn"] != self.rn:
            return "ended", ()
        if tm in self.done:
            return "finished", ()
        self.ttot[tm] += d["tot"] - self.ptot.get(p, 0)
        self.ptot[p] = d["tot"]
        ev = []
        laps = min(self.ttot[tm], track) // lap
        if laps > self.laps[tm]:
            self.laps[tm] = laps
            ev.append(("lap", tm, laps))
        if self.ttot[tm] >= track:
            self.done.append(tm)
            ev.append(("finish", tm, len(self.done)))
            if len(self.done) == TEAMS - 1:          # three teams home: the last team takes 4th, the race ends
                self.done += [t for t in range(TEAMS) if t not in self.done]
                ev.append(("end", -1, list(self.done)))
                self.rn += 1
                self.ptot.clear()
                self.ttot = [0] * TEAMS
                self.laps = [0] * TEAMS
                self.done = []
        return "ok", ev


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
