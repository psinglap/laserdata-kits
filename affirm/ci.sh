#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the customer aggregate lane at small scale for about a minute,
# with a peak and a history backfill next to it, and pass only if the lane reports PASS, checkouts soon after
# a loan event find the stored aggregate current, and redelivered aggregates never replace newer ones.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after the heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py lending.py features.py backfill.py kafka_bridge.py
# The feature store starts first and creates the topics; the system of record and the neighbour follow.
timeout 78 python -u features.py > features.log 2>&1 &
sleep 3
# 20,000 returning customers, 400 loan events a second (a quarter followed by a checkout within 0.05-300 s),
# 150 other checkouts a second, 1% of aggregates redelivered, 3x for 15 s from second 30 (the peak);
# 1 MB/s of history backfill from second 20.
timeout 72 python -u lending.py --customers 20000 --events-per-s 400 --checkouts-per-s 150 \
  --peak 3 --peak-at 30 --peak-for 15 > lending.log 2>&1 &
timeout 70 python -u backfill.py --mbps 1 --at 20 --duration 35 > backfill.log 2>&1 &
wait || true
echo "--- lending.log";  head -n 2 lending.log; tail -n 4 lending.log
echo "--- features.log"; tail -n 8 features.log
echo "--- backfill.log"; head -n 2 backfill.log; tail -n 2 backfill.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" features.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[features\] +[1-9][0-9,]* changes/s' features.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
def grab(pat, l):
    m = re.search(pat, l)
    return float(m.group(1).replace(",", "")) if m else None
def nums(pat, ls):
    return [v for v in (grab(pat, l) for l in ls) if v is not None]
f = [l for l in open("features.log") if re.search(r"\[features\] +[1-9][0-9,]* changes/s", l)][10:]
c = [l for l in open("features.log") if re.search(r"\[checkout\] reads [1-9]", l)][10:]
lend = open("lending.log").read()
print("steady-state medians: changes/s %.0f  change-to-store p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, f)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
peak = [l for l in f if (grab(r"\] +([0-9,]+) changes/s", l) or 0) > 700]
if peak:
    print("peak: %d s above 700 changes/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
recent, cur, rec, una = (nums(q, c) for q in (r"loan event ([0-9,]+): current", r"loan event [0-9,]+: current ([0-9.]+)%",
                                              r"recompute ([0-9.]+)%", r"unavailable ([0-9.]+)%"))
n = sum(recent)
w = lambda xs: sum(r * x for r, x in zip(recent, xs)) / n if n else 0
print("checkouts within 300 s of a loan event: %d s checked, %.0f reads; current %.3f%% (worst second %.2f%%), "
      "recompute %.3f%%, unavailable %.3f%%" % (len(recent), n, w(cur), min(cur) if cur else 0, w(rec), w(una)))
wp = {k: sum(nums(k + r" ([0-9,]+)", f)) for k in ("first", "newer", "schema-up", "duplicate", "older")}
print("write path over the steady seconds: " + "  ".join("%s %.0f" % kv for kv in wp.items()))
b = [l for l in open("backfill.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if b:
    print("backfill neighbour: %d s, median %.1f MB/s on its own topic" % (len(b), st.median(nums(r"~([0-9.]+) MB/s", b))))
if len(recent) < 30 or n < 1000:
    raise SystemExit("too few checkouts soon after a loan event")
if w(cur) < 99.5:
    raise SystemExit("too many checkouts soon after a loan event found the stored aggregate behind")
if wp["duplicate"] + wp["older"] == 0 or wp["first"] == 0:
    raise SystemExit("redelivered aggregates or first-time customers never reached the write path")
if "(peak)" not in lend or not peak:
    raise SystemExit("the peak did not reach the lane")
PY
grep -q "\[backfill\] done" backfill.log || { echo "the backfill neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
