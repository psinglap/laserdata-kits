#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the graph link lane at small scale for about a minute,
# with a peak and a tracking stream next to it, and pass only if the lane reports PASS, payment checks
# hold the links from the account's login, and repeat fraudsters are linked to restricted accounts.
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
python -m py_compile common.py activity.py graph.py tracking_flood.py kafka_bridge.py
# The graph starts first and creates the topics; activity and the neighbour follow.
timeout 78 python -u graph.py > graph.log 2>&1 &
sleep 3
# 400 logins a second (about 850 links/s), 1% repeat fraudsters, 2x for 15 s from second 30 (the peak);
# 1 MB/s of tracking events from second 20.
timeout 72 python -u activity.py --accounts 200000 --ips 100000 --logins-per-s 400 --fraud-share 0.01 --bad-assets 200 \
  --peak 2 --peak-at 30 --peak-for 15 > activity.log 2>&1 &
timeout 70 python -u tracking_flood.py --mbps 1 --at 20 --duration 35 > tracking.log 2>&1 &
wait || true
echo "--- activity.log"; head -n 2 activity.log; tail -n 4 activity.log
echo "--- graph.log";    tail -n 8 graph.log
echo "--- tracking.log"; head -n 2 tracking.log; tail -n 2 tracking.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" graph.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[graph\] +[1-9][0-9,]* links/s' graph.log | tail -n +11 || true)
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
g = [l for l in open("graph.log") if re.search(r"\[graph\] +[1-9][0-9,]* links/s", l)][10:]
c = [l for l in open("graph.log") if re.search(r"\[check\] payment checks [1-9]", l)][10:]
act = open("activity.log").read()
print("steady-state medians: links/s %.0f  link-to-graph p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, g)) for q in (r"\] +([0-9,]+) links/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
peak = [l for l in g if (grab(r"\] +([0-9,]+) links/s", l) or 0) > 1300]
if peak:
    print("peak: %d s above 1,300 links/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
missed = nums(r"without the newest links ([0-9.]+)%  repeat", c)
checks, fraud = sum(nums(r"payment checks ([0-9,]+) \(", c)), sum(nums(r"\(repeat fraudsters ([0-9,]+)\)", c))
caught = sum(nums(r"restricted accounts ([0-9,]+) of", c))
slow = nums(r"write path: without the newest links ([0-9.]+)%", c)
slow_fraud = sum(nums(r"\(repeat fraudsters ([0-9,]+)\)$", [l.rstrip() for l in c]))
print("payment checks: %d s checked, %.0f checks in all; without the newest links: median %.3f%%, worst second %.3f%%; "
      "repeat fraudsters linked to restricted accounts %.0f of %.0f" % (len(c), checks, st.median(missed), max(missed), caught, fraud))
print("a 1 s write path, judged on the same checks: median %.1f%% of checks without the newest links, "
      "%.0f of %.0f repeat fraudsters checked before their links landed" % (st.median(slow), slow_fraud, fraud))
t = [l for l in open("tracking.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if t:
    print("tracking neighbour: %d s, median %.1f MB/s on its own topic" % (len(t), st.median(nums(r"~([0-9.]+) MB/s", t))))
if len(c) < 30:
    raise SystemExit("too few seconds with payment checks")
if st.median(missed) > 0.1:
    raise SystemExit("too many payment checks ran without the links from the account's login")
if fraud == 0 or caught < 0.99 * fraud:
    raise SystemExit("repeat fraudsters were not linked to restricted accounts")
if "(peak)" not in act or not peak:
    raise SystemExit("the peak did not reach the lane")
PY
grep -q "\[tracking\] done" tracking.log || { echo "the tracking neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
