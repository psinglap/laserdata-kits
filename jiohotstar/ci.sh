#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the playback health lane at small scale for about a minute,
# with three injected CDN incidents, a death-overs surge and full heartbeats next to it. Pass only if both
# hops report PASS, the incidents reach routing and clear, and routing rarely sends a play to a flagged CDN.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py heartbeats.py qos.py routing.py full_flood.py kafka_bridge.py
# The health check and routing start first and create the topics; heartbeats and the neighbour follow.
timeout 78 python -u qos.py > qos.log 2>&1 &
timeout 78 python -u routing.py --plays-per-s 500 > routing.log 2>&1 &
sleep 3
# 800 heartbeats a second of ~1.1 KB, 2.5x for 15 s from second 30 (the death overs); incidents at 12, 30
# and 48 s, 9 s each; 2 MB/s of full ~7.5 KB heartbeats from second 20.
timeout 72 python -u heartbeats.py --heartbeats-per-s 800 --surge 2.5 --surge-at 30 --surge-for 15 \
  --incident-at 12 --incident-every 18 --incident-for 9 > heartbeats.log 2>&1 &
timeout 70 python -u full_flood.py --mbps 2 --at 20 --duration 35 > full.log 2>&1 &
wait || true
echo "--- heartbeats.log"; head -n 2 heartbeats.log; grep "incident" heartbeats.log || true; tail -n 3 heartbeats.log
echo "--- qos.log"; grep -E "\] (degraded|healthy) " qos.log || true; tail -n 4 qos.log
echo "--- routing.log"; grep -E "\] (degraded|healthy) " routing.log || true; tail -n 4 routing.log
echo "--- full.log"; head -n 2 full.log; tail -n 2 full.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" qos.log routing.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
check() {  # $1 log, $2 pattern of a live second
  local steady live fail
  steady=$(grep -E "$2" "$1" | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$1 steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed: $1"; exit 1; fi
}
check qos.log '\[qos\] +[1-9][0-9,]* heartbeats/s'
check routing.log '\[routing\] +[1-9][0-9,]* signals/s'
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
q = [l for l in open("qos.log") if re.search(r"\[qos\] +[1-9][0-9,]* heartbeats/s", l)][10:]
r = [l for l in open("routing.log") if re.search(r"\[routing\] +[1-9][0-9,]* signals/s", l)][10:]
for name, ls, what in (("heartbeat-to-check", q, r"\] +([0-9,]+) heartbeats/s"), ("check-to-routing", r, r"\] +([0-9,]+) signals/s")):
    print("%s steady-state medians: %.0f msgs/s  p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  (worst p99 %.1f ms, %d s)" % (
        (name, st.median(num(what, ls))) + tuple(st.median(num(p, ls)) for p in (r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms"))
        + (max(num(r"p99 +([0-9.]+)ms", ls)), len(ls))))
surge = [l for l in q if float(re.search(r"\] +([0-9,]+) heartbeats", l).group(1).replace(",", "")) > 1500]
if surge:
    print("death overs: %d s above 1,500 heartbeats/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(surge), st.median(num(r"p99 +([0-9.]+)ms", surge)), max(num(r"p99 +([0-9.]+)ms", surge))))
flags = [l for l in open("routing.log") if re.search(r"\[routing\] degraded ", l)]
for l in flags:
    print("  " + l.strip())
win = num(r"degraded ([0-9.]+) s before", flags)
lane = num(r"\(detection window\)  flag to routing ([0-9.]+) ms", flags)
late = sum(num(r"plays sent after the flag ([0-9]+)$", flags))
healthy = sum(1 for l in open("routing.log") if re.search(r"\[routing\] healthy ", l))
pl = [l for l in open("routing.log") if re.search(r"\[routing\] plays [1-9]", l)]
print("incidents at routing: %d flagged, %d healthy again; detection window median %.2f s; flag to routing median %.1f ms; "
      "plays %d, steered to a healthy CDN %d, sent after the flag %d" % (len(flags), healthy, st.median(win) if win else 0,
      st.median(lane) if lane else 0, sum(num(r"plays ([0-9,]+)/s", pl)), sum(num(r"healthy CDN ([0-9,]+)", pl)), late))
f = [l for l in open("full.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if f:
    print("full-heartbeat neighbour: %d s, median %.1f MB/s on its own topic" % (len(f), st.median(num(r"~([0-9.]+) MB/s", f))))
if len(flags) < 2 or healthy < 1:
    raise SystemExit("incidents did not reach routing and clear")
if late > 2 * len(flags):
    raise SystemExit("routing sent plays to flagged CDNs too often: the signal arrived late")
if "death overs" not in open("heartbeats.log").read() or not surge:
    raise SystemExit("the death-overs surge did not reach the lane")
PY
grep -q "\[full\] done" full.log || { echo "the full-heartbeat neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
