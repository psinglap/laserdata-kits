#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the 5G core probe record lane at small scale for about a
# minute, with three injected NF incidents, a signalling storm and RAN counters next to it. Pass only if both
# hops report PASS, the incidents reach the workflow engine and clear, and it rarely gives a procedure to a flagged NF.
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
python -m py_compile common.py vprobes.py anomaly.py workflow.py ran_flood.py kafka_bridge.py
# The anomaly check and the workflow engine start first and create the topics; vProbes and the neighbour follow.
timeout 78 python -u anomaly.py > anomaly.log 2>&1 &
timeout 78 python -u workflow.py --procedures-per-s 500 > workflow.log 2>&1 &
sleep 3
# 800 SDRs a second of ~600 B, 2.5x for 15 s from second 30 (the signalling storm); incidents at 12, 30
# and 48 s, 9 s each; 2 MB/s of ~8 KB RAN counters and logs from second 20.
timeout 72 python -u vprobes.py --records-per-s 800 --storm 2.5 --storm-at 30 --storm-for 15 \
  --incident-at 12 --incident-every 18 --incident-for 9 > vprobes.log 2>&1 &
timeout 70 python -u ran_flood.py --mbps 2 --at 20 --duration 35 > ran.log 2>&1 &
wait || true
echo "--- vprobes.log"; head -n 2 vprobes.log; grep "incident" vprobes.log || true; tail -n 3 vprobes.log
echo "--- anomaly.log"; grep -E "\] (degraded|healthy) " anomaly.log || true; tail -n 4 anomaly.log
echo "--- workflow.log"; grep -E "\] (degraded|healthy) " workflow.log || true; tail -n 4 workflow.log
echo "--- ran.log"; head -n 2 ran.log; tail -n 2 ran.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" anomaly.log workflow.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
check() {  # $1 log, $2 pattern of a live second
  local steady live fail
  steady=$(grep -E "$2" "$1" | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$1 steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed: $1"; exit 1; fi
}
check anomaly.log '\[anomaly\] +[1-9][0-9,]* SDRs/s'
check workflow.log '\[workflow\] +[1-9][0-9,]* alerts/s'
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
q = [l for l in open("anomaly.log") if re.search(r"\[anomaly\] +[1-9][0-9,]* SDRs/s", l)][10:]
r = [l for l in open("workflow.log") if re.search(r"\[workflow\] +[1-9][0-9,]* alerts/s", l)][10:]
for name, ls, what in (("SDR-to-check", q, r"\] +([0-9,]+) SDRs/s"), ("check-to-workflow", r, r"\] +([0-9,]+) alerts/s")):
    print("%s steady-state medians: %.0f msgs/s  p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  (worst p99 %.1f ms, %d s)" % (
        (name, st.median(num(what, ls))) + tuple(st.median(num(p, ls)) for p in (r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms"))
        + (max(num(r"p99 +([0-9.]+)ms", ls)), len(ls))))
surge = [l for l in q if float(re.search(r"\] +([0-9,]+) SDRs", l).group(1).replace(",", "")) > 1500]
if surge:
    print("signalling storm: %d s above 1,500 SDRs/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(surge), st.median(num(r"p99 +([0-9.]+)ms", surge)), max(num(r"p99 +([0-9.]+)ms", surge))))
flags = [l for l in open("workflow.log") if re.search(r"\[workflow\] degraded ", l)]
for l in flags:
    print("  " + l.strip())
win = [float(m.group(1)) for m in (re.search(r"degraded ([0-9.]+) s before", l) for l in flags) if m]  # "nan" when the onset was not seen
lane = num(r"\(detection window\)  flag to workflow ([0-9.]+) ms", flags)
late = sum(num(r"procedures given after the flag ([0-9]+)$", flags))
healthy = sum(1 for l in open("workflow.log") if re.search(r"\[workflow\] healthy ", l))
pl = [l for l in open("workflow.log") if re.search(r"\[workflow\] procedures [1-9]", l)]
print("incidents at the workflow engine: %d flagged, %d healthy again; detection window median %.2f s; flag to workflow median %.1f ms; "
      "procedures %d, steered to a healthy peer %d, given after the flag %d" % (len(flags), healthy, st.median(win) if win else 0,
      st.median(lane) if lane else 0, sum(num(r"procedures ([0-9,]+)/s", pl)), sum(num(r"healthy peer ([0-9,]+)", pl)), late))
f = [l for l in open("ran.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if f:
    print("RAN counters neighbour: %d s, median %.1f MB/s on its own topic" % (len(f), st.median(num(r"~([0-9.]+) MB/s", f))))
if len(flags) < 2 or healthy < 1:
    raise SystemExit("incidents did not reach the workflow engine and clear")
if late > 2 * len(flags):
    raise SystemExit("the workflow engine gave procedures to flagged NFs too often: the alert arrived late")
if "signalling storm" not in open("vprobes.log").read() or not surge:
    raise SystemExit("the signalling storm did not reach the lane")
PY
grep -q "\[ran\] done" ran.log || { echo "the RAN counters neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
