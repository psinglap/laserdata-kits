#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the payment downtime lane at small scale for about a minute,
# with three injected downtimes, a match-day surge and a workflow-event stream next to it. Pass only if both
# hops report PASS, the downtimes reach Checkout and resolve, and Checkout rarely offers a flagged method.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py payments.py downtime.py checkout.py workflow_flood.py msk_bridge.py
# The detector and Checkout start first and create the topics; payments and the neighbour follow.
timeout 78 python -u downtime.py > downtime.log 2>&1 &
timeout 78 python -u checkout.py --checkouts-per-s 500 > checkout.log 2>&1 &
sleep 3
# 500 payments a second, 2.5x for 15 s from second 30 (the surge); downtimes at 12, 30 and 48 s, 9 s each;
# 1 MB/s of workflow events from second 20.
timeout 72 python -u payments.py --payments-per-s 500 --surge 2.5 --surge-at 30 --surge-for 15 \
  --incident-at 12 --incident-every 18 --incident-for 9 > payments.log 2>&1 &
timeout 70 python -u workflow_flood.py --mbps 1 --at 20 --duration 35 > workflow.log 2>&1 &
wait || true
echo "--- payments.log"; head -n 2 payments.log; grep "incident" payments.log || true; tail -n 3 payments.log
echo "--- downtime.log"; grep -E "\] (started|resolved) " downtime.log || true; tail -n 4 downtime.log
echo "--- checkout.log"; grep -E "downtime (started|resolved)" checkout.log || true; tail -n 4 checkout.log
echo "--- workflow.log"; head -n 2 workflow.log; tail -n 2 workflow.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" downtime.log checkout.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
check() {  # $1 log, $2 pattern of a live second
  local steady live fail
  steady=$(grep -E "$2" "$1" | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$1 steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed: $1"; exit 1; fi
}
check downtime.log '\[downtime\] +[1-9][0-9,]* payment events/s'
check checkout.log '\[checkout\] +[1-9][0-9,]* signals/s'
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
d = [l for l in open("downtime.log") if re.search(r"\[downtime\] +[1-9][0-9,]* payment events/s", l)][10:]
c = [l for l in open("checkout.log") if re.search(r"\[checkout\] +[1-9][0-9,]* signals/s", l)][10:]
for name, ls, what in (("status-to-detector", d, r"\] +([0-9,]+) payment events/s"), ("detector-to-checkout", c, r"\] +([0-9,]+) signals/s")):
    print("%s steady-state medians: %.0f msgs/s  p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  (worst p99 %.1f ms, %d s)" % (
        (name, st.median(num(what, ls))) + tuple(st.median(num(p, ls)) for p in (r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms"))
        + (max(num(r"p99 +([0-9.]+)ms", ls)), len(ls))))
surge = [l for l in d if float(re.search(r"\] +([0-9,]+) payment", l).group(1).replace(",", "")) > 900]
if surge:
    print("surge: %d s above 900 payments/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(surge), st.median(num(r"p99 +([0-9.]+)ms", surge)), max(num(r"p99 +([0-9.]+)ms", surge))))
starts = [l for l in open("checkout.log") if "downtime started" in l]
for l in starts:
    print("  " + l.strip())
win = [float(m.group(1)) for m in (re.search(r"degraded ([0-9.]+) s", l) for l in starts) if m]  # "nan" when the start of the degradation was not seen
lane = num(r"\(detection window\)  flag to checkout ([0-9.]+) ms", starts)
late = sum(num(r"offered after the flag ([0-9]+)$", starts))
resolved = sum(1 for l in open("checkout.log") if "downtime resolved" in l)
ck = [l for l in open("checkout.log") if re.search(r"\[checkout\] checkouts [1-9]", l)]
print("downtimes at Checkout: %d started, %d resolved; detection window median %.2f s; flag to Checkout median %.1f ms; "
      "checkouts %d, method hidden %d, offered after the flag %d" % (len(starts), resolved, st.median(win) if win else 0,
      st.median(lane) if lane else 0, sum(num(r"checkouts ([0-9,]+)/s", ck)), sum(num(r"method hidden ([0-9,]+)", ck)), late))
w = [l for l in open("workflow.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if w:
    print("workflow neighbour: %d s, median %.1f MB/s on its own topic" % (len(w), st.median(num(r"~([0-9.]+) MB/s", w))))
if len(starts) < 2 or resolved < 1:
    raise SystemExit("downtimes did not reach Checkout and resolve")
if late > 2 * len(starts):
    raise SystemExit("Checkout offered flagged methods too often: the signal arrived late")
if "surge" not in open("payments.log").read() or not surge:
    raise SystemExit("the surge did not reach the lane")
PY
grep -q "\[workflow\] done" workflow.log || { echo "the workflow neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
