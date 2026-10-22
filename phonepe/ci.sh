#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the payer velocity lane at small scale for about a minute,
# with a peak and an app-events stream next to it, and pass only if the lane reports PASS and the risk
# checks on repeat payers (bursts included) are checked and count the payer's previous payment.
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
python -m py_compile common.py payments.py risk.py events_flood.py kafka_bridge.py
# The risk store starts first and creates the topics; payments and the neighbour follow.
timeout 78 python -u risk.py > risk.log 2>&1 &
sleep 3
# 500 payments a second, 5% of them in bursts, 2x for 15 s from second 30 (the peak); 1 MB/s of app events from second 20.
timeout 72 python -u payments.py --payers 200000 --payments-per-s 500 --burst-share 0.05 --peak 2 --peak-at 30 --peak-for 15 > payments.log 2>&1 &
timeout 70 python -u events_flood.py --mbps 1 --at 20 --duration 35 > events.log 2>&1 &
wait || true
echo "--- payments.log"; head -n 2 payments.log; tail -n 4 payments.log
echo "--- risk.log";     tail -n 8 risk.log
echo "--- events.log";   head -n 2 events.log; tail -n 2 events.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" risk.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[risk\] +[1-9][0-9,]* velocity updates/s' risk.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
h = [l for l in open("risk.log") if re.search(r"\[risk\] +[1-9][0-9,]* velocity updates/s", l)][10:]
c = [l for l in open("risk.log") if re.search(r"\[check\] repeat-payer checks [1-9]", l)][10:]
p = open("payments.log").read()
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: velocity updates/s %.0f  payment-to-store p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(q, h)) for q in (r"\] +([0-9,]+) velocity updates/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
peak = [l for l in h if float(re.search(r"\] +([0-9,]+) velocity", l).group(1).replace(",", "")) > 800]
if peak:
    print("peak: %d s above 800 payments/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(num(r"p99 +([0-9.]+)ms", peak)), max(num(r"p99 +([0-9.]+)ms", peak))))
missed = num(r"missing the previous payment ([0-9.]+)%", c)
print("risk checks on repeat payers: %d s checked, %.0f checks in all (%.0f in bursts), missing the previous payment: "
      "median %.3f%%, worst second %.3f%%, velocity rule hits %.0f" % (
    len(c), sum(num(r"checks ([0-9,]+) \(", c)), sum(num(r"in bursts ([0-9,]+)\)  missing", c)), st.median(missed), max(missed),
    sum(num(r"rule hits ([0-9,]+)", c))))
e = [l for l in open("events.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("app-events neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(num(r"~([0-9.]+) MB/s", e))))
if len(c) < 30:
    raise SystemExit("too few seconds with risk checks")
if st.median(missed) > 0.1:
    raise SystemExit("too many risk checks missed the payer's previous payment")
if sum(num(r"in bursts ([0-9,]+)\)  missing", c)) == 0 or sum(num(r"rule hits ([0-9,]+)", c)) == 0:
    raise SystemExit("no burst checks or velocity rule hits reached the store")
if "(peak)" not in p or not peak:
    raise SystemExit("the peak did not reach the lane")
PY
grep -q "\[events\] done" events.log || { echo "the app-events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
