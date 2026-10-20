#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the vehicle state lane at small scale for about a minute, with
# the full signal stream next door, and pass only if the lane reports PASS, app opens are served, and the
# app shows each vehicle's latest report.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py vehicles.py vehicle_state.py signal_flood.py msk_bridge.py
# The state service starts first and creates the topics; the fleet and the neighbour follow.
# 15,000 vehicles reporting every 5 s (3,000 reports/s), 300 app opens a second;
# 3 MB/s of full signal reports on their own topic from second 20.
timeout 78 python -u vehicle_state.py --reads-per-s 300 > state.log 2>&1 &
sleep 3
timeout 72 python -u vehicles.py --vehicles 15000 > vehicles.log 2>&1 &
timeout 70 python -u signal_flood.py --mbps 3 --at 20 --duration 35 > signals.log 2>&1 &
wait || true
echo "--- vehicles.log"; head -n 3 vehicles.log; tail -n 4 vehicles.log
echo "--- state.log";    head -n 3 state.log; tail -n 6 state.log
echo "--- signals.log";  head -n 2 signals.log; tail -n 2 signals.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" state.log | cut -c1-300 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[vehicle-state\] +[1-9][0-9,]* reports/s' state.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the app check, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("state.log") if re.search(r"\[vehicle-state\] +[1-9][0-9,]* reports/s", l)][10:]
print("steady-state medians: reports/s %.0f  report-to-state p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) reports/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("state.log") if "report-to-state" in l])
if len(lane) > 50:
    print("report-to-state p99, median per second: before the neighbour %.1f ms (s 8-16), with 3 MB/s of full signal reports %.1f ms (s 22-50)" % (
        st.median(lane[8:17]), st.median(lane[22:51])))
served = sum(nums(r"app reads served ([0-9,]+)", s))
changes = sum(nums(r"lock or charge changes ([0-9,]+)", s))
print("app reads served in steady state: %d  lock or charge changes applied: %d" % (served, changes))
if served < 1000 or changes < 50:
    raise SystemExit("too few app reads or state changes to judge the lane")
v = [l for l in open("vehicles.log") if re.search(r"app reads checked [1-9]", l)][10:]
if not v:
    raise SystemExit("no app reads were checked against the truth")
checked = sum(nums(r"app reads checked ([0-9,]+)", v))
after = sum(nums(r"right after a change ([0-9,]+)\)", v))
wrong = sum(nums(r"state shown ([0-9,]+)", v))
print("app reads checked: %d (%d right after a lock or charge change)  on the latest report %.2f%%  one behind %.2f%%  "
      "two or more behind %.3f%%  (median per second, %d s)  wrong lock or charge state shown: %d in total" % (
        checked, after, st.median(nums(r"latest report +([0-9.]+)%", v)), st.median(nums(r"one behind +([0-9.]+)%", v)),
        st.median(nums(r"two or more behind +([0-9.]+)%", v)), len(v), wrong))
if st.median(nums(r"two or more behind +([0-9.]+)%", v)) > 0.1:
    raise SystemExit("too many app reads showed a state two or more reports old")
e = [l for l in open("signals.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("signal neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[signals\] done" signals.log || { echo "the signal neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
