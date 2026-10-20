#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the ping lane at small scale for about a minute,
# and pass only if the lane reports PASS and ETAs flow end to end and get checked against the truth.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py fleet.py track.py retry_storm.py confluent_bridge.py
timeout 80 python -u fleet.py --partners 10000 --every 5 --assign-p 0.3 --wait-s 10 40 > fleet.log 2>&1 &
sleep 5
# A 20x retry storm next door from second 30 to 50 of the tracker's run, on the same broker.
timeout 65 python -u retry_storm.py --mbps 0.05 --x 20 --at 30 --for 20 > storm.log 2>&1 &
timeout 70 python -u track.py --why > track.log 2>&1 || true
wait || true
echo "--- fleet.log"; grep -v "ETAs checked 0 " fleet.log | tail -n 8
echo "--- track.log"; tail -n 12 track.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" track.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[track\] +[1-9][0-9,]* pings/s' track.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
t = [l for l in open("track.log") if re.search(r"\[track\] +[1-9][0-9,]* pings/s", l)][10:]
f = [l for l in open("fleet.log") if re.search(r"ETAs checked [1-9]", l)][10:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
if t:
    print("steady-state medians: pings/s %.0f  ping-to-tracker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
        st.median(num(p, t)) for p in (r"\] +([0-9,]+) pings/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("track.log") if l.startswith("[track]") and "pings/s" in l]
if len(lane) > 50:
    print("ping-to-tracker p99, median per second: before the storm %.1f ms (s 12-28), during the 20x storm %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
if f:
    print("ETAs on the latest ping %.2f%%  one behind %.2f%%  two or more behind %.3f%%  (median per second, %d s)" % (
        st.median(num(r"latest ping +([0-9.]+)%", f)), st.median(num(r"one ping behind +([0-9.]+)%", f)),
        st.median(num(r"two or more behind +([0-9.]+)%", f)), len(f)))
    if st.median(num(r"two or more behind +([0-9.]+)%", f)) > 1.0:
        raise SystemExit("too many ETAs computed on a position two or more pings old")
PY
grep -Eq "ETAs published [1-9]" track.log || { echo "no ETAs published"; exit 1; }
grep -Eq "ETAs checked [1-9]" fleet.log || { echo "no ETAs checked against the truth"; exit 1; }
echo "--- storm.log"; grep -E "storm 20x" storm.log | head -n 3; tail -n 2 storm.log
grep -q "storm 20x" storm.log || { echo "retry storm did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
