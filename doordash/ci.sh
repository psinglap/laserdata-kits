#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the store feedback lane at small scale for about a minute,
# with client analytics flooding the same broker for 20 s, and pass only if the lane reports PASS and
# every dispatch decision flows back to the Dashers to be checked against the truth.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py dashers.py dispatch.py analytics_flood.py kafka_bridge.py
# Reduced scale: 20,000 stores in 200 markets, 300 orders/s, a dispatch run every 500 ms,
# marketplace time 60x faster so a delivery takes seconds.
timeout 80 python -u dashers.py --stores 20000 --markets 200 --dashers 8000 --in-flight 3000 --minute 1 > dashers.log 2>&1 &
sleep 5
timeout 65 python -u analytics_flood.py --mbps 4 --at 30 --for 20 > analytics.log 2>&1 &
timeout 70 python -u dispatch.py --stores 20000 --orders-per-s 300 --run-ms 500 --why > dispatch.log 2>&1 || true
wait || true
echo "--- dashers.log";  tail -n 8 dashers.log
echo "--- dispatch.log"; tail -n 8 dispatch.log
echo "--- FAIL seconds, with where the time went (line: output)"; grep -n -A1 "FAIL (" dispatch.log | cut -c1-150 || true
echo "--- dashers.log, seconds with a send over 100 ms or a GC pause over 10 ms"; grep -n -E "slowest send [0-9]{3,} ms|GC pause [0-9]{2,} ms" dashers.log | sed -E 's/^([0-9]+):.*(slowest send.*)/\1: \2/' | head -n 12 || true
echo "--- broker log lines about saving or flushing"; docker compose logs --no-color --timestamps iggy 2>/dev/null | grep -i -E "sav|flush|fsync|segment|slow|warn" | tail -n 25 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[dispatch\] +[1-9][0-9,]* feedback events/s' dispatch.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README. Lines that don't parse are skipped.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
t = [l for l in open("dispatch.log") if re.search(r"\[dispatch\] +[1-9][0-9,]* feedback events/s", l)][10:]
print("steady-state medians: feedback events/s %.0f  event-to-coordinator p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(p, t)) for p in (r"\] +([0-9,]+) feedback events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("dispatch.log") if re.search(r"\[dispatch\] +[0-9,]+ feedback events/s", l)])
if len(lane) > 50:
    print("event-to-coordinator p99, median per second: before the flood %.1f ms (s 12-28), during the flood %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
k = [l for l in open("dashers.log") if re.search(r"decisions checked +[1-9][0-9,]*/s", l)][10:]
if k:
    s, f = nums(r"stale store feedback +([0-9.]+)%", k), nums(r"flipped speed/batch +([0-9.]+)%", k)
    print("decisions made on stale store feedback: median %.3f%% per second, max %.3f%%; flipped speed/batch: median %.3f%%, max %.3f%% (%d s)" % (
        st.median(s), max(s), st.median(f), max(f), len(k)))
PY
grep -Eq "delivered [1-9]" dashers.log || { echo "no deliveries completed"; exit 1; }
grep -Eq "decisions checked +[1-9][0-9,]*/s" dashers.log || { echo "no dispatch decisions checked against the truth"; exit 1; }
echo "--- analytics.log"; grep -m 3 "analytics on" analytics.log | tail -n 2 || true
grep -q "analytics on" analytics.log || { echo "analytics flood did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
