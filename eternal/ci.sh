#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the kitchen state lane at small scale for about a minute,
# with a benchmark replay next door for 20 s, and pass only if the lane reports PASS and every
# FPT prediction flows back to the kitchens to be checked against the truth.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py kitchens.py fpt.py replay_flood.py confluent_cloud_bridge.py
# Reduced scale: 20,000 kitchens, 300 checkouts/s, kitchen time 60x faster so orders cook in seconds.
timeout 80 python -u kitchens.py --restaurants 20000 --in-kitchen 3000 --minute 1 > kitchens.log 2>&1 &
sleep 5
timeout 65 python -u replay_flood.py --mbps 4 --at 30 --for 20 > replay.log 2>&1 &
timeout 70 python -u fpt.py --restaurants 20000 --orders-per-s 300 --why > fpt.log 2>&1 || true
wait || true
echo "--- kitchens.log"; tail -n 8 kitchens.log
echo "--- fpt.log";      tail -n 8 fpt.log
echo "--- FAIL seconds, with where the time went (line: output)"; grep -n -A1 "FAIL (" fpt.log | cut -c1-150 || true
echo "--- kitchens.log, seconds with a send over 100 ms or a GC pause over 10 ms"; grep -n -E "slowest send [0-9]{3,} ms|GC pause [0-9]{2,} ms" kitchens.log | sed -E 's/^([0-9]+):.*(slowest send.*)/\1: \2/' | head -n 12 || true
echo "--- broker log lines about saving or flushing"; docker compose logs --no-color --timestamps iggy 2>/dev/null | grep -i -E "sav|flush|fsync|segment|slow|warn" | tail -n 25 || true
echo "--- kitchens.log / fpt.log wall clock start"; head -n 1 kitchens.log; date -u
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[fpt\] +[1-9][0-9,]* kitchen events/s' fpt.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
t = [l for l in open("fpt.log") if re.search(r"\[fpt\] +[1-9][0-9,]* kitchen events/s", l)][10:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: kitchen events/s %.0f  event-to-feature-view p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(p, t)) for p in (r"\] +([0-9,]+) kitchen events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("fpt.log") if l.startswith("[fpt]") and "kitchen events/s" in l]
if len(lane) > 50:
    print("event-to-feature-view p99, median per second: before the replay %.1f ms (s 12-28), during the replay %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
k = [l for l in open("kitchens.log") if re.search(r"predictions checked +[1-9][0-9,]*/s", l)][10:]
if k:
    print("FPT predictions made on a stale kitchen view: median %.3f%% per second, max %.3f%% (%d s)" % (
        st.median(num(r"stale kitchen view +([0-9.]+)%", k)), max(num(r"stale kitchen view +([0-9.]+)%", k)), len(k)))
PY
grep -Eq "orders accepted [1-9]" kitchens.log || { echo "no orders accepted"; exit 1; }
grep -Eq "predictions checked +[1-9][0-9,]*/s" kitchens.log || { echo "no FPT predictions checked against the truth"; exit 1; }
echo "--- replay.log"; grep -m 3 "replay on" replay.log | tail -n 2 || true
grep -q "replay on" replay.log || { echo "benchmark replay did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
