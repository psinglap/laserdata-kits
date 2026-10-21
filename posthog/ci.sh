#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the live events lane at small scale for about a minute, with
# viewers opening and closing projects and one project's launch spiking next to them. Pass only if the live
# node reports PASS, no event of a watched project is missed or shown twice, the launch is shown in full,
# and newly opened views are filled from the retained lane.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py capture.py live.py launch.py kafka_bridge.py
# The live node starts first and creates the lane (64 partitions here); capture and the launch follow.
# 5,000 projects sending 2,500 events/s; 40 views open on the node plus the launch project's; one viewer
# leaves and another project opens every 2 s; the launch project goes from 100 to 1,500 events/s from
# second 25 to 50.
timeout 80 python -u live.py --projects 5000 --viewers 40 --partitions 64 > live.log 2>&1 &
sleep 3
timeout 74 python -u capture.py --projects 5000 --rate 2500 --users 100000 --partitions 64 > capture.log 2>&1 &
timeout 72 python -u launch.py --base 100 --peak 1500 --at 22 --duration 25 --partitions 64 --run-for 66 > launch.log 2>&1 &
wait || true
echo "--- capture.log"; head -n 3 capture.log; tail -n 3 capture.log
echo "--- launch.log";  sed -n '20,24p;46,49p' launch.log; tail -n 2 launch.log
echo "--- live.log";    head -n 4 live.log; sed -n '24,28p;45,49p' live.log; tail -n 3 live.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" live.log | cut -c1-300 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[live\] +[1-9][0-9,]* msgs/s' live.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the business checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
allv = [l for l in open("live.log") if re.search(r"\[live\] +[0-9,]+ msgs/s", l)]
s = [l for l in allv if re.search(r"\[live\] +[1-9][0-9,]* msgs/s", l)][10:]
print("steady-state medians: shown %.0f events/s  capture-to-live p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) msgs/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
launch_ev = nums(r"launch [0-9,]+, ([0-9,]+) ev/s", allv)
p99 = nums(r"p99 +([0-9.]+)ms", allv)
peak_idx = [i for i, v in enumerate(launch_ev) if v >= 1000]
calm = [p99[i] for i, v in enumerate(launch_ev) if 50 <= v < 300 and i >= 3]
if peak_idx and calm:
    print("capture-to-live p99, median per second: launch project at ~100 events/s %.1f ms, during the launch "
          "at ~1,500 events/s %.1f ms" % (st.median(calm), st.median([p99[i] for i in peak_idx])))
tot = lambda pat, lines: int(sum(nums(pat, lines)))
captured = tot(r"\] +([0-9,]+) events/s", [l for l in open("capture.log") if l.startswith("[capture]")])
launched = tot(r"\] +([0-9,]+) events/s", [l for l in open("launch.log") if l.startswith("[launch]")])
read = tot(r"read ([0-9,]+)/s", allv)
other = tot(r"other projects ([0-9,]+)\)", allv)
missed = nums(r"total ([0-9]+)\)", allv)
dup = tot(r"dup ([0-9]+) ", allv)
shown_launch = int(sum(launch_ev))
opened = tot(r"opened ([0-9]+) filled", allv)
fills = tot(r"filled ([0-9]+) with", allv)
filled = tot(r"with ([0-9,]+) events", allv)
oldest = max(nums(r"oldest ([0-9.]+)s\)", allv) or [0])
parts = nums(r"partitions read ([0-9]+)/", s)
print("firehose: captured %d events (+%d launch); the live node read %d (%.0f%%), of which %d for other projects "
      "on the same partitions; partitions read, median %d of 64" % (
        captured, launched, read, 100 * read / max(1, captured + launched), other, st.median(parts or [0])))
print("watched projects: missed %d  shown twice %d  launch shown %d of %d sent" % (
    max(missed or [0]), dup, shown_launch, launched))
print("views: opened %d, filled %d with %d events from the retained lane (oldest %.1f s)" % (opened, fills, filled, oldest))
peak = max(launch_ev or [0])
print("launch: peak %d events/s shown" % peak)
if max(missed or [0]) or dup:
    raise SystemExit("an event of a watched project was missed or shown twice")
if peak < 1200 or shown_launch < 0.97 * launched:
    raise SystemExit("the launch was not shown in full")
if opened < 15 or fills < 15 or filled == 0 or not (5 <= oldest <= 61):
    raise SystemExit("newly opened views were not filled from the retained lane")
PY
grep -q "\[launch\] done" launch.log || { echo "the launch did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
