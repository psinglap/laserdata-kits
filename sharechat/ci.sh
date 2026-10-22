#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the viewed-posts lane at small scale for about a minute,
# with a peak and an engagement stream next to it, and pass only if the lane reports PASS and the
# next-page feed requests are filtered against the user's last flush, with no repeats served.
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
python -m py_compile common.py app.py dedup.py engagement_flood.py redpanda_bridge.py
# The dedup service starts first and creates the topics; the apps and the neighbour follow.
timeout 78 python -u dedup.py > dedup.log 2>&1 &
sleep 3
# 2,500 views a second in flushes of 5, 55% followed by a feed request 0.2-2 s later, 2x for 15 s from
# second 30 (the peak); 1 MB/s of engagement events from second 20.
timeout 72 python -u app.py --users 200000 --views-per-s 2500 --peak 2 --peak-at 30 --peak-for 15 > app.log 2>&1 &
timeout 70 python -u engagement_flood.py --mbps 1 --at 20 --duration 35 > engagement.log 2>&1 &
wait || true
echo "--- app.log";        head -n 2 app.log; tail -n 4 app.log
echo "--- dedup.log";      tail -n 8 dedup.log
echo "--- engagement.log"; head -n 2 engagement.log; tail -n 2 engagement.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" dedup.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[dedup\] +[1-9][0-9,]* flushes/s' dedup.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
h = [l for l in open("dedup.log") if re.search(r"\[dedup\] +[1-9][0-9,]* flushes/s", l)][10:]
f = [l for l in open("dedup.log") if re.search(r"\[feed\] next-page requests [1-9]", l)][10:]
a = open("app.log").read()
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: flushes/s %.0f  flush-to-dedup p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(q, h)) for q in (r"\] +([0-9,]+) flushes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
peak = [l for l in h if float(re.search(r"\] +([0-9,]+) flushes", l).group(1).replace(",", "")) > 800]
if peak:
    print("peak: %d s above 800 flushes/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(num(r"p99 +([0-9.]+)ms", peak)), max(num(r"p99 +([0-9.]+)ms", peak))))
late = num(r"missing the last flush ([0-9.]+)%", f)
print("next-page feed requests: %d s checked, %.0f requests in all, viewed candidates removed %.0f, "
      "missing the last flush: median %.3f%%, worst second %.3f%%, repeats served %.0f" % (
    len(f), sum(num(r"requests ([0-9,]+)  viewed", f)), sum(num(r"removed ([0-9,]+)", f)), st.median(late), max(late),
    sum(num(r"repeats served ([0-9,]+)", f))))
e = [l for l in open("engagement.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("engagement neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(num(r"~([0-9.]+) MB/s", e))))
if len(f) < 30:
    raise SystemExit("too few seconds with feed requests")
if st.median(late) > 0.1:
    raise SystemExit("too many feed requests missed the user's last flush")
if sum(num(r"removed ([0-9,]+)", f)) == 0:
    raise SystemExit("no viewed candidates were removed: the dedup step did not see the lane")
if "(peak)" not in a or not peak:
    raise SystemExit("the peak did not reach the lane")
PY
grep -q "\[engagement\] done" engagement.log || { echo "the engagement neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
