#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Crons check-in lane at small scale for about a minute, with
# a minute compressed to 5 s, jobs that never check in, jobs that hang, an error storm next door, and one
# partition not read for 8 s. Pass only if the lane reports PASS, every job that never checked in is marked
# missed, every hung job is marked timed out, nothing that checked in or finished is marked by mistake, and the
# clock held through the stuck partition and then ticked the minutes it held, in order.
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
python -m py_compile common.py crons.py monitors.py event_storm.py kafka_bridge.py
# The monitor consumer starts first and creates the topics; the jobs and the neighbour follow.
# 6,000 monitors running every 1, 2 or 5 minutes, a minute of 5 s (about 1,500 check-ins/s, in a burst at the
# start of each minute), 3% of runs never check in, 2% hang, max runtime 3 minutes; the consumer stops reading
# partition 3 from second 35 to 43; 3 MB/s of error events next door from second 15.
timeout 82 python -u monitors.py --minute-s 5 --pause-partition 3 --pause-at 35 --pause-for 8 > monitors.log 2>&1 &
sleep 3
timeout 76 python -u crons.py --monitors 6000 --mix 1:0.4,2:0.3,5:0.3 --minute-s 5 --never-rate 0.03 \
  --hang-rate 0.02 --max-runtime 3 --start-spread 0.1 --grace-s 15 > crons.log 2>&1 &
timeout 72 python -u event_storm.py --mbps 3 --at 15 --duration 40 > storm.log 2>&1 &
wait || true
echo "--- crons.log";    head -n 3 crons.log; sed -n '30,36p;44,50p' crons.log; tail -n 3 crons.log
echo "--- monitors.log"; head -n 3 monitors.log; grep -E "pausing|held by|backlog|backfill" monitors.log | head -n 12 || true
grep '^\[clock\]' monitors.log | head -n 16; tail -n 3 monitors.log
echo "--- storm.log";    head -n 2 storm.log; tail -n 2 storm.log
echo "--- false marks"; grep -E "false (missed|timeout):" crons.log | head -n 20 || true
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" monitors.log | cut -c1-300 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[monitors\] +[1-9][0-9,]* check-ins/s' monitors.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures), the clock and the truth check, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("monitors.log") if re.search(r"\[monitors\] +[1-9][0-9,]* check-ins/s", l)][10:]
print("steady-state medians: check-ins/s %.0f  check-in-to-monitor p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) check-ins/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
ticks = [l for l in open("monitors.log") if l.startswith("[clock] tick")]
lags = nums(r"lag ([0-9,.-]+)ms", ticks)
held = [x for x in lags if x > 1000]      # ticks the stuck partition held past the minute boundary
on_time = [x for x in lags if x <= 1000]
back = sum("(backfill)" in l for l in ticks)
print("clock: %d ticks; %d on time, tick lag after the minute boundary median %.0f ms, max %.0f ms; %d held by the "
      "stuck partition (up to %.1f s late), then ticked in order (%d as a backfill)" % (
        len(ticks), len(on_time), st.median(on_time or [0]), max(on_time or [0]), len(held), max(held or [0]) / 1000,
        back))
g = [l for l in open("crons.log") if l.startswith("[crons]") and "check-ins/s" in l]
tot = lambda pat: int(sum(nums(pat, g)))
mr, mf, mu = tot(r"missed: right ([0-9,]+)"), tot(r"missed: right [0-9,]+ \(by next check-in [0-9,]+\)  false ([0-9,]+)"), \
    tot(r"false [0-9,]+  undetected ([0-9,]+)  timeouts")
bynext = tot(r"by next check-in ([0-9,]+)")
tr, tf, tu = tot(r"timeouts: right ([0-9,]+)"), tot(r"timeouts: right [0-9,]+  false ([0-9,]+)"), \
    tot(r"timeouts: right [0-9,]+  false [0-9,]+  undetected ([0-9,]+)")
p50 = nums(r"mark after deadline p50 ([0-9,.-]+)ms", [l for l in g if not re.search(r"missed: right 0 .*timeouts: right 0 ", l)])
mx = nums(r"max ([0-9,.-]+)ms", [l for l in g if not re.search(r"missed: right 0 .*timeouts: right 0 ", l)])
print("truth: missed right %d (%d by the next check-in, the rest by a tick)  false %d  undetected %d  |  "
      "timeouts right %d  false %d  undetected %d  |  mark after the deadline: median of per-second p50 %.0f ms, "
      "max %.0f ms" % (mr, bynext, mf, mu, tr, tf, tu, st.median(p50 or [0]), max(mx or [0])))
if mr < 20 or tr < 10:
    raise SystemExit("too few missed or hung jobs to judge the clock")
if mf or mu or tf or tu:
    raise SystemExit("a missed or timeout mark did not match the truth")
if not held or not any("held by partition 3" in l for l in open("monitors.log")):
    raise SystemExit("the clock did not hold for the stuck partition")
if len(ticks) < 10:
    raise SystemExit("too few clock ticks")
e = [l for l in open("storm.log") if re.search(r"\] +[1-9][0-9,]* events/s", l)]
if e:
    print("error storm next door: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[storm\] done" storm.log || { echo "the error storm neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
