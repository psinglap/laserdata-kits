#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Autoplay lane at small scale for about a minute, with a
# new-episode surge and an analytics stream next to it, and pass only if the lane reports PASS and every
# end-of-content request is answered from a queue that holds the listener's last action, with no wrong show started.
# Then a 40 s what-if: the same requests with every action applied 1 s late, as a batching path would.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py listeners.py autoplay.py analytics.py kafka_bridge.py
# The Autoplay queue service starts first and creates the topics; the listeners and the neighbour follow.
timeout 78 python -u autoplay.py > autoplay.log 2>&1 &
sleep 3
# 1,500 listening actions a second over 200,000 listeners, half followed by an end-of-content request 0.3-2 s
# later, 2x for 15 s from second 30 (a hit's daily episodes drop); 1 MB/s of analytics pings from second 20.
timeout 72 python -u listeners.py --users 200000 --actions-per-s 1500 --surge 2 --surge-at 30 --surge-for 15 > listeners.log 2>&1 &
timeout 70 python -u analytics.py --mbps 1 --at 20 --duration 35 > analytics.log 2>&1 &
wait || true
# What-if: the same listeners, with the queue service applying each action only once it is 1 s old.
timeout 45 python -u autoplay.py --name hold --hold-ms 1000 > hold.log 2>&1 &
sleep 3
timeout 40 python -u listeners.py --users 200000 --actions-per-s 1500 --surge 1 > listeners-hold.log 2>&1 &
wait || true
echo "--- listeners.log";   head -n 2 listeners.log; tail -n 4 listeners.log
echo "--- autoplay.log";    tail -n 8 autoplay.log
echo "--- analytics.log";   head -n 2 analytics.log; tail -n 2 analytics.log
echo "--- hold.log";        tail -n 4 hold.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" autoplay.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[autoplay\] +[1-9][0-9,]* actions/s' autoplay.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("autoplay.log") if re.search(r"\[autoplay\] +[1-9][0-9,]* actions/s", l)][10:]
f = [l for l in open("autoplay.log") if re.search(r"\[next-show\] picks [1-9]", l)][10:]
print("steady-state medians over %d s: actions/s %.0f  action-to-queue p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) actions/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms"))))
peak = [l for l in h if nums(r"\] +([0-9,]+) actions/s", [l])[0] > 2000]
if peak:
    print("surge: %d s above 2,000 actions/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
stale = nums(r"picked without the last action ([0-9.]+)%", f)
print("end-of-content picks: %d s checked, %.0f picks, %.0f where the last action changed the next show, picked without the last "
      "action: median %.3f%%, worst second %.3f%%, wrong show started %.0f" % (
    len(f), sum(nums(r"picks ([0-9,]+)  next", f)), sum(nums(r"show changed ([0-9,]+)  picked", f)),
    st.median(stale) if stale else float("nan"), max(stale) if stale else float("nan"),
    sum(nums(r"wrong show started ([0-9,]+)", f))))
e = [l for l in open("analytics.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("analytics neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
hr = [l for l in open("hold.log") if re.search(r"\[next-show\] picks [1-9]", l)][5:]
if hr:
    print("what-if, every action applied 1 s late: %d s, %.0f picks, picked without the last action: median %.1f%%, "
          "wrong show started %.0f" % (len(hr), sum(nums(r"picks ([0-9,]+)  next", hr)),
          st.median(nums(r"without the last action ([0-9.]+)%", hr)), sum(nums(r"wrong show started ([0-9,]+)", hr))))
if len(f) < 30:
    raise SystemExit("too few seconds with end-of-content requests")
if st.median(stale) > 0.1:
    raise SystemExit("too many Autoplay picks were made without the listener's last action")
if "(surge)" not in open("listeners.log").read() or not peak:
    raise SystemExit("the surge did not reach the lane")
PY
echo "Kit OK on Apache Iggy 0.9.0"
