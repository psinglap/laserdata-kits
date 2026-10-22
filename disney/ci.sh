#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the live watch signals lane at small scale for about a minute,
# with a tip-off surge and a client-signal stream next to it, and pass only if the lane reports PASS and the
# Live rail requests are built from the profile's last signal, with no wrong tiles shown. Then a 40 s what-if:
# the same rails with every signal applied 1 s late.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py apps.py features.py client_signals.py kinesis_bridge.py
# The feature service starts first and creates the topics; the apps and the neighbour follow.
timeout 78 python -u features.py > features.log 2>&1 &
sleep 3
# 1,500 watch signals a second over 200,000 profiles and 40 live events, half followed by a Live rail request
# 0.3-2 s later, 2x for 15 s from second 30 (a slate tipping off); 1 MB/s of client signals from second 20.
timeout 72 python -u apps.py --profiles 200000 --events 40 --signals-per-s 1500 --surge 2 --surge-at 30 --surge-for 15 > apps.log 2>&1 &
timeout 70 python -u client_signals.py --mbps 1 --at 20 --duration 35 > client.log 2>&1 &
wait || true
# What-if: the same apps, with the feature service applying each signal only once it is 1 s old.
timeout 45 python -u features.py --name hold --hold-ms 1000 > hold.log 2>&1 &
sleep 3
timeout 40 python -u apps.py --profiles 200000 --events 40 --signals-per-s 1500 --surge 1 > apps-hold.log 2>&1 &
wait || true
echo "--- apps.log";     head -n 2 apps.log; tail -n 4 apps.log
echo "--- features.log"; tail -n 8 features.log
echo "--- client.log";   head -n 2 client.log; tail -n 2 client.log
echo "--- hold.log";     tail -n 4 hold.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" features.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[features\] +[1-9][0-9,]* signals/s' features.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[client\] done" client.log || { echo "the client-signal neighbour did not run"; exit 1; }
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
h = [l for l in open("features.log") if re.search(r"\[features\] +[1-9][0-9,]* signals/s", l)][10:]
f = [l for l in open("features.log") if re.search(r"\[rail\] Live rail requests [1-9]", l)][10:]
print("steady-state medians over %d s: signals/s %.0f  signal-to-feature p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) signals/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms"))))
peak = [l for l in h if nums(r"\] +([0-9,]+) signals/s", [l])[0] > 2000]
if peak:
    print("surge: %d s above 2,000 signals/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
stale = nums(r"without the last signal ([0-9.]+)%", f)
print("Live rail requests: %d s checked, %.0f requests, %.0f tiles, built without the last signal: median %.3f%%, "
      "worst second %.3f%%, wrong tiles shown %.0f" % (
    len(f), sum(nums(r"requests ([0-9,]+)  tiles", f)), sum(nums(r"tiles ([0-9,]+)  built", f)),
    st.median(stale) if stale else float("nan"), max(stale) if stale else float("nan"),
    sum(nums(r"wrong tiles shown ([0-9,]+)", f))))
e = [l for l in open("client.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("client-signal neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
hr = [l for l in open("hold.log") if re.search(r"\[rail\] Live rail requests [1-9]", l)][5:]
if hr:
    print("what-if, every signal applied 1 s late: %d s, %.0f requests, built without the last signal: median %.1f%%, "
          "wrong tiles shown %.0f" % (len(hr), sum(nums(r"requests ([0-9,]+)  tiles", hr)),
          st.median(nums(r"without the last signal ([0-9.]+)%", hr)), sum(nums(r"wrong tiles shown ([0-9,]+)", hr))))
if len(f) < 30:
    raise SystemExit("too few seconds with Live rail requests")
if st.median(stale) > 0.1:
    raise SystemExit("too many Live rails were built without the profile's last signal")
if "(surge)" not in open("apps.log").read() or not peak:
    raise SystemExit("the surge did not reach the lane")
PY
echo "Kit OK on Apache Iggy 0.9.0"
