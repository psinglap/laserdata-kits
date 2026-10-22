#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the session context lane at small scale for about a minute, with a
# sale-opening surge and a clickstream next to it, and pass only if the lane reports PASS and the type-ahead
# requests are boosted from a context that holds the shopper's last action, with no wrong category boosted.
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
python -m py_compile common.py app.py context.py clickstream.py kafka_bridge.py
# The context service starts first and creates the topics; the app and the neighbour follow.
timeout 78 python -u context.py > context.log 2>&1 &
sleep 3
# 1,500 shopper actions a second over 200,000 shoppers, half followed by a type-ahead request 0.3-2 s later,
# 2x for 15 s from second 30 (a sale opening); 1 MB/s of clickstream from second 20.
timeout 72 python -u app.py --users 200000 --actions-per-s 1500 --surge 2 --surge-at 30 --surge-for 15 > app.log 2>&1 &
timeout 70 python -u clickstream.py --mbps 1 --at 20 --duration 35 > clickstream.log 2>&1 &
wait || true
# What-if: the same app, with the context service applying each action only once it is 1 s old.
timeout 45 python -u context.py --name hold --hold-ms 1000 > hold.log 2>&1 &
sleep 3
timeout 40 python -u app.py --users 200000 --actions-per-s 1500 --surge 1 > app-hold.log 2>&1 &
wait || true
echo "--- app.log";         head -n 2 app.log; tail -n 4 app.log
echo "--- context.log";     tail -n 8 context.log
echo "--- clickstream.log"; head -n 2 clickstream.log; tail -n 2 clickstream.log
echo "--- hold.log";        tail -n 4 hold.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" context.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[context\] +[1-9][0-9,]* actions/s' context.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[clickstream\] done" clickstream.log || { echo "the clickstream neighbour did not run"; exit 1; }
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
h = [l for l in open("context.log") if re.search(r"\[context\] +[1-9][0-9,]* actions/s", l)][10:]
f = [l for l in open("context.log") if re.search(r"\[type-ahead\] requests [1-9]", l)][10:]
print("steady-state medians over %d s: actions/s %.0f  action-to-context p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) actions/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms"))))
peak = [l for l in h if nums(r"\] +([0-9,]+) actions/s", [l])[0] > 2000]
if peak:
    print("surge: %d s above 2,000 actions/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
stale = nums(r"without the last action ([0-9.]+)%", f)
print("type-ahead requests: %d s checked, %.0f requests, %.0f where the last action changed the lead category, boosted without the last "
      "action: median %.3f%%, worst second %.3f%%, wrong category boosted %.0f" % (
    len(f), sum(nums(r"requests ([0-9,]+)  lead", f)), sum(nums(r"category changed ([0-9,]+)  boosted", f)),
    st.median(stale) if stale else float("nan"), max(stale) if stale else float("nan"),
    sum(nums(r"wrong category boosted ([0-9,]+)", f))))
e = [l for l in open("clickstream.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("clickstream neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
hr = [l for l in open("hold.log") if re.search(r"\[type-ahead\] requests [1-9]", l)][5:]
if hr:
    print("what-if, every action applied 1 s late: %d s, %.0f requests, boosted without the last action: median %.1f%%, "
          "wrong category boosted %.0f" % (len(hr), sum(nums(r"requests ([0-9,]+)  lead", hr)),
          st.median(nums(r"without the last action ([0-9.]+)%", hr)), sum(nums(r"wrong category boosted ([0-9,]+)", hr))))
if len(f) < 30:
    raise SystemExit("too few seconds with type-ahead requests")
if st.median(stale) > 0.1:
    raise SystemExit("too many type-ahead requests were boosted without the shopper's last action")
if "(surge)" not in open("app.log").read() or not peak:
    raise SystemExit("the surge did not reach the lane")
PY
echo "Kit OK on Apache Iggy 0.9.0"
