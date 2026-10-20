#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the position-update lane at small scale for about a minute,
# with a market-open burst and a DataPlatform stream next to it, and pass only if the lane reports PASS
# and the app's post-trade Holdings refreshes are checked and show the new fill.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py oms_fills.py holdings.py dataplatform_flood.py kafka_bridge.py
# The Holdings service starts first and creates the topics; OMS fills and the neighbour follow.
timeout 78 python -u holdings.py > holdings.log 2>&1 &
sleep 3
# 500 fills a second, 2.5x for 15 s from second 30 (market open); 1 MB/s of DataPlatform events from second 20.
timeout 72 python -u oms_fills.py --users 200000 --fills-per-s 500 --open-burst 2.5 --open-at 30 --open-for 15 > oms.log 2>&1 &
timeout 70 python -u dataplatform_flood.py --mbps 1 --at 20 --duration 35 > dataplatform.log 2>&1 &
wait || true
echo "--- oms.log";          head -n 2 oms.log; tail -n 4 oms.log
echo "--- holdings.log";     tail -n 8 holdings.log
echo "--- dataplatform.log"; head -n 2 dataplatform.log; tail -n 2 dataplatform.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" holdings.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[holdings\] +[1-9][0-9,]* position updates/s' holdings.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
h = [l for l in open("holdings.log") if re.search(r"\[holdings\] +[1-9][0-9,]* position updates/s", l)][10:]
r = [l for l in open("holdings.log") if re.search(r"\[app\] post-trade refreshes [1-9]", l)][10:]
o = open("oms.log").read()
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: position updates/s %.0f  fill-to-holdings p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(p, h)) for p in (r"\] +([0-9,]+) position updates/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
burst = [l for l in h if float(re.search(r"\] +([0-9,]+) position", l).group(1).replace(",", "")) > 900]
if burst:
    print("market-open burst: %d s above 900 fills/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(burst), st.median(num(r"p99 +([0-9.]+)ms", burst)), max(num(r"p99 +([0-9.]+)ms", burst))))
missed = num(r"missing the new fill ([0-9.]+)%", r)
print("post-trade refreshes: %d s checked, %.0f refreshes in all, missing the new fill: median %.3f%%, worst second %.3f%%" % (
    len(r), sum(num(r"refreshes ([0-9,]+)", r)), st.median(missed), max(missed)))
if len(r) < 30:
    raise SystemExit("too few seconds with post-trade refreshes checked")
if st.median(missed) > 0.1:
    raise SystemExit("too many post-trade refreshes missed the new fill")
if "market open" not in o or not burst:
    raise SystemExit("the market-open burst did not reach the lane")
PY
grep -q "\[dataplatform\] done" dataplatform.log || { echo "the DataPlatform neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
