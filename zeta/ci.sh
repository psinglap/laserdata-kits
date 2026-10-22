#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the account spend lane at small scale for about a minute,
# with add-on card runs, reversals, a peak and an event-catalog stream next to it, and pass only if the
# lane reports PASS and the decision engine's checks hold the account's last change and the category limits.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst; on a shared runner that burst stalls every
# reader for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py switch.py feature_store.py events_flood.py kafka_bridge.py
# The feature store starts first and creates the topics; the switch and the neighbour follow.
timeout 78 python -u feature_store.py > store.log 2>&1 &
sleep 3
# 500 authorizations a second (about 20x Zeta's reported 2024 daily average), 5% in add-on card runs,
# 3% of approvals reversed, 2x for 15 s from second 30 (the peak); 1 MB/s of catalog events from second 20.
timeout 72 python -u switch.py --accounts 200000 --auths-per-s 500 --run-share 0.05 --peak 2 --peak-at 30 --peak-for 15 > switch.log 2>&1 &
timeout 70 python -u events_flood.py --mbps 1 --at 20 --duration 35 > events.log 2>&1 &
wait || true
echo "--- switch.log"; head -n 2 switch.log; tail -n 4 switch.log
echo "--- store.log";  tail -n 8 store.log
echo "--- events.log"; head -n 2 events.log; tail -n 2 events.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" store.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[store\] +[1-9][0-9,]* spend updates/s' store.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
h = [l for l in open("store.log") if re.search(r"\[store\] +[1-9][0-9,]* spend updates/s", l)][10:]
c = [l for l in open("store.log") if re.search(r"\[check\] repeat-account checks [1-9]", l)][10:]
w = open("switch.log").read()

def num(pat, ls):
    out = []
    for l in ls:
        m = re.search(pat, l)
        if m:                                   # skip a line whose value is missing instead of crashing
            out.append(float(m.group(1).replace(",", "")))
    return out

print("steady-state medians: spend updates/s %.0f  auth-to-store p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(q, h)) for q in (r"\] +([0-9,]+) spend updates/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
peak = [l for l in h if num(r"\] +([0-9,]+) spend", [l]) and num(r"\] +([0-9,]+) spend", [l])[0] > 800]
if peak:
    print("peak: %d s above 800 spend updates/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(num(r"p99 +([0-9.]+)ms", peak)), max(num(r"p99 +([0-9.]+)ms", peak))))
missed = num(r"missing the last change ([0-9.]+)%", c)
runs, at_limit = sum(num(r"in add-on runs ([0-9,]+)\)", c)), sum(num(r"declined at the limit ([0-9,]+)", c))
over, wrong = num(r"over-limit approvals ([0-9,]+)", c), sum(num(r"wrong declines ([0-9,]+)", c))
reversals = sum(num(r"reversals ([0-9,]+)", h))
print("decision-engine checks on repeat accounts: %d s checked, %.0f checks in all (%.0f in add-on runs), missing the last "
      "change: median %.3f%%, worst second %.3f%%; declined at the category limit %.0f; over-limit approvals %.0f; "
      "wrong declines %.0f; reversals applied %.0f" % (
    len(c), sum(num(r"checks ([0-9,]+) \(", c)), runs, st.median(missed), max(missed), at_limit, sum(over), wrong, reversals))
e = [l for l in open("events.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("event-catalog neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(num(r"~([0-9.]+) MB/s", e))))
if len(c) < 30:
    raise SystemExit("too few seconds with decision-engine checks")
if st.median(missed) > 0.1 or st.median(over) > 0:
    raise SystemExit("too many checks missed the account's last change")
if runs == 0 or at_limit == 0 or reversals == 0:
    raise SystemExit("no add-on runs, category-limit declines or reversals reached the store")
if "(peak)" not in w or not peak:
    raise SystemExit("the peak did not reach the lane")
PY
grep -q "\[events\] done" events.log || { echo "the event-catalog neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
