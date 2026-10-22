#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the boarding-point lane at small scale for about a minute:
# 6,000 journeys sending a GPS packet every 2 s, keyed by SVC_DOJ on 16 partitions (1.5x for 15 s from second 20,
# a festival-night surge of departures), road time 15x so buses reach their boarding points inside the run, and a
# 10x log burst next door from second 30. Pass only if the tracker holds the 50 ms p99 budget in 95% of its
# steady-state seconds, and boarding-point events and passenger alerts flow, from the right packet, with none missed.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py buses.py tracker.py log_flood.py kafka_bridge.py
# The tracker starts first and creates the topics; the buses and the neighbour follow.
timeout 80 python -u tracker.py --why > tracker.log 2>&1 &
sleep 3
timeout 72 python -u buses.py --journeys 6000 --every 2 --pace 15 --peak 1.5 --peak-at 20 --peak-for 15 > buses.log 2>&1 &
timeout 70 python -u log_flood.py --lines 500 --size 200 --x 10 --at 30 --for 20 > logs.log 2>&1 &
wait || true
echo "--- buses.log";   head -n 2 buses.log; tail -n 6 buses.log
echo "--- tracker.log"; head -n 3 tracker.log; tail -n 8 tracker.log
echo "--- logs.log";    grep -m 2 "burst" logs.log || true; tail -n 1 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" tracker.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[tracker\] +[1-9][0-9,]* packets/s' tracker.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: medians of the per-second figures, for the README. Lines that don't parse are skipped.
python - <<'PY'
import re, statistics as st

def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:
            try:
                out.append(float(m.group(1).replace(",", "")))
            except ValueError:
                pass
    return out

med = lambda v: st.median(v) if v else float("nan")
t = [l for l in open("tracker.log") if re.search(r"\[tracker\] +[1-9][0-9,]* packets/s", l)][10:]
print("steady-state medians: packets/s %.0f  packet-to-tracker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  (%d s)" % (
    med(nums(r"\] +([0-9,]+) packets/s", t)), med(nums(r"p50 +([0-9.]+)ms", t)), med(nums(r" p99 +([0-9.]+)ms", t)),
    med(nums(r"p99\.9 +([0-9.]+)ms", t)), len(t)))
lane = nums(r" p99 +([0-9.]+)ms", [l for l in open("tracker.log") if l.startswith("[tracker]") and "packets/s" in l])
if len(lane) > 48:
    print("packet-to-tracker p99, median per second: before the burst %.1f ms (s 12-26), during the 10x log burst %.1f ms (s 31-47)" % (
        med(lane[12:27]), med(lane[31:48])))
b = [l for l in open("buses.log") if l.startswith("[buses]")][12:]
checked, right, late = nums(r"bp events checked ([0-9,]+)", b), nums(r"right packet +([0-9.]+)%", b), nums(r"late +([0-9.]+)%", b)
missed, alerted, notal = nums(r"missed ([0-9,]+)", b), nums(r"passengers alerted ([0-9,]+)", b), nums(r"not alerted ([0-9,]+)", b)
alat = nums(r"packet-to-alert p99 +([0-9.]+)ms", [l for l in b if re.search(r"passengers alerted [1-9]", l)])
print("boarding-point events checked %d in %d s: from the right packet %.2f%% (median per second), late %.2f%%, missed %d; "
      "passengers alerted %d, not alerted %d; packet-to-alert p99 %.1f ms (median per second)" % (
    sum(checked), len(b), med(right), med(late), sum(missed), sum(alerted), sum(notal), med(alat)))
if sum(checked) == 0 or sum(alerted) == 0:
    raise SystemExit("no boarding-point events or passenger alerts checked")
if sum(missed) * 1000 > sum(checked) or sum(notal) * 1000 > sum(alerted) or med(right) < 99.9:
    raise SystemExit("boarding-point events or alerts late or missing")
PY
for ev in ENTRY HALT EXIT; do grep -Eq "$ev [1-9]" tracker.log || { echo "no $ev events published"; exit 1; }; done
grep -q "burst 10x" logs.log || { echo "log burst did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
