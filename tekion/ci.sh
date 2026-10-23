#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the vehicle context lane at small scale for about a minute, with a
# dealer group's repricing pass and a lakehouse CDC flood next to it, and pass only if the agent context reports
# PASS, test-drive bookings flow end to end, and the TTL-cache comparison is measured.
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
python -m py_compile common.py dms_inventory.py agent_context.py cdc_flood.py msk_bridge.py
# 200 rooftops x 150 vehicles, 800 vehicle changes/s, a 3x repricing pass on a 40-rooftop group from second 30 to 45,
# 150 agent answers/s against a 30 s TTL comparison, and 2 MB/s of lakehouse CDC from second 20 for 35 s.
timeout 80 python -u dms_inventory.py --rooftops 200 --vehicles 150 --rate 800 --reprice-run 3 --reprice-at 30 \
  --reprice-for 15 --group-rooftops 40 > dms.log 2>&1 &
sleep 5
timeout 70 python -u cdc_flood.py --mbps 2 --at 20 --duration 35 > cdc.log 2>&1 &
timeout 70 python -u agent_context.py --answers-per-s 150 --ttl 30 > context.log 2>&1 || true
wait || true
echo "--- dms.log";     head -n 1 dms.log; tail -n 4 dms.log
echo "--- context.log"; head -n 3 context.log; tail -n 6 context.log
echo "--- cdc.log";     head -n 2 cdc.log; tail -n 2 cdc.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" context.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[context\] +[1-9][0-9,]* changes/s' context.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\[context\] +[1-9][0-9,]* changes/s"
all_c = [l for l in open("context.log") if re.search(live, l)]
c = all_c[10:]
print("steady-state medians: changes/s %.0f  change-to-context p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, c)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
p99, rate = nums(r"p99 +([0-9.]+)ms", all_c), nums(r"\] +([0-9,]+) changes/s", all_c)
base = st.median(rate)
run = [i for i, r in enumerate(rate) if r > 1.4 * base]        # the repricing seconds, found from the rate itself
off = [i for i in range(10, len(rate)) if i not in run]
if run and off:
    print("p99 median per second: repricing pass %.1f ms over %d s (median %.0f changes/s, worst second %.1f ms), other steady seconds %.1f ms" % (
        st.median(p99[i] for i in run), len(run), st.median(rate[i] for i in run), max(p99[i] for i in run), st.median(p99[i] for i in off)))
answers, drives = sum(nums(r"answers ([0-9,]+)", c)), sum(nums(r"test drives ([0-9,]+)", c))
gone, moved = sum(nums(r"would have offered ([0-9,]+) vehicles", c)), sum(nums(r"and ([0-9,]+) old prices", c))
share = nums(r"old prices \(([0-9.]+)% of answers\)", c)
print("agent answers %d  test drives sent %d  a 30 s TTL cache would have offered %d vehicles no longer in stock and %d old prices "
      "(median %.1f%% of answers)" % (answers, drives, gone, moved, st.median(share) if share else 0))
if answers < 1000 or drives < 1000 or gone + moved < 1:
    raise SystemExit("agent answers, test drives or the TTL comparison did not flow")
d = [x for x in open("dms.log") if "test drives booked" in x][15:]
ok, dec, mv = (sum(nums(q, d)) for q in (r"booked ([0-9,]+)  decl", r"no longer available ([0-9,]+)", r"moved price ([0-9,]+)"))
print("DMS after warm-up: test drives booked %d  declined as no longer available %d (%.2f%%)  booked at a moved price %d" % (
    ok, dec, 100 * dec / max(ok + dec, 1), mv))
if ok < 1000:
    raise SystemExit("test-drive bookings did not reach the DMS")
PY
grep -q "\[cdc\] done" cdc.log || { echo "the lakehouse CDC neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
