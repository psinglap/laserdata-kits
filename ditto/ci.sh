#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run one Ditto CDC Data Bridge lane at small scale for about a minute:
# 30,000 cabin_orders documents on 12 partitions keyed by _id, 1,000 changes/s with a 3x landing wave (3,000/s)
# from second 30 to 45, 0.5% of events delivered twice, one requeryRequired, two downstream mirrors from the start,
# a third added at second 40 that rebuilds from the log, and 2 MB/s of bulk export next door. Pass only if each of
# the first two mirrors holds the 100 ms change-to-mirror p99 in 95% of its steady-state seconds, no mirror ever
# breaks a change chain (a lost, repeated or reordered change), repeats are dropped, every mirror sees the
# requeryRequired, and the late mirror rebuilds from the log and then keeps up.
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
python -m py_compile common.py big_peer.py mirror.py bulk_export.py kafka_bridge.py
timeout 82 python -u big_peer.py --docs 30000 --rate 1000 --wave 3 --wave-at 30 --wave-for 15 --landed 20 \
  --dup-share 0.005 --requery-at 55 > big_peer.log 2>&1 &
sleep 4
for n in ops-dashboard catering; do
  timeout 72 python -u mirror.py --name $n > $n.log 2>&1 &
done
timeout 66 python -u bulk_export.py --mbps 2 --at 16 --duration 35 > export.log 2>&1 &
sleep 36
timeout 36 python -u mirror.py --name warehouse > warehouse.log 2>&1 &
wait || true
echo "--- big_peer.log"; head -n 2 big_peer.log; grep -E "requeryRequired|landing wave" big_peer.log | head -n 3; tail -n 2 big_peer.log
for n in ops-dashboard catering warehouse; do echo "--- $n.log"; head -n 3 $n.log; grep "requeryRequired" $n.log || true; tail -n 2 $n.log; done
echo "--- big_peer seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" big_peer.log || true
echo "--- export.log"; head -n 2 export.log; tail -n 2 export.log
for n in ops-dashboard catering; do
  echo "--- $n FAIL seconds (line: output)"; grep -n "FAIL (" $n.log | sed -E 's/  repeats.*docs//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[$n\] +[1-9][0-9,]* changes/s" $n.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$n steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
  if ! grep -q "requeryRequired at txnId" $n.log; then echo "$n did not see requeryRequired"; exit 1; fi
done
if grep -qE "chains broken [1-9]" ops-dashboard.log catering.log warehouse.log; then echo "a change chain broke"; exit 1; fi
grep -q "rebuilt the mirror from the log" warehouse.log || { echo "the late mirror did not rebuild"; exit 1; }
wlive=$(grep -cE "\[warehouse\] +[1-9][0-9,]* changes/s.*PASS \(" warehouse.log || true)
echo "warehouse live seconds inside the budget after its rebuild: $wlive"
if [ "$wlive" -lt 5 ]; then echo "the late mirror did not keep up after its rebuild"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for n in ("ops-dashboard", "catering"):
    p = [l for l in open(f"{n}.log") if re.search(r"\] +[1-9][0-9,]* changes/s", l)][10:]
    print("%s steady-state medians: changes/s %.0f  change-to-mirror p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst p99 %.1f ms" % (
        (n,) + tuple(st.median(nums(q, p)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                                     r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
    rep = sum(nums(r"repeats dropped ([0-9,]+)", open(f"{n}.log")))
    print("%s repeats dropped by txnId %d, chains broken %d" % (n, rep, sum(nums(r"chains broken ([0-9,]+) \(", open(f"{n}.log")))))
    if rep < 1:
        raise SystemExit(f"{n}: no repeated event was dropped")
w = [l for l in open("warehouse.log") if "rebuilt the mirror" in l]
print("warehouse (added at second 40): " + w[0].split("] ", 1)[1].strip())
b = [l for l in open("big_peer.log") if "changes/s" in l]
wave = [l for l in b if "landing wave" in l]
print("landing-wave seconds: %d at a median %.0f changes/s" % (len(wave), st.median(nums(r"\] +([0-9,]+) changes/s", wave)) if wave else 0))
PY
grep -q "\[export\] done" export.log || { echo "the export neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
