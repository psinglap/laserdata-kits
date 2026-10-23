#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the geohash forecast lane at small scale for about a minute:
# 40 regions x 150 geohash-6 cells (6,000 cells, 12 demand and 12 supply values each) in a frame every second,
# Rider Pricing quoting 200 rides a second, Driver Earnings restarted once (as a deploy would), a concert spike in
# one region, and 2 MB/s of analytics events next door.
# Pass only if each reader holds the 50 ms chunk-to-reader p99 in 95% of its steady-state seconds, the restarted
# reader rebuilds every region's latest frame from the log, frames reach both readers, the concert frames arrive,
# and fewer quotes are priced on a superseded forecast from the live lane than from a copy polled every 0.2 s.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst stalled every
# partition for 0.2-0.8 s at once while a heavy neighbour ran; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py forecaster.py marketplace.py analytics_flood.py msk_bridge.py
timeout 82 python -u forecaster.py --regions 40 --cells 150 --cadence-s 1 --chunk 50 --event-at 30 --event-for 15 > forecaster.log 2>&1 &
sleep 4
timeout 70 python -u analytics_flood.py --mbps 2 --at 20 --duration 35 > analytics.log 2>&1 &
timeout 76 python -u marketplace.py --role pricing --quotes-per-s 200 --poll-s 0.2 > pricing.log 2>&1 &
# Driver Earnings runs, stops at 24 s and starts again: it must rebuild every region's latest frame from the log.
timeout 24 python -u marketplace.py --role earnings > earnings.log 2>&1 || true
sleep 1
timeout 51 python -u marketplace.py --role earnings --rebuild-s 3 > earnings-restart.log 2>&1 || true
wait || true
echo "--- forecaster.log"; head -n 1 forecaster.log; sed -n '30,32p' forecaster.log; tail -n 2 forecaster.log
echo "--- forecaster seconds with a frame started or sent over 50 ms late"; grep -nE "(send|late) max ([5-9][0-9]|[0-9]{3,})ms" forecaster.log || true
echo "--- analytics.log"; head -n 2 analytics.log; tail -n 2 analytics.log
for f in pricing earnings earnings-restart; do echo "--- $f.log"; head -n 2 $f.log | cut -c1-400; tail -n 2 $f.log | cut -c1-400; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" pricing.log earnings*.log | cut -c1-200 || true
steady() { for f in "$@"; do grep -E '\] +[1-9][0-9,]* forecast msgs/s' $f | tail -n +11 || true; done; }
for r in pricing earnings; do
  if [ $r = pricing ]; then s=$(steady pricing.log); else s=$(steady earnings.log earnings-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$r steady-state seconds with live frames: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt the latest frame of 40 regions from the log" earnings-restart.log \
  || { echo "the restarted reader did not rebuild every region's latest frame from the log"; exit 1; }
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) forecast msgs/s"
for n in ("pricing", "earnings", "earnings-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: msgs/s %.0f  chunk-to-reader p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst p99 %.1f ms  seal-to-ready p99 %.1f ms  frames ready/s %.0f" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"-(?:pricing|earnings) p50 +([0-9.]+)ms", r"p50 +[0-9.]+ms +p99 +([0-9.]+)ms", r"p99\.9 +([0-9.]+)ms")) +
              (max(nums(r"p50 +[0-9.]+ms +p99 +([0-9.]+)ms", p)),) +
              tuple(st.median(nums(q, p) or [0]) for q in (r"seal-to-ready p99 ([0-9.]+)ms", r"frames ready ([0-9,]+) "))))
for l in open("earnings-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
p = [l for l in open("pricing.log") if re.search(live, l)][10:]
q, j, sl, sp = (sum(nums(x, p)) for x in (r"quotes ([0-9,]+) \(", r"judged ([0-9,]+):", r"live lane ([0-9,]+) ", r"s poll ([0-9,]+) "))
print("after warm-up (%d s): quotes priced %d  judged %d | on a superseded forecast: live lane %d (%.2f%%)  0.2 s poll %d (%.2f%%)"
      % (len(p), q, j, sl, 100 * sl / max(j, 1), sp, 100 * sp / max(j, 1)))
ev = sum(nums(r"concert frames ([0-9]+)", [l for n in ("pricing", "earnings-restart") for l in open(f"{n}.log")]))
print("concert-spike frames ready at the readers: %d" % ev)
if j < 4000:
    raise SystemExit("quotes did not flow")
if sl >= sp:
    raise SystemExit("the live lane priced as many quotes on a superseded forecast as a 0.2 s poll")
if ev < 10:
    raise SystemExit("the concert-spike frames did not reach the readers")
PY
echo "Kit OK on Apache Iggy 0.9.0"
