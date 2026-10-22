#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the mark price lane at small scale for about a minute with two
# liquidation workers, a market crash and a trade export next door. Pass only if each worker holds the 50 ms lane
# p99 in 95% of its steady-state seconds, liquidation orders flow back and are filled, and the comparison with a
# worker running 0.5 s behind is measured.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# The runner's kernel holds up to 30 s of dirty pages by default and writes them in one burst; with a few MB/s
# neighbour that burst stalled every reader 0.2-0.8 s at once. Flush small and often instead: it is the runner's
# disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py exchange.py liquidator.py trade_export.py kafka_bridge.py
# 505 instruments with a mark batch every second, 40,000 open positions, 600 position changes/s, the market
# falling 5% from second 30 to 45, and 2 MB/s of trade export from second 20 for 35 s.
timeout 84 python -u exchange.py --instruments 505 --positions 40000 --rate 600 --crash-at 30 --crash-for 15 --crash-pct 5 > exchange.log 2>&1 &
sleep 4
for n in 1 2; do
  timeout 74 python -u liquidator.py --name liquidator-$n --shard $((n - 1)) --shards 2 > liquidator-$n.log 2>&1 &
done
timeout 70 python -u trade_export.py --mbps 2 --at 20 --duration 35 > export.log 2>&1 &
wait || true
echo "--- exchange.log"; head -n 1 exchange.log; sed -n '30,34p' exchange.log; tail -n 2 exchange.log
for n in 1 2; do echo "--- liquidator-$n.log"; head -n 3 liquidator-$n.log; tail -n 3 liquidator-$n.log; done
echo "--- exchange seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" exchange.log | sed -E 's/past liquidation price.*open/open/' || true
echo "--- export.log"; head -n 2 export.log; tail -n 2 export.log
for n in 1 2; do
  echo "--- liquidator-$n FAIL seconds (line: output)"; grep -n "FAIL (" liquidator-$n.log | sed -E 's/  liquidation orders.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[liquidator-$n\] +[1-9][0-9,]* marks/s" liquidator-$n.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "liquidator-$n steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for n in (1, 2):
    p = [l for l in open(f"liquidator-{n}.log") if re.search(r"\] +[1-9][0-9,]* marks/s", l)][10:]
    print("liquidator-%d steady-state medians: marks/s %.0f  position changes/s %.0f  lane p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst p99 %.1f ms  mark batch checked max %.1f ms  orders %d" % (
        (n,) + tuple(st.median(nums(q, p)) for q in (r"\] +([0-9,]+) marks/s", r"([0-9,]+) position changes/s",
                                                     r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms"))
        + (max(nums(r"p99 +([0-9.]+)ms", p)), max(nums(r"checked in ([0-9.]+)ms", p)),
           sum(nums(r"liquidation orders ([0-9,]+)", p)))))
l = [x for x in open("exchange.log") if "liquidations filled" in x][12:]
crash = [x for x in l if "(crash)" in x]
filled, stale, over, fund, lover, lfund, ln = (sum(nums(q, l)) for q in (
    r"liquidations filled ([0-9,]+)", r"stale ([0-9,]+)", r"past liquidation price ([0-9,]+) USDT",
    r"  past bankruptcy ([0-9,]+) USDT  a worker", r"behind: past liquidation ([0-9,]+) USDT",
    r"behind: past liquidation [0-9,]+ USDT  past bankruptcy ([0-9,]+) USDT", r"USDT \(([0-9,]+) fills\)"))
p99 = nums(r"mark-to-fill p99 ([0-9.]+)ms", [x for x in l if not re.search(r"filled 0 ", x)])
print("after warm-up: liquidations filled %d (stale %d)  mark-to-fill p99 median %.1f ms" % (filled, stale, st.median(p99) if p99 else float("nan")))
print("fills past the liquidation price: %.0f USDT live, %.0f USDT for a worker 0.5 s behind (%d fills)" % (over, lover, ln))
print("fills past the bankruptcy price (insurance fund): %.0f USDT live, %.0f USDT for a worker 0.5 s behind" % (fund, lfund))
print("crash seconds: %d with %d liquidations filled" % (len(crash), sum(nums(r"liquidations filled ([0-9,]+)", crash))))
if filled < 200 or ln < 100:
    raise SystemExit("liquidation orders or the lagged comparison did not flow")
PY
grep -q "\[export\] done" export.log || { echo "the export neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
