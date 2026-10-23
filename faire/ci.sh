#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the FBF stock lane at small scale for about a minute, with a Faire
# Market order burst and lakehouse CDC next to it, and pass only if the checkout view reports PASS, multi-brand
# orders flow end to end, and the periodic-sync comparison is measured.
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
python -m py_compile common.py warehouse.py checkout.py lake_cdc.py changefeed_bridge.py
# 150 brands x 120 SKUs, 800 stock changes/s, a 3x Faire Market order burst from second 30 to 45,
# 60 multi-brand checkouts/s against a 60 s periodic-sync comparison, and 2 MB/s of lakehouse CDC from second 20 for 35 s.
timeout 80 python -u warehouse.py --brands 150 --skus 120 --rate 800 --market-x 3 --market-at 30 --market-for 15 \
  > warehouse.log 2>&1 &
sleep 5
timeout 70 python -u lake_cdc.py --mbps 2 --at 20 --duration 35 > cdc.log 2>&1 &
timeout 70 python -u checkout.py --carts-per-s 60 --sync-every 60 > checkout.log 2>&1 || true
wait || true
echo "--- warehouse.log"; head -n 1 warehouse.log; tail -n 4 warehouse.log
echo "--- checkout.log";  head -n 3 checkout.log; tail -n 6 checkout.log
echo "--- cdc.log";       head -n 2 cdc.log; tail -n 2 cdc.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" checkout.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[checkout\] +[1-9][0-9,]* changes/s' checkout.log | tail -n +11 || true)
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
live = r"\[checkout\] +[1-9][0-9,]* changes/s"
all_c = [l for l in open("checkout.log") if re.search(live, l)]
c = all_c[10:]
print("steady-state medians: changes/s %.0f  change-to-checkout p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, c)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
p99, rate = nums(r"p99 +([0-9.]+)ms", all_c), nums(r"\] +([0-9,]+) changes/s", all_c)
base = st.median(rate)
run = [i for i, r in enumerate(rate) if r > 1.4 * base]        # the Faire Market seconds, found from the rate itself
off = [i for i in range(10, len(rate)) if i not in run]
if run and off:
    print("p99 median per second: Faire Market burst %.1f ms over %d s (median %.0f changes/s, worst second %.1f ms), other steady seconds %.1f ms" % (
        st.median(p99[i] for i in run), len(run), st.median(rate[i] for i in run), max(p99[i] for i in run), st.median(p99[i] for i in off)))
carts, orders = sum(nums(r"carts ([0-9,]+)", c)), sum(nums(r"orders ([0-9,]+)", c))
wrong = sum(nums(r"would have accepted ([0-9,]+) lines", c))
share = nums(r"can't fill \(([0-9.]+)% of carts\)", c)
print("checkouts %d  orders placed %d  a 60 s inventory sync would have accepted %d lines the warehouse can't fill "
      "(median %.1f%% of carts)" % (carts, orders, wrong, st.median(share) if share else 0))
if carts < 1000 or orders < 1000 or wrong < 1:
    raise SystemExit("checkouts, orders or the periodic-sync comparison did not flow")
w = [x for x in open("warehouse.log") if "lines reserved" in x][15:]
ok, short, so = (sum(nums(q, w)) for q in (r"lines reserved ([0-9,]+)", r"short at the warehouse ([0-9,]+)", r"with a short line ([0-9,]+)"))
print("warehouse after warm-up: lines reserved %d  short at the warehouse %d (%.2f%%)  orders with a short line %d" % (
    ok, short, 100 * short / max(ok + short, 1), so))
if ok < 1000:
    raise SystemExit("checkout orders did not reach the warehouse")
PY
grep -q "\[cdc\] done" cdc.log || { echo "the lakehouse CDC neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
