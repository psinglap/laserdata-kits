#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Moloco Commerce Media item status lane at small scale for
# about a minute and a half: two Decision API servers, Item Kill Switch bulk calls every 5 s, a flash sale on one
# category with a 4x shopper-event burst next door, and a third server added as the sale opens, as the GKE
# autoscaler would.
# Pass only if each server holds the 20 ms p99 budget in 95% of its steady-state seconds with no sequence gaps,
# stock-outs and kill-switch blocks reach the servers and are counted as ads for unavailable items, and the added
# server rebuilds every item's status from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after a heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py status.py decision_node.py user_events.py status_bridge.py
# 16 categories x 5,000 items = 80,000 items in live campaigns; 2,000 stock updates a second, 40 stock-outs and
# 30 restocks a second, a kill-switch bulk call blocking 50 items every 5 s; a flash sale on electronics from
# second 40 to 55 (5x its stock updates and stock-outs); 1 MB/s of shopper events next door, 4x over the same 15 s.
C=16; I=5000; D=2000
timeout 92 python -u status.py --cats $C --items-per-cat $I --rate 2000 --outs 40 --backs 30 --kill-every 5 --kill-items 50 \
  --sale-cat electronics --sale-at 40 --sale-for 15 --sale-x 5 --snapshot-every 15 > status.log 2>&1 &
sleep 4
timeout 80 python -u user_events.py --mbps 1 --x 4 --at 36 --for 15 --duration 74 > user-events.log 2>&1 &
timeout 82 python -u decision_node.py --name dapi-1 --cats $C --items-per-cat $I --decisions-per-s $D --replay-s 0 > dapi-1.log 2>&1 &
timeout 34 python -u decision_node.py --name dapi-2 --cats $C --items-per-cat $I --decisions-per-s $D --replay-s 0 > dapi-2.log 2>&1 || true
sleep 1
# dapi-3 is added as the flash sale opens: it must rebuild every item's status from the last 20 s of the log
# (one 15 s rolling snapshot plus the changes since) while the sale is running.
timeout 46 python -u decision_node.py --name dapi-3 --cats $C --items-per-cat $I --decisions-per-s $D --replay-s 20 > dapi-3.log 2>&1 || true
wait || true
echo "--- status.log";      grep -m 1 snapshot status.log || true; grep -m 1 "flash sale" status.log || true; tail -n 3 status.log
echo "--- user-events.log"; head -n 1 user-events.log; grep -m 1 -E 'records/s .*burst' user-events.log || true; tail -n 2 user-events.log
for f in dapi-1 dapi-2 dapi-3; do
  echo "--- $f.log"; grep -E "rebuilt|without the log" $f.log || true; tail -n 4 $f.log | cut -c1-330
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" dapi-*.log | cut -c1-170 || true
status=0
# Skip each server's first 10 live seconds (warm-up, or the replay right after it was added); a shared CI runner
# may blip, so require 95% of each server's remaining seconds to pass, and no sequence gap anywhere.
for node in dapi-1:40 dapi-2:18 dapi-3:28; do
  name=${node%%:*}; need=${node##*:}
  s=$(grep -E "\[$name\] +[1-9][0-9,]* status changes/s" $name.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$name steady-state seconds with live status changes: PASS $live, FAIL $fail (need $need live, 95% PASS)"
  if [ "$live" -lt "$need" ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
  gaps=$(grep -Eo 'gaps [0-9]+' $name.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$name sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "item status lane check failed"; exit 1; }
grep -Eq 'rebuilt from the last 20 s of the log in [0-9.]+s: status for ([0-9,]+) of \1 items' dapi-3.log \
  || { echo "the added server did not rebuild every item's status"; exit 1; }
if ! grep -Eq 'stock-outs [1-9][0-9,]*  blocks [0-9,]+  restocks [0-9,]+  ads for unavailable items' dapi-1.log; then
  echo "no stock-outs reached the Decision API server"; exit 1; fi
if ! grep -Eq 'blocks [1-9][0-9,]*  restocks' dapi-1.log; then echo "no kill-switch blocks reached the server"; exit 1; fi
grep -q "flash sale: electronics" status.log || { echo "the flash sale did not run"; exit 1; }
grep -q "\[user-events\] done" user-events.log || { echo "the shopper-event neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[dapi-[123]\] +([0-9,]+) status changes/s"
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("dapi-1", "dapi-3"):
    t = [l for l in open(f"{name}.log") if re.search(live, l) and not re.search(r"\] +0 status changes/s", l)][10:]
    if not t:
        continue
    p99 = nums(r"p99 +([0-9.]+)ms", t)
    med = lambda pat: st.median(nums(pat, t) or [float("nan")])
    print("%s steady-state medians over %d s: status changes/s %.0f  status-to-server p50 %.2f ms  p99 %.2f ms  "
          "p99.9 %.2f ms  worst-second p99 %.2f ms  stock-outs/s %.0f  blocks/s %.0f  restocks/s %.0f  "
          "ads for unavailable items/s %.4f (%.4f per 1M decisions)  1 s behind %.1f/s" % (
          name, len(t), med(live), med(r"p50 +([0-9.]+)ms"), med(r"p99 +([0-9.]+)ms"), med(r"p99.9 +([0-9.]+)ms"),
          max(p99 or [float("nan")]), med(r"stock-outs ([0-9,]+)"), med(r"blocks ([0-9,]+)"), med(r"restocks ([0-9,]+)"),
          med(r"ads for unavailable items ([0-9,.]+)/s"), med(r"\(([0-9.]+) per 1M decisions\)"),
          med(r"1 s behind ([0-9,.]+)/s")))
    print("%s totals over those seconds: ads for unavailable items %.3f, at 1 s behind %.0f" % (
          name, sum(nums(r"ads for unavailable items ([0-9,.]+)/s", t)), sum(nums(r"1 s behind ([0-9,.]+)/s", t))))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("dapi-1.log") if re.search(live, l)])
if len(rows) > 56:   # the flash sale and the shopper-event burst run from status.py's second 40 to 55, about dapi-1's 36 to 51
    print("dapi-1 p99, median per second: before the flash sale %.2f ms (s 15-33), during it %.2f ms (s 38-50)" % (
        st.median(rows[15:34]), st.median(rows[38:51])))
for f in ("dapi-3.log", "dapi-1.log"):
    for l in open(f):
        if "rebuilt" in l or "without the log" in l:
            print(l.strip())
sl = [l for l in open("status.log") if "status changes/s" in l]
sale, calm = nums(r"\] +([0-9,]+) status changes/s", [l for l in sl if "flash sale" in l]), \
             nums(r"\] +([0-9,]+) status changes/s", [l for l in sl if "flash sale" not in l][2:])
outs_sale = nums(r"stock-outs ([0-9,]+)", [l for l in sl if "flash sale" in l])
outs_calm = nums(r"stock-outs ([0-9,]+)", [l for l in sl if "flash sale" not in l][2:])
if sale and calm:
    print("status.py: median %.0f status changes/s (%.0f stock-outs/s), %.0f (%.0f) during the flash sale (%d s)" % (
        st.median(calm), st.median(outs_calm), st.median(sale), st.median(outs_sale), len(sale)))
e = nums(r"~([0-9.]+) MB/s", [l for l in open("user-events.log") if re.search(r"\] +[1-9][0-9,]* records/s", l)])
if e:
    print("shopper-event neighbour: %d s, median %.1f MB/s, peak %.1f MB/s on its own topic" % (len(e), st.median(e), max(e)))
PY
echo "Kit OK on Apache Iggy 0.9.0"
