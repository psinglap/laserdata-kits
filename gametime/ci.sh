#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Zone Deals pool lane at small scale for about a minute: 120
# events x 6 zones x 25 seller listings, the hottest event 4x that (20,250 listings), 600 listing changes a second
# (sold elsewhere, automatic pricing, seller repricing, delists, new tickets), 60 Zone Deals orders a second, one
# event's final hour (automatic pricing drops its listings, its Zone Deals drop, 300 extra changes a second on it
# and 180 extra orders a second on it for 15 s),
# 2 MB/s of app events next door, two pickers (each owning half the zones' orders), and one restarted.
# Pass only if each picker holds the 50 ms change-to-pool p99 in 95% of its steady-state seconds, the restarted
# one rebuilds its pool from the log, and picks are checked against the listings, with fewer re-picks than a pool
# refreshed every 3 s would have caused.
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
python -m py_compile common.py listings.py zone_picker.py app_events_flood.py confluent_bridge.py
timeout 82 python -u listings.py --events 120 --zones 6 --listings 25 --rate 600 --orders 60 \
  --final-hour 4 --final-at 40 --final-for 15 --hot-changes 300 > listings.log 2>&1 &
sleep 4
timeout 70 python -u app_events_flood.py --mbps 2 --at 20 --duration 35 > app-events.log 2>&1 &
timeout 76 python -u zone_picker.py --name zp-1 --shard 0 --shards 2 > zp-1.log 2>&1 &
# zp-2 runs, stops at 24 s and starts again: it must rebuild its pool from the log before it takes an order.
timeout 24 python -u zone_picker.py --name zp-2 --shard 1 --shards 2 > zp-2.log 2>&1 || true
sleep 1
timeout 51 python -u zone_picker.py --name zp-2 --shard 1 --shards 2 > zp-2-restart.log 2>&1 || true
wait || true
echo "--- listings.log"; head -n 1 listings.log; grep -m 1 "final hour on" listings.log || true
grep -E "\(final hour\)" listings.log | head -n 3 | cut -c1-420 || true; tail -n 2 listings.log | cut -c1-420
echo "--- listings seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" listings.log | sed -E 's/zone orders.*tick/tick/' || true
echo "--- app-events.log"; head -n 2 app-events.log; tail -n 2 app-events.log
for f in zp-1 zp-2 zp-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" zp-*.log | sed -E 's/  zone orders picked.*//' || true
steady() { for f in "$@"; do grep -E '\[zp-[12]\] +[1-9][0-9,]* changes/s' $f | tail -n +11 || true; done; }
for zp in zp-1 zp-2; do
  if [ $zp = zp-1 ]; then s=$(steady zp-1.log); else s=$(steady zp-2.log zp-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$zp steady-state seconds with live changes: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt the pool from the log in [0-9.]+s: [1-9][0-9,]* listings" zp-2-restart.log \
  || { echo "the restarted picker did not rebuild its pool from the log"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) changes/s"
for n in ("zp-1", "zp-2", "zp-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: changes/s %.0f  change-to-pool p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
for l in open("zp-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the pickers have stopped (nothing picked)
f = [l for l in open("listings.log") if "zone orders" in l and not re.search(r"\bpicked 0 \(", l)][12:]
fin = [l for l in f if "(final hour)" in l]
def tot(lines):
    return [sum(nums(q, lines)) for q in (r"picked ([0-9,]+) \(", r"\) +re-picks ([0-9,]+)", r"\) +re-picks [0-9,]+ \(sold out ([0-9,]+)",
            r"\) +re-picks [0-9,]+ \(sold out [0-9,]+ over the deal price ([0-9,]+)", r"\) no seat ([0-9,]+)  a pool",
            r"would re-pick ([0-9,]+)", r"would re-pick [0-9,]+ \(sold out ([0-9,]+)", r"would re-pick [0-9,]+ \(sold out [0-9,]+ over the deal price ([0-9,]+)",
            r"would re-pick .*?\) no seat ([0-9,]+)", r"unpicked ([0-9,]+)")]
for name, lines in (("after warm-up", f), ("during the final hour", fin)):
    c, b, bg, bo, bn, s, sg, so, sn, un = tot(lines)
    print("%s (%d s): zone picks checked %d  re-picks %d (sold out %d, over the deal price %d, %.2f%%) no seat %d  unpicked %d | "
          "a pool refreshed every 3 s would re-pick %d (sold out %d, over the deal price %d, %.2f%%) and find no seat for %d (%.2f%%)" % (
          name, len(lines), c, b, bg, bo, 100 * b / max(c, 1), bn, un, s, sg, so, 100 * s / max(c, 1), sn, 100 * sn / max(c, 1)))
c, b, _, _, bn, s, _, _, sn, un = tot(f)
print("zone orders not placed because no Zone Deal was on offer at that moment (after warm-up): %d" % sum(nums(r"no deal shown ([0-9,]+)", f)))
pp = [l for l in f if "order-to-pick" in l]
print("order-to-pick, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"pick p50 ([0-9.]+)ms", pp)), st.median(nums(r"pick p50 [0-9.]+ms p99 ([0-9.]+)ms", pp))))
if c < 2000 or not fin:
    raise SystemExit("zone picks or the final hour did not flow")
if un * 50 > c:
    raise SystemExit("more than 2% of zone orders were never picked")
if b >= s:
    raise SystemExit("the live pool re-picked as many orders as the 3 s comparison pool")
PY
grep -q "\[app-events\] done" app-events.log || { echo "the app events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
