#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Dailyhunt floor lane at small scale for about a minute
# and a half: two bid nodes, breaking news on one language with a 4x ad-event burst next door, and a third
# bid node added as the breaking news starts, as an autoscaler would.
# Pass only if each node holds the 20 ms p99 budget in 95% of its steady-state seconds with no sequence gaps,
# floor steps reach the nodes and are counted as stale-floor auctions, and the added node rebuilds every
# placement's floor from the log.
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
python -m py_compile common.py floors.py bidnode.py ad_events.py pubsub_bridge.py
# 15 languages x 800 ad slots = 12,000 placements; 2,000 floor changes a second, 50 of them steps;
# breaking news on Hindi from second 40 to 55 (4x its floor changes, steps mostly up);
# 1 MB/s of ad events next door, 4x over the same 15 s.
L=15; P=800; A=20000
timeout 92 python -u floors.py --langs $L --placements-per-lang $P --rate 2000 --steps 50 \
  --spike-lang hi --spike-at 40 --spike-for 15 --spike-x 4 --snapshot-every 15 > floors.log 2>&1 &
sleep 4
timeout 80 python -u ad_events.py --mbps 1 --x 4 --at 36 --for 15 --duration 74 > ad-events.log 2>&1 &
timeout 82 python -u bidnode.py --name bidnode-1 --langs $L --placements-per-lang $P --auctions-per-s $A --replay-s 0 > bidnode-1.log 2>&1 &
timeout 34 python -u bidnode.py --name bidnode-2 --langs $L --placements-per-lang $P --auctions-per-s $A --replay-s 0 > bidnode-2.log 2>&1 || true
sleep 1
# bidnode-3 is added as breaking news starts: it must rebuild every floor from the last 20 s of the log
# (one 15 s rolling snapshot plus the changes since) while the spike is running.
timeout 46 python -u bidnode.py --name bidnode-3 --langs $L --placements-per-lang $P --auctions-per-s $A --replay-s 20 > bidnode-3.log 2>&1 || true
wait || true
echo "--- floors.log";    grep -m 1 snapshot floors.log || true; grep -m 1 "breaking news" floors.log || true; tail -n 3 floors.log
echo "--- ad-events.log"; head -n 1 ad-events.log; grep -m 1 -E 'records/s .*burst' ad-events.log || true; tail -n 2 ad-events.log
for f in bidnode-1 bidnode-2 bidnode-3; do
  echo "--- $f.log"; grep -E "rebuilt|without the log" $f.log || true; tail -n 4 $f.log | cut -c1-300
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" bidnode-*.log | cut -c1-160 || true
status=0
# Skip each node's first 10 live seconds (warm-up, or the replay right after it was added); a shared CI runner
# may blip, so require 95% of each node's remaining seconds to pass, and no sequence gap anywhere.
for node in bidnode-1:40 bidnode-2:18 bidnode-3:28; do
  name=${node%%:*}; need=${node##*:}
  s=$(grep -E "\[$name\] +[1-9][0-9,]* floor changes/s" $name.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$name steady-state seconds with live floor changes: PASS $live, FAIL $fail (need $need live, 95% PASS)"
  if [ "$live" -lt "$need" ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
  gaps=$(grep -Eo 'gaps [0-9]+' $name.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$name sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "floor lane check failed"; exit 1; }
grep -Eq 'rebuilt from the last 20 s of the log in [0-9.]+s: floors for ([0-9,]+) of \1 placements' bidnode-3.log \
  || { echo "the added node did not rebuild every placement's floor"; exit 1; }
if ! grep -Eq 'steps [1-9][0-9,]*  stale-floor auctions' bidnode-1.log; then echo "no floor steps reached the bid node"; exit 1; fi
grep -q "breaking news: hi" floors.log || { echo "the breaking-news spike did not run"; exit 1; }
grep -q "\[ad-events\] done" ad-events.log || { echo "the ad-event neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[bidnode-[123]\] +([0-9,]+) floor changes/s"
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("bidnode-1", "bidnode-3"):
    t = [l for l in open(f"{name}.log") if re.search(live, l) and not re.search(r"\] +0 floor changes/s", l)][10:]
    if not t:
        continue
    p99 = nums(r"p99 +([0-9.]+)ms", t)
    med = lambda pat: st.median(nums(pat, t) or [float("nan")])
    print("%s steady-state medians over %d s: floor changes/s %.0f  floor-to-node p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms  steps/s %.0f  stale-floor auctions/s %.3f (%.3f per 1M)  Rs given away/s %.4f  "
          "at a 1 s refresh %.0f/s, Rs %.2f/s" % (
          name, len(t), med(live), med(r"p50 +([0-9.]+)ms"), med(r"p99 +([0-9.]+)ms"), med(r"p99.9 +([0-9.]+)ms"),
          max(p99 or [float("nan")]), med(r"steps ([0-9,]+)"), med(r"stale-floor auctions ([0-9,.]+)/s"),
          med(r"\(([0-9.]+) per 1M\)"), med(r"Rs ([0-9,.]+) given away"), med(r"at a 1 s refresh ([0-9,]+)/s"),
          med(r"at a 1 s refresh [0-9,]+/s, Rs ([0-9,.]+)")))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("bidnode-1.log") if re.search(live, l)])
if len(rows) > 56:   # breaking news and the ad-event burst run from floors.py's second 40 to 55, about bidnode-1's 36 to 51
    print("bidnode-1 p99, median per second: before breaking news %.2f ms (s 15-33), during it %.2f ms (s 38-50)" % (
        st.median(rows[15:34]), st.median(rows[38:51])))
for f in ("bidnode-3.log", "bidnode-1.log"):
    for l in open(f):
        if "rebuilt" in l or "without the log" in l:
            print(l.strip())
fl = [l for l in open("floors.log") if "floor changes/s" in l]
spike, calm = nums(r"\] +([0-9,]+) floor changes/s", [l for l in fl if "breaking news" in l]), \
              nums(r"\] +([0-9,]+) floor changes/s", [l for l in fl if "breaking news" not in l][2:])
if spike and calm:
    print("floors.py: median %.0f floor changes/s, %.0f during breaking news (%d s)" % (st.median(calm), st.median(spike), len(spike)))
e = nums(r"~([0-9.]+) MB/s", [l for l in open("ad-events.log") if re.search(r"\] +[1-9][0-9,]* records/s", l)])
if e:
    print("ad-event neighbour: %d s, median %.1f MB/s, peak %.1f MB/s on its own topic" % (len(e), st.median(e), max(e)))
PY
echo "Kit OK on Apache Iggy 0.9.0"
