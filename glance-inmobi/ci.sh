#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Glance ad-state lane at small scale for about a minute: one
# beacon collector with its phones, three ad servers taking prefetch refills, a content interaction feed next door
# from second 20, and one ad server restarted mid-run. Then a 50 s what-if: the same three servers applying beacons
# 2 s late, as a micro-batch pipeline would.
# Pass only if each long-running ad server holds the 50 ms p99 budget in 95% of its steady-state seconds, no
# sequence gap anywhere, caps and dismissals are at work, stale placements stay under 0.1% of ads placed, and the
# restarted server rebuilds its view from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the shared runner that burst, 30-35 s after a
# heavy neighbour starts, stalls every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py beacons.py adserver.py interactions_flood.py kafka_bridge.py
# 800 phone syncs a second over 50,000 users, cap 3 per campaign per 20 s window (a compressed day).
W="--window-s 20"
timeout 80 python -u adserver.py --name ad-1 --pod 0 $W > ad-1.log 2>&1 &
timeout 79 python -u adserver.py --name ad-2 --pod 1 $W > ad-2.log 2>&1 &
sleep 2
timeout 76 python -u beacons.py --syncs 800 --users 50000 --ad-servers 3 $W --duration 74 > collector.log 2>&1 &
timeout 74 python -u interactions_flood.py --mbps 2 --at 20 --duration 35 > interactions.log 2>&1 &
# ad-3 runs, stops at 34 s and starts again: on restart it must rebuild its view from the log first.
timeout 34 python -u adserver.py --name ad-3 --pod 2 $W > ad-3.log 2>&1 || true
timeout 40 python -u adserver.py --name ad-3 --pod 2 $W > ad-3-restart.log 2>&1 || true
wait || true
# What-if: every server applies beacons only once they are 2 s old.
for n in 1 2 3; do
  timeout 52 python -u adserver.py --name hold-$n --pod $((n - 1)) $W --hold-ms 2000 > hold-$n.log 2>&1 &
done
sleep 2
timeout 50 python -u beacons.py --syncs 800 --users 50000 --ad-servers 3 $W --duration 48 > collector-hold.log 2>&1 || true
wait || true
for f in ad-1 ad-2 ad-3 ad-3-restart hold-1; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; tail -n 2 $f.log
done
echo "--- collector.log"; head -n 2 collector.log; tail -n 2 collector.log
echo "--- interactions.log"; head -n 2 interactions.log; tail -n 2 interactions.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" ad-1.log ad-2.log | cut -c1-170 || true
status=0
# Skip each server's first 10 s with live beacons (warm-up), then require 95% of the remaining seconds to pass.
for s in ad-1 ad-2; do
  steady=$(grep -E "^\[$s\] beacons +[1-9][0-9,]*/s" $s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$s steady-state seconds with live beacons: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
for f in ad-1 ad-2 ad-3 ad-3-restart; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "ad-state lane check failed"; exit 1; }
grep -Eq 'rebuilt the ad-state view from the log in [0-9.]+s: [1-9][0-9,]* users' ad-3-restart.log \
  || { echo "the restarted ad server did not rebuild its view from the log"; exit 1; }
grep -q "\[interactions\] done" interactions.log || { echo "the interaction feed did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"beacons +[1-9][0-9,]*/s"
def steady(s):
    return [l for l in open(f"{s}.log") if l.startswith(f"[{s}]") and re.search(live, l)]
for s in ("ad-1", "ad-2"):
    t = steady(s)[10:]
    if t:
        p99 = nums(r"p99 +([0-9.]+)ms", t)
        print("%s steady-state medians over %d s: beacons/s %.0f  refills/s %.0f  ads placed/s %.0f  beacon-to-ad-server "
              "p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms" % (
              s, len(t), st.median(nums(r"beacons +([0-9,]+)/s", t)), st.median(nums(r"refills ([0-9,]+)", t)),
              st.median(nums(r"ads placed ([0-9,]+)", t)), st.median(nums(r"p50 +([0-9.]+)ms", t)), st.median(p99),
              st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
rows = nums(r"p99 +([0-9.]+)ms", steady("ad-1"))
if len(rows) > 50:   # the interaction feed runs from about second 20 to 55 of the collector's run
    print("ad-1 p99, median per second: before the interaction feed %.1f ms (s 6-15), with 2 MB/s next door %.1f ms (s 20-48)" % (
        st.median(rows[6:16]), st.median(rows[20:49])))
def totals(f):
    m = None
    for l in open(f):
        m = re.search(r"this run ([0-9,]+) of ([0-9,]+) \(([0-9.]+)%\)", l) or m
    return tuple(float(g.replace(",", "")) for g in m.groups()) if m else (0.0, 0.0, 0.0)
def skipped(f):
    lines = open(f).readlines()
    return sum(nums(r"skipped at cap ([0-9,]+)", lines)), sum(nums(r"dismissed ([0-9,]+)  stale", lines))
bad = []
for s in ("ad-1", "ad-2"):
    stale, placed, pct = totals(f"{s}.log")
    cap, dis = skipped(f"{s}.log")
    print("%s: ads placed %d  skipped at cap %d  skipped for a dismissed advertiser %d  stale placements %d (%.3f%%)" % (
        s, placed, cap, dis, stale, pct))
    if placed < 5000 or cap == 0 or dis == 0:
        bad.append(f"{s}: too few placements, or caps and dismissals never applied")
    if pct >= 0.1:
        bad.append(f"{s}: stale placements reached 0.1%")
r = re.search(r"rebuilt the ad-state view from the log in ([0-9.]+)s: ([0-9,]+) users, ([0-9,]+) beacons", open("ad-3-restart.log").read())
if r:
    print("restarted ad server rebuilt its view in %s s: %s users, %s beacons" % r.groups())
for s in ("hold-1", "hold-2", "hold-3"):
    stale, placed, pct = totals(f"{s}.log")
    print("what-if, beacons applied 2 s late, %s: ads placed %d  stale placements %d (%.3f%%)" % (s, placed, stale, pct))
if bad:
    raise SystemExit("; ".join(bad))
PY
echo "Kit OK on Apache Iggy 0.9.0"
