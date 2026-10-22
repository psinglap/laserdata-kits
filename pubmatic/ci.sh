#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the household frequency lane at small scale for about a
# minute: three CTV ad servers, a log-level feed next door from second 20, and one ad server restarted
# mid-run. Then a 50 s what-if: the same three servers applying each other's impressions 2 s late, as a
# micro-batch pipeline would.
# Pass only if each long-running ad server holds the 50 ms p99 budget in 90% of its steady-state seconds,
# no sequence gap anywhere, caps are reached and over-cap impressions stay under 0.1% in every full window,
# and the restarted server rebuilds its cap view from the log.
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
python -m py_compile common.py adserver.py loglevel_flood.py kafka_bridge.py
# 1,500 ad slots a second per server over 50,000 households, cap 3 per campaign per 15 s window (a compressed day).
ARGS="--rate 1500 --households 50000 --window-s 15"
timeout 78 python -u adserver.py --name ad-1 $ARGS > ad-1.log 2>&1 &
sleep 1
timeout 77 python -u adserver.py --name ad-2 $ARGS > ad-2.log 2>&1 &
timeout 74 python -u loglevel_flood.py --mbps 2 --at 20 --duration 35 > loglevel.log 2>&1 &
# ad-3 runs, stops at 34 s and starts again: on restart it must rebuild its cap view from the log first.
timeout 34 python -u adserver.py --name ad-3 $ARGS > ad-3.log 2>&1 || true
timeout 40 python -u adserver.py --name ad-3 $ARGS > ad-3-restart.log 2>&1 || true
wait || true
# What-if: every server applies the others' impressions only once they are 2 s old.
for n in 1 2 3; do
  timeout 50 python -u adserver.py --name hold-$n $ARGS --hold-ms 2000 > hold-$n.log 2>&1 &
done
wait || true
for f in ad-1 ad-2 ad-3 ad-3-restart hold-1; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; grep "^\[cap\]" $f.log | tail -n 3 || true; tail -n 2 $f.log
done
echo "--- loglevel.log"; head -n 2 loglevel.log; tail -n 2 loglevel.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" ad-1.log ad-2.log | cut -c1-170 || true
status=0
# Skip each server's first 10 s (warm-up); a shared CI runner may blip, so require 90% of the remaining seconds
# with impressions from other servers to pass (as zerodha does). Most runs have no FAIL second at all; the rare
# failing run had 3-4 FAIL seconds that hit ad-1 and ad-2 in the same wall-clock seconds while p50 stayed near
# 8 ms: a stall of the whole runner, not of one server's reader.
for s in ad-1 ad-2; do
  steady=$(grep -E "^\[$s\] .*from other servers +[1-9][0-9,]*/s" $s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$s steady-state seconds with live impressions: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 10 )) ]; }; then status=1; fi
done
for f in ad-1 ad-2 ad-3 ad-3-restart; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "household lane check failed"; exit 1; }
grep -Eq 'rebuilt the cap view from the log in [0-9.]+s: [1-9][0-9,]* households' ad-3-restart.log \
  || { echo "the restarted ad server did not rebuild its cap view from the log"; exit 1; }
grep -q "\[log-level\] done" loglevel.log || { echo "the log-level neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"from other servers +[1-9][0-9,]*/s"
for s in ("ad-1", "ad-2"):
    t = [l for l in open(f"{s}.log") if l.startswith(f"[{s}]") and re.search(live, l)][10:]
    if t:
        p99 = nums(r"p99 +([0-9.]+)ms", t)
        print("%s steady-state medians over %d s: slots filled/s %.0f  from other servers/s %.0f  exposure-to-ad-server "
              "p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms" % (
              s, len(t), st.median(nums(r"\] +([0-9,]+) slots", t)), st.median(nums(r"servers +([0-9,]+)/s", t)),
              st.median(nums(r"p50 +([0-9.]+)ms", t)), st.median(p99), st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("ad-1.log") if l.startswith("[ad-1]") and re.search(live, l)])
if len(rows) > 55:   # the log-level feed runs from about second 20 to 55
    print("ad-1 p99, median per second: before the log-level feed %.1f ms (s 10-19), with 2 MB/s next door %.1f ms (s 24-52)" % (
        st.median(rows[10:20]), st.median(rows[24:53])))
def windows(f):
    out = []
    for l in open(f):
        m = re.search(r"impressions ([0-9,]+)  household-campaign pairs at cap ([0-9,]+)  over-cap impressions ([0-9,]+) \(([0-9.]+)%\)", l)
        if m:
            out.append(tuple(float(g.replace(",", "")) for g in m.groups()))
    return out
w = windows("ad-1.log")[1:]                 # the first window was partly before the servers started
for n, cap, o, pct in w:
    print("window: impressions %d  household-campaign pairs at cap %d  over-cap impressions %d (%.3f%%)" % (n, cap, o, pct))
if len(w) < 2 or any(cap == 0 for _, cap, _, _ in w):
    raise SystemExit("caps were not reached in enough full windows")
if any(pct >= 0.1 for _, _, _, pct in w):
    raise SystemExit("over-cap impressions reached 0.1% in a window")
r = re.search(r"rebuilt the cap view from the log in ([0-9.]+)s: ([0-9,]+) households, ([0-9,]+) impressions", open("ad-3-restart.log").read())
if r:
    print("restarted ad server rebuilt its cap view in %s s: %s households, %s impressions" % r.groups())
h = windows("hold-1.log")[1:]
for n, cap, o, pct in h:
    print("what-if, impressions applied 2 s late: impressions %d  over-cap impressions %d (%.3f%%)" % (n, o, pct))
PY
echo "Kit OK on Apache Iggy 0.9.0"
