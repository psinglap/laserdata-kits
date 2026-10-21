#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the frequency-cap lane at small scale for about a minute:
# three ad servers deciding on-demand slots all the time and a live ad break every 10 s (each live profile's
# pod split across the three servers), 0.5% of impressions delivered twice, ad playback beacons next door
# from second 20, and one ad server restarted mid-run. Then a 50 s what-if: the same three servers applying
# each other's impressions 2 s late, as a pipeline in retries would.
# Pass only if each long-running ad server holds the 50 ms p99 budget in 90% of its steady-state seconds,
# no sequence gap anywhere, repeats are dropped, caps are reached, over-cap impressions stay under 0.1% and
# live sponsor repeats under 0.1% of live impressions in every full window, and the restarted server
# rebuilds its cap view from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ad_server.py playback_beacons.py kafka_bridge.py
# Per server 400 on-demand slots a second over 60,000 profiles; 2,400 profiles watch the live event, a 3-slot
# break every 10 s; cap windows of 20 s (a compressed day).
ARGS="--servers 3 --rate 400 --profiles 60000 --live-profiles 2400 --live-every-s 10 --ramp-ms 1000 --window-s 20"
timeout 78 python -u ad_server.py --name ads-1 --index 0 $ARGS > ads-1.log 2>&1 &
sleep 1
timeout 77 python -u ad_server.py --name ads-2 --index 1 $ARGS > ads-2.log 2>&1 &
timeout 74 python -u playback_beacons.py --mbps 2 --at 20 --duration 35 > playback.log 2>&1 &
# ads-3 runs, stops at 34 s and starts again: on restart it must rebuild its cap view from the log first.
timeout 34 python -u ad_server.py --name ads-3 --index 2 $ARGS > ads-3.log 2>&1 || true
timeout 40 python -u ad_server.py --name ads-3 --index 2 $ARGS > ads-3-restart.log 2>&1 || true
wait || true
# What-if: every server applies the others' impressions only once they are 2 s old.
for n in 1 2 3; do
  timeout 50 python -u ad_server.py --name hold-$n --index $((n - 1)) $ARGS --hold-ms 2000 > hold-$n.log 2>&1 &
done
wait || true
for f in ads-1 ads-2 ads-3 ads-3-restart hold-1; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; grep "^\[cap\]" $f.log | tail -n 3 || true; tail -n 2 $f.log
done
echo "--- playback.log"; head -n 2 playback.log; tail -n 2 playback.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" ads-1.log ads-2.log | cut -c1-200 || true
status=0
# Skip each server's first 10 s (warm-up); a shared CI runner may blip, so require 90% of the remaining seconds
# with impressions from other servers to pass.
for s in ads-1 ads-2; do
  steady=$(grep -E "^\[$s\] .*from other servers +[1-9][0-9,]*/s" $s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$s steady-state seconds with live impressions: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 10 )) ]; }; then status=1; fi
done
for f in ads-1 ads-2 ads-3 ads-3-restart; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "frequency-cap lane check failed"; exit 1; }
grep -Eq 'rebuilt the cap view from the log in [0-9.]+s: [1-9][0-9,]* profiles' ads-3-restart.log \
  || { echo "the restarted ad server did not rebuild its cap view from the log"; exit 1; }
grep -q "\[playback\] done" playback.log || { echo "the playback-beacon neighbour did not run"; exit 1; }
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
for s in ("ads-1", "ads-2"):
    t = [l for l in open(f"{s}.log") if l.startswith(f"[{s}]") and re.search(live, l)][10:]
    b = [l for l in t if "(live break)" in l]
    if t:
        p99 = nums(r"p99 +([0-9.]+)ms", t)
        print("%s steady-state medians over %d s: from other servers/s %.0f  impression-to-cap-view p50 %.1f ms  "
              "p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms" % (
              s, len(t), st.median(nums(r"servers +([0-9,]+)/s", t)), st.median(nums(r"p50 +([0-9.]+)ms", t)),
              st.median(p99), st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
    if b:
        print("%s in %d live-break seconds: live slots/s %.0f  from other servers/s %.0f  p99 median %.1f ms, worst %.1f ms" % (
              s, len(b), st.median(nums(r"\+ +([0-9,]+) live", b)), st.median(nums(r"servers +([0-9,]+)/s", b)),
              st.median(nums(r"p99 +([0-9.]+)ms", b)), max(nums(r"p99 +([0-9.]+)ms", b))))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("ads-1.log") if l.startswith("[ads-1]") and re.search(live, l)])
if len(rows) > 55:   # the playback beacons run from about second 20 to 55
    print("ads-1 p99, median per second: before the playback beacons %.1f ms (s 10-19), with 2 MB/s next door %.1f ms (s 24-52)" % (
        st.median(rows[10:20]), st.median(rows[24:53])))
rep = nums(r"repeats dropped ([0-9,]+)", open("ads-1.log").readlines())
print("ads-1 repeats dropped by event ID: %d" % (rep[-1] if rep else 0))
if not rep or rep[-1] == 0:
    raise SystemExit("no repeated impression was dropped: de-duplication did not run")
PAT = (r"impressions ([0-9,]+) \(live ([0-9,]+)\)  profile-campaign pairs at cap ([0-9,]+)  over-cap impressions "
       r"([0-9,]+) \(([0-9.]+)%\)  live sponsor repeats ([0-9,]+) \(([0-9.]+)% of live\)")
def windows(f):
    out = []
    for l in open(f):
        m = re.search(PAT, l)
        if m:
            out.append(tuple(float(g.replace(",", "")) for g in m.groups()))
    return out
w = windows("ads-1.log")[1:]                # the first window was partly before the servers started
for n, ln, cap, o, pct, lo, lpct in w:
    print("window: impressions %d (live %d)  profile-campaign pairs at cap %d  over-cap impressions %d (%.3f%%)  "
          "live sponsor repeats %d (%.3f%% of live)" % (n, ln, cap, o, pct, lo, lpct))
if len(w) < 2 or any(cap == 0 or ln == 0 for _, ln, cap, *_ in w):
    raise SystemExit("caps were not reached, or no live break ran, in enough full windows")
if any(pct >= 0.1 or lpct >= 0.1 for _, _, _, _, pct, _, lpct in w):
    raise SystemExit("over-cap impressions or live sponsor repeats reached 0.1% in a window")
r = re.search(r"rebuilt the cap view from the log in ([0-9.]+)s: ([0-9,]+) profiles, ([0-9,]+) impressions", open("ads-3-restart.log").read())
if r:
    print("restarted ad server rebuilt its cap view in %s s: %s profiles, %s impressions" % r.groups())
h = windows("hold-1.log")[1:]
for n, ln, cap, o, pct, lo, lpct in h:
    print("what-if, impressions applied 2 s late: impressions %d  over-cap impressions %d (%.3f%%)  "
          "live sponsor repeats %d (%.3f%% of live)" % (n, o, pct, lo, lpct))
PY
echo "Kit OK on Apache Iggy 0.9.0"
