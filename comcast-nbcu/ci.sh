#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the stream-limit lane at small scale for about a minute:
# three playback nodes sharing 3,000 accounts in 20 s live-event blocks (40% of accounts tuning in within a
# second of kickoff), households on three streams switching a device, fourth devices trying to start,
# heartbeats every 5 s, 0.5% of session events delivered twice, player QoE beacons next door from second 20,
# and one playback node restarted mid-run. Then a 50 s what-if: the same three nodes applying each other's
# events 2 s late, as a consumer stuck in a rebalance would.
# Pass only if each long-running node holds the 50 ms p99 budget in 95% of its steady-state seconds, no
# sequence gap anywhere, repeats are dropped, in every judged block fourth devices are refused, switches are
# let in, and viewers wrongly blocked plus streams over the limit stay under 0.1% of start decisions, and the
# restarted node rebuilds its view from the log.
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
python -m py_compile common.py stream_limit.py qoe_beacons.py kafka_bridge.py
ARGS="--nodes 3 --accounts 3000 --cycle-s 20 --ramp-ms 1000 --hb-s 5"
timeout 78 python -u stream_limit.py --name pb-1 --index 0 $ARGS > pb-1.log 2>&1 &
sleep 1
timeout 77 python -u stream_limit.py --name pb-2 --index 1 $ARGS > pb-2.log 2>&1 &
timeout 74 python -u qoe_beacons.py --mbps 2 --at 20 --duration 35 > qoe.log 2>&1 &
# pb-3 runs, stops at 34 s and starts again: on restart it must rebuild its view from the log first.
timeout 34 python -u stream_limit.py --name pb-3 --index 2 $ARGS > pb-3.log 2>&1 || true
timeout 40 python -u stream_limit.py --name pb-3 --index 2 $ARGS > pb-3-restart.log 2>&1 || true
wait || true
# What-if: every node applies the others' events only once they are 2 s old.
for n in 1 2 3; do
  timeout 50 python -u stream_limit.py --name hold-$n --index $((n - 1)) $ARGS --hold-ms 2000 > hold-$n.log 2>&1 &
done
wait || true
for f in pb-1 pb-2 pb-3 pb-3-restart hold-1; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; grep "^\[limit\]" $f.log | tail -n 3 || true; tail -n 2 $f.log
done
echo "--- qoe.log"; head -n 2 qoe.log; tail -n 2 qoe.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" pb-1.log pb-2.log | cut -c1-200 || true
status=0
# Skip each node's first 10 s (warm-up); a shared CI runner may blip, so require 95% of the remaining seconds
# with events from other nodes to pass.
for s in pb-1 pb-2; do
  steady=$(grep -E "^\[$s\] .*from other nodes +[1-9][0-9,]*/s" $s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$s steady-state seconds with live session events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
for f in pb-1 pb-2 pb-3 pb-3-restart; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "stream-limit lane check failed"; exit 1; }
grep -Eq 'rebuilt the stream-limit view from the log in [0-9.]+s: [1-9][0-9,]* active streams' pb-3-restart.log \
  || { echo "the restarted playback node did not rebuild its view from the log"; exit 1; }
grep -q "\[qoe\] done" qoe.log || { echo "the QoE beacon neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"from other nodes +[1-9][0-9,]*/s"
for s in ("pb-1", "pb-2"):
    t = [l for l in open(f"{s}.log") if l.startswith(f"[{s}]") and re.search(live, l)][10:]
    if t:
        p99 = nums(r"p99 +([0-9.]+)ms", t)
        print("%s steady-state medians over %d s: from other nodes/s %.0f  session-event-to-view p50 %.1f ms  "
              "p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms  peak starts/s %.0f" % (
              s, len(t), st.median(nums(r"nodes +([0-9,]+)/s", t)), st.median(nums(r"p50 +([0-9.]+)ms", t)),
              st.median(p99), st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99), max(nums(r"starts +([0-9,]+)/s", t))))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("pb-1.log") if l.startswith("[pb-1]") and re.search(live, l)])
if len(rows) > 55:   # the QoE beacons run from about second 20 to 55
    print("pb-1 p99, median per second: before the QoE beacons %.1f ms (s 10-19), with 2 MB/s next door %.1f ms (s 24-52)" % (
        st.median(rows[10:20]), st.median(rows[24:53])))
rep = nums(r"repeats dropped ([0-9,]+)", open("pb-1.log").readlines())
print("pb-1 repeats dropped by event ID: %d" % (rep[-1] if rep else 0))
if not rep or rep[-1] == 0:
    raise SystemExit("no repeated session event was dropped: de-duplication did not run")
PAT = (r"start decisions ([0-9,]+)  granted ([0-9,]+)  refused ([0-9,]+) \(fourth devices refused ([0-9,]+)\)  "
       r"device switches let in ([0-9,]+)  viewers wrongly blocked ([0-9,]+) \([0-9.]+%, switches ([0-9,]+)\)  "
       r"streams over the limit ([0-9,]+) \([0-9.]+%\)  released by lease ([0-9,]+)")
def blocks(f):
    out = []
    for l in open(f):
        m = re.search(PAT, l)
        if m:
            out.append(tuple(int(g.replace(",", "")) for g in m.groups()))
    return out
bad = False
for s in ("pb-1", "pb-2"):
    b = blocks(f"{s}.log")[1:]              # the first block was partly before the node started
    for n, g, r, f4, sw, wb, wsw, over, rel in b:
        print("%s block: start decisions %d  granted %d  refused %d (fourth devices %d)  switches let in %d  "
              "viewers wrongly blocked %d  streams over the limit %d  released by lease %d" % (s, n, g, r, f4, sw, wb, over, rel))
    if len(b) < 2 or any(f4 == 0 or sw == 0 or (wb + over) * 1000 >= n for n, g, r, f4, sw, wb, wsw, over, rel in b):
        bad = True
r = re.search(r"rebuilt the stream-limit view from the log in ([0-9.]+)s: ([0-9,]+) active streams on ([0-9,]+) accounts, "
              r"([0-9,]+) session events", open("pb-3-restart.log").read())
if r:
    print("restarted playback node rebuilt its view in %s s: %s active streams on %s accounts, from %s session events" % r.groups())
for s in ("hold-1", "hold-2", "hold-3"):
    for n, g, r_, f4, sw, wb, wsw, over, rel in blocks(f"{s}.log")[1:]:
        print("what-if, events applied 2 s late, %s: start decisions %d  viewers wrongly blocked %d (%.1f%%, %d of them "
              "device switches)  streams over the limit %d (%.1f%%)  fourth devices refused %d" % (
              s, n, wb, 100 * wb / n if n else 0, wsw, over, 100 * over / n if n else 0, f4))
if bad:
    raise SystemExit("fewer than two judged blocks, no fourth device refused or switch let in, or wrong decisions reached 0.1%")
PY
echo "Kit OK on Apache Iggy 0.9.0"
