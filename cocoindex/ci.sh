#!/usr/bin/env bash
# CI smoke test: Apache Iggy 0.9.0 and CocoIndex 1.0.26 (its own Iggy connector) into an embedded LanceDB table.
# The review service writes thread changes into Iggy for about 85 s (with a 2x spike), and the CocoIndex app keeps
# the review_context table current. The app is killed with SIGKILL mid-run and restarted 5 s later. Pass only if
# change-to-index freshness stays inside 1 s in 95% of steady-state seconds, the crash happened and the restarted app
# replayed the changes held in Iggy, agent lookups were measured, and at the end every thread in LanceDB matches the
# topic (no missing, stale, resolved-but-still-indexed or extra row).
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 30 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which stalled every reader on the runner for
# 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py review_events.py index_app.py context_check.py kafka_shadow.py
rm -rf lancedb_data cocoindex_state .index-heartbeat

# 5,000 open review threads, 250 changes/s for 85 s, 2x for 8 s from 20 s.
timeout 100 python -u review_events.py --threads 5000 --rate 250 --duration 85 --spike-x 2 --spike-at 20 --spike-for 8 \
  > review.log 2>&1 &
REVIEW=$!
sleep 2
python -u index_app.py > index.log 2>&1 &
INDEX=$!
timeout 190 python -u context_check.py --duration 88 --session-s 30 --wait-s 60 > check.log 2>&1 &
CHECK=$!
sleep 45
echo "--- $(date +%T) killing the CocoIndex app with SIGKILL; restarting it in 5 s"
kill -9 "$INDEX" || true
sleep 5
python -u index_app.py >> index.log 2>&1 &
INDEX=$!
wait "$REVIEW" || true
wait "$CHECK" || true
kill "$INDEX" || true

echo "--- review.log"; head -n 2 review.log; tail -n 2 review.log
echo "--- index.log"; grep -v "changes/s through" index.log | grep -v WARN | tail -n 8 || true
echo "--- check.log (catching up, the last lines)"
grep "catching up" check.log | head -n 3 || true; grep "catching up" check.log | tail -n 3 || true
grep -v "catching up" check.log | tail -n 6
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" check.log | cut -c1-200 || true
# Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of seconds to pass. Seconds while the app is down
# or catching up are reported separately and not counted.
steady=$(grep -E '\[check\] +[1-9][0-9,]* changes/s indexed' check.log | tail -n +11 || true)
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
log = open("check.log").read().splitlines()
s = [l for l in log if re.search(r"\[check\] +[1-9][0-9,]* changes/s indexed", l)][10:]
print("steady-state medians: changes/s %.0f  change-to-index p50 %.0f ms  p99 %.0f ms  p99.9 %.0f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99\.9 +([0-9.]+)ms")))
print("steady-state p99, worst second %.0f ms over %d seconds" % (max(nums(r"p99 +([0-9.]+)ms", s)), len(s)))
live = nums(r"stale answers ([0-9.]+)%", s)
snap = nums(r"snapshot from [0-9]+s ago: ([0-9.]+)%", s)
print("agent lookups, steady state: stale answers from the live index median %.1f%% (max %.1f%%); from a snapshot "
      "taken at session start median %.1f%%" % (st.median(live), max(live), st.median(snap)))
down = [l for l in log if "indexer down" in l]
held = nums(r"([0-9,]+) held in Iggy", [l for l in log if "catching up" in l])
catch = [l for l in log if "catching up" in l]
print("indexer down for %d s; up to %d changes held in Iggy; %d catching-up seconds in all (start-up and restart)" % (
    len(down), max(held or [0]), len(catch)))
idx = open("index.log").read().splitlines()
res = [l for l in idx if "resumed at offset" in l]
for l in res:
    print(l)
if not down or len(res) < 2 or not max(held or [0]):
    raise SystemExit("the crash was not seen, or the restarted app did not replay what Iggy held")
if not live or not snap:
    raise SystemExit("agent lookups were not measured")
PY
grep "\[verify\]" check.log
grep -q "\[verify\] OK" check.log || { echo "index does not match the topic"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0 and CocoIndex 1.0.26"
