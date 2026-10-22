#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the live ad break loop at small scale for about a minute, with
# full-census heartbeats next to it and one ad server failing in one market, and pass only if the ad break lane
# reports PASS, the checker raises an alert for the failing cohort and no other, and the breaks that lost a slot to
# the fault were flagged at the ad ops console while they were still running.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst, about 30 s after a
# heavy neighbour starts, stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ad_break_checker.py live_match.py heartbeat_flood.py kafka_bridge.py
# The checker starts first and creates the topics; the live channels and the neighbour follow.
# 12,000 sessions on 12 channels, a break of 3 x 15 s slots every 55 s per channel (about 1,700 ad events/s, in
# bursts as each channel's break and slots begin); ad-server-b stops answering in dallas from second 20 for 30 s;
# 2 MB/s of heartbeats from second 15.
timeout 78 python -u ad_break_checker.py > checker.log 2>&1 &
sleep 3
timeout 72 python -u live_match.py --sessions 12000 --channels 12 --slots 3 --slot-s 15 --cycle-s 55 \
  --fault-at 20 --fault-for 30 --consumers 3 > match.log 2>&1 &
timeout 70 python -u heartbeat_flood.py --mbps 2 --at 15 --duration 40 > heartbeats.log 2>&1 &
wait || true
echo "--- match.log";      head -n 2 match.log; grep -E "ALERT|CLEAR" match.log || true; tail -n 3 match.log
echo "--- checker.log";    tail -n 4 checker.log
echo "--- heartbeats.log"; head -n 2 heartbeats.log; tail -n 2 heartbeats.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" checker.log | cut -c1-400 || true
echo "--- match stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" match.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[ad-breaks\] +[1-9][0-9,]* events/s' checker.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the alert checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
r = [l for l in open("checker.log") if re.search(r"\[ad-breaks\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  event-to-checker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("checker.log") if "event-to-checker" in l])
if len(lane) > 50:
    print("event-to-checker p99, median per second: before the neighbour %.1f ms (s 6-14), with 2 MB/s of heartbeats %.1f ms (s 20-50)" % (
        st.median(lane[6:15]), st.median(lane[20:51])))
att = sum(nums(r"ad attempts ([0-9,]+)", r))
unf = sum(nums(r"unfilled ([0-9,]+) \(", r))
print("ad attempts in steady state: %d  unfilled: %d (%.1f%%)" % (att, unf, 100 * unf / max(att, 1)))
if att < 1000:
    raise SystemExit("too few ad attempts to judge the lane")
m = open("match.log").read()
a = re.search(r"ALERT ad-server-b x dallas at the ad ops console ([0-9,]+) ms after the fault began.*?checker-to-console ([0-9.]+) ms", m)
if not a:
    raise SystemExit("no alert for the failing cohort reached the ad ops console")
print("failing cohort flagged at the ad ops console %s ms after the fault began (%s ms after the event that tipped it)" % a.groups())
if float(a.group(1).replace(",", "")) > 10_000:
    raise SystemExit("the failing cohort was flagged more than 10 s after the fault began")
f = [l for l in open("match.log") if re.search(r"\[live-match\] +[0-9,]+ events/s", l)]
other = sum(nums(r"other cohorts ([0-9,]+)", f))
ended = sum(nums(r"lost a slot, ended ([0-9,]+):", f))
live = sum(nums(r"still running ([0-9,]+) ", f))
print("failing-cohort breaks that lost a slot to the fault: %d  flagged while still running: %d (%.2f%%)" % (
    ended, live, 100 * live / max(ended, 1)))
req = sum(nums(r"failing-cohort ad requests ([0-9,]+),", f))
ahead = sum(nums(r"had the alert ([0-9,]+) \(", f))
ahead_c = sum(nums(r"had the alert [0-9,]+ \(at [0-9.]+ s: ([0-9,]+)\)", f))
print("failing-cohort ad requests during the fault: %d  after the console had the alert: %d (%.1f%%)  at a 30 s slower path: %d (%.1f%%)" % (
    req, ahead, 100 * ahead / max(req, 1), ahead_c, 100 * ahead_c / max(req, 1)))
print("alerts for healthy cohorts: %d" % other)
if other:
    raise SystemExit("a healthy cohort was alerted")
if ended < 20:
    raise SystemExit("too few failing-cohort breaks to judge the loop")
if live * 100 < ended * 95:
    raise SystemExit("more than 5% of failing-cohort breaks were flagged after they had ended")
d = [l for l in f if re.search(r"event-to-consumers n [1-9]", l)]
if d:
    print("event-to-consumers (ad event to 3 consumers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
        st.median(nums(r"consumers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
e = [l for l in open("heartbeats.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("heartbeat neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[heartbeats\] done" heartbeats.log || { echo "the heartbeat neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
