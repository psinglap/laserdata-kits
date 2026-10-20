#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the cargo alert loop at small scale for about a minute, with
# gateway backfill uploads next to it, and pass only if the cargo sensor lane reports PASS, the alert rules send
# alerts, and door alerts reach the driver app while the door is still open.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py alert_rules.py fleet.py backfill_flood.py kafka_bridge.py
# The alert rules start first and create the topics; the fleet and the neighbour follow.
# 120,000 assets, a reading a minute each and a door opening every 30 min per asset (about 2,100 events/s);
# 2 MB/s of backfill uploads from second 20.
timeout 78 python -u alert_rules.py > rules.log 2>&1 &
sleep 3
timeout 72 python -u fleet.py --assets 120000 --door-every-min 30 --notifiers 3 > fleet.log 2>&1 &
timeout 70 python -u backfill_flood.py --mbps 2 --at 20 --duration 35 > backfill.log 2>&1 &
wait || true
echo "--- fleet.log";    head -n 3 fleet.log; tail -n 4 fleet.log
echo "--- rules.log";    tail -n 6 rules.log
echo "--- backfill.log"; head -n 2 backfill.log; tail -n 2 backfill.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" rules.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[alert-rules\] +[1-9][0-9,]* events/s' rules.log | tail -n +11 || true)
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
r = [l for l in open("rules.log") if re.search(r"\[alert-rules\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  event-to-rules p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("rules.log") if "event-to-rules" in l])
if len(lane) > 50:
    print("event-to-rules p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of backfill %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
sent = sum(nums(r"alerts sent ([0-9,]+)", r))
temp = sum(nums(r"temperature ([0-9,]+),", r))
cause = sum(nums(r"cause named ([0-9,]+)\)", r))
print("alerts sent in steady state: %d  temperature alerts: %d, with a door event named as the likely cause: %d" % (sent, temp, cause))
if sent < 100:
    raise SystemExit("the alert rules did not send alerts")
if temp < 1 or cause < 1:
    raise SystemExit("no temperature alerts, or none linked to a door event")
f = [l for l in open("fleet.log") if re.search(r"\[fleet\] +[1-9][0-9,]* events/s", l)][10:]
alerts = sum(nums(r"at the driver app ([0-9,]+),", f))
open_ = sum(nums(r"still open ([0-9,]+) \(", f))
slow = sum(nums(r"it would be ([0-9,]+)\)", f))
print("door-outside-geofence alerts at the driver app: %d  while the door was still open: %d (%.2f%%)  at a 60 s lane: %d (%.2f%%)" % (
    alerts, open_, 100 * open_ / max(alerts, 1), slow, 100 * slow / max(alerts, 1)))
if alerts < 50:
    raise SystemExit("too few door alerts to judge the loop")
d = [l for l in f if re.search(r"event-to-notifiers n [1-9]", l)]
if not d:
    raise SystemExit("no alerts reached the notifiers")
print("event-to-notifiers (gateway event to 3 notifiers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
    st.median(nums(r"notifiers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if open_ * 100 < alerts * 99:
    raise SystemExit("more than 1% of door alerts reached the driver app after the door had closed")
e = [l for l in open("backfill.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("backfill neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[backfill\] done" backfill.log || { echo "the backfill neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
