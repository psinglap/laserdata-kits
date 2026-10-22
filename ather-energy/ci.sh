#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the rider alert lane at small scale for about a minute, with a
# reconnect storm in the middle and full sensor packets next door, and pass only if the lane reports PASS through
# it, every fresh alert reaches the app once and on time, no stale event is pushed, and ride history files the
# whole reconnect backlog.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst: on the shared runner that burst stalls every
# partition at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py alert_service.py ride_history.py scooters.py telemetry_flood.py kafka_bridge.py
# The alert service and ride history start first and create the topics; the fleet and the neighbour follow.
# 60,000 scooters, an event every 30 s each (about 2,000 events/s), 1% resent; 5% of the fleet offline for
# 6 minutes reconnects at 30 s over 8 s; 2 MB/s of sensor packets from 15 s, 4 MB/s during the reconnect.
# vmstat records the runner's CPU and disk each second, so a slow second can be traced to the host.
vmstat -n 1 80 > vmstat.log 2>&1 &
timeout 80 python -u alert_service.py > service.log 2>&1 &
timeout 80 python -u ride_history.py > history.log 2>&1 &
sleep 3
timeout 72 python -u scooters.py --scooters 60000 --event-s 30 --storm-at 30 --storm-share 0.05 --offline-min 6 \
  --flush-s 8 > scooters.log 2>&1 &
timeout 70 python -u telemetry_flood.py --mbps 2 --at 15 --duration 45 --burst-at 15 --burst-for 10 --burst-x 2 \
  > telemetry.log 2>&1 &
wait || true
echo "--- scooters.log";  head -n 2 scooters.log; grep "reconnect storm done" scooters.log || true; tail -n 3 scooters.log
echo "--- service.log";   sed -n "20,21p" service.log | cut -c1-420; tail -n 2 service.log | cut -c1-200; echo "service log lines: $(wc -l < service.log)"
echo "--- history.log";   grep -m 3 -E "filed [1-9]" history.log || true; tail -n 2 history.log
echo "--- telemetry.log"; head -n 2 telemetry.log; grep -m 2 "burst" telemetry.log || true; tail -n 1 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" service.log | cut -c1-400 || true
echo "--- runner each second, from the alert service's start (vmstat: r runnable, b blocked, us sy id wa CPU %)"; cat -n vmstat.log | cut -c1-110 || true
echo "--- scooters stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" scooters.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[alert-service\] +[1-9][0-9,]* events/s' service.log | tail -n +11 || true)
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
s = [l for l in open("service.log") if re.search(r"\[alert-service\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  event-to-service p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
sc = [l for l in open("scooters.log") if re.search(r"\[scooters\] +[1-9][0-9,]* events/s", l)]
storm = [i for i, l in enumerate(sc) if "(reconnect)" in l]
lane = [l for l in open("service.log") if "event-to-service" in l]
if storm and len(lane) > storm[-1] + 2:
    # the service starts 3 s before the fleet; its second-by-second lines line up with the fleet's within a second
    before = nums(r"p99 +([0-9.]+)ms", lane[10:storm[0]])
    during = nums(r"p99 +([0-9.]+)ms", lane[storm[0]:storm[-1] + 3])
    print("event-to-service p99, median per second: before the reconnect storm %.1f ms (%d s), during it %.1f ms (%d s), worst second during it %.1f ms" % (
        st.median(before), len(before), st.median(during), len(during), max(during)))
repeats = sum(nums(r"repeats dropped ([0-9,]+)", s))
filed = sum(nums(r"late events filed, not pushed ([0-9,]+)", s))
delayed = sum(nums(r"delayed ([0-9,]+)\)", s))
print("alert service in steady state: alerts sent %d, resent events dropped %d, late events that reached the lane and were filed %d, late crash alerts sent as delayed %d" % (
    sum(nums(r"alerts sent ([0-9,]+)", s)), repeats, filed, delayed))
if repeats < 10:
    raise SystemExit("no resent events reached the lane: the repeat check was not exercised")
f = sc[10:]
exp, app, ontime = (sum(nums(q, f)) for q in (r"alerts expected ([0-9,]+)", r"at the app ([0-9,]+),", r"on time \(<[0-9]+ ms\) ([0-9,]+)"))
missing, twice, stale = (sum(nums(q, f)) for q in (r"missing ([0-9,]+)", r"pushed twice ([0-9,]+)", r"stale pushed ([0-9,]+)"))
late_crash = sum(nums(r"late crash sent ([0-9,]+)", f))
print("fresh alerts expected at the app: %d  reached it: %d  on time (<1 s): %d (%.3f%%)  missing: %d  pushed twice: %d  stale pushed as news: %d  late crash alerts sent: %d" % (
    exp, app, ontime, 100 * ontime / max(app, 1), missing, twice, stale, late_crash))
d = [l for l in f if re.search(r"event-to-app n [1-9]", l)]
if d:
    print("event-to-app (event to the app push reader, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
        st.median(nums(r"app n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"app n [0-9,]+ p50 [0-9.]+ms p99 ([0-9.]+)ms", d)), len(d)))
if app < 500:
    raise SystemExit("too few alerts reached the app to judge the lane")
if twice or stale:
    raise SystemExit("an alert was pushed twice, or a stale event was pushed as news")
if missing * 1000 > exp or ontime * 100 < app * 99:
    raise SystemExit("more than 0.1% of fresh alerts never reached the app, or more than 1% arrived late")
done = [l for l in open("scooters.log") if "reconnect storm done" in l]
if not done:
    raise SystemExit("the reconnect storm did not run")
backlog = int(re.search(r"([0-9,]+) buffered events", done[0]).group(1).replace(",", ""))
h = [l for l in open("history.log") if "total filed" in l]
tf, tr = (int(x.replace(",", "")) for x in re.search(r"total filed ([0-9,]+) repeats ([0-9,]+)", h[-1]).groups())
busy = [l for l in h if re.search(r"filed [1-9]", l)]
print("reconnect backlog: %d events sent to ride history, %d filed and %d resends dropped, over %d s; most behind %.2f s" % (
    backlog, tf, tr, len(busy), max(nums(r"behind the newest by ([0-9.]+) s", busy) or [0])))
if backlog < 1000 or tf + tr != backlog:
    raise SystemExit("ride history did not file the whole reconnect backlog")
e = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic, %.1f MB/s during the reconnect burst" % (
        len(e), st.median(nums(r"~([0-9.]+) MB/s", e)), st.median(nums(r"~([0-9.]+) MB/s", [l for l in e if "burst" in l]) or [0])))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
