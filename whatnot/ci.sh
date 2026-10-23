#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the story SLO lane at small scale for about a minute, with the rest of
# client telemetry next to it and a black screen on most web livestream joins, and pass only if the story lane
# reports PASS, the judge pages for livestream_join x web and no other cohort, within the story deadline plus a few
# seconds of the fault's start, and every story is judged once although some events are delivered twice.
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
python -m py_compile common.py story_judge.py live_shows.py client_events_flood.py kafka_bridge.py
# The judge starts first and creates the topics; the clients and the neighbour follow.
# 20,000 clients, 150 livestream joins/s and 250 bids/s (about 1,500 story events/s, 1% sent twice); the story
# deadline and the SLI window are cut from 60 s to 10 s so the run fits in a minute; a black screen on 80% of web
# joins from second 20 for 30 s; 2 MB/s of client telemetry from second 15.
timeout 78 python -u story_judge.py --deadline-s 10 --window-s 10 > judge.log 2>&1 &
sleep 3
timeout 72 python -u live_shows.py --clients 20000 --joins 150 --bids 250 --deadline-s 10 \
  --fault-at 20 --fault-for 30 --consumers 3 > shows.log 2>&1 &
timeout 70 python -u client_events_flood.py --mbps 2 --at 15 --duration 40 > client_events.log 2>&1 &
wait || true
echo "--- shows.log";         head -n 2 shows.log; grep -E "ALERT|CLEAR" shows.log || true; tail -n 3 shows.log
echo "--- judge.log";         tail -n 4 judge.log
echo "--- client_events.log"; head -n 2 client_events.log; tail -n 2 client_events.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" judge.log | cut -c1-400 || true
echo "--- shows stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" shows.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[judge\] +[1-9][0-9,]* events/s' judge.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the SLO checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
allj = [l for l in open("judge.log") if re.search(r"\[judge\] +[1-9][0-9,]* events/s", l)]
r = allj[10:]
print("steady-state medians: events/s %.0f  event-to-judge p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("judge.log") if "event-to-judge" in l])
if len(lane) > 50:
    print("event-to-judge p99, median per second: before the neighbour %.1f ms (s 6-14), with 2 MB/s of client events %.1f ms (s 20-50)" % (
        st.median(lane[6:15]), st.median(lane[20:51])))
ok, failed = sum(nums(r"judged ok ([0-9,]+) ", allj)), sum(nums(r" failed ([0-9,]+) excluded", allj))
excl, rep = sum(nums(r"excluded ([0-9,]+) ", allj)), sum(nums(r"repeats dropped ([0-9,]+) ", allj))
late = sum(nums(r"late outcomes ([0-9,]+) ", allj))
sw = nums(r"deadline-to-verdict p99 ([0-9]+)ms", [l for l in allj if not re.search(r" failed 0 excluded", l)])
print("stories judged: ok %d  failed %d  excluded by a product rule %d  repeated events dropped %d  late outcomes %d" % (
    ok, failed, excl, rep, late))
if sw:
    print("deadline-to-verdict p99 (the judge waits 200 ms for events in flight), median per second: %.0f ms" % st.median(sw))
if ok < 5000 or failed < 1 or excl < 1 or rep < 100:
    raise SystemExit("too few stories, failures, exclusions or repeats to judge the lane")
# Before the fault reaches verdicts (second 20 + 10 s deadline), every cohort's SLI is healthy.
pre = [l for l in allj[12:] if "alerting: none" in l][:12]
for cohort in ("livestream_join/web", "livestream_join/ios", "livestream_join/android", "place_bid/web",
               "place_bid/ios", "place_bid/android"):
    v = nums(re.escape(cohort) + r" ([0-9.]+)%", pre)
    if not v or min(v) < 97:
        raise SystemExit("SLI for %s before the fault was not healthy: %s" % (cohort, v))
print("SLI before the fault, lowest second: " + "  ".join("%s %.1f%%" % (c, min(nums(re.escape(c) + r" ([0-9.]+)%", pre)))
      for c in ("livestream_join/web", "livestream_join/ios", "livestream_join/android", "place_bid/web")))
m = open("shows.log").read()
a = re.search(r"ALERT livestream_join x web at the pager ([0-9,]+) ms after the fault began: SLI ([0-9.]+)% of ([0-9]+) clients, missing (\{[^}]*\}); deadline-to-pager ([0-9.]+) ms", m)
if not a:
    raise SystemExit("no alert for livestream_join x web reached the pager")
print("livestream_join x web paged %s ms after the black screen began (story deadline 10 s): SLI %s%% of %s clients, missing %s; %s ms after the deadline of the story that tipped it" % a.groups())
if float(a.group(1).replace(",", "")) > 15_000:
    raise SystemExit("the page came more than 5 s after the first broken story's deadline")
if "'video_playing'" not in a.group(4):
    raise SystemExit("the alert did not name video_playing as the missing outcome")
f = [l for l in open("shows.log") if re.search(r"\[live-shows\] +[0-9,]+ events/s", l)]
other = sum(nums(r"other cohorts ([0-9,]+)", f))
print("alerts for healthy cohorts: %d" % other)
if other:
    raise SystemExit("a healthy cohort was alerted")
web = sum(nums(r"web joins during the fault ([0-9,]+):", f))
black = sum(nums(r"no video ([0-9,]+),", f))
before = sum(nums(r"before the pager had the alert ([0-9,]+) ", f))
mp = [l for l in f if "once-a-minute judge: " in l and "judge: not yet" not in l]
mpage = nums(r"once-a-minute judge: ([0-9]+) s", mp[-1:]) if mp else []
print("web joins during the fault: %d  with a black screen: %d  of them before the pager had the alert: %d" % (web, black, before))
if mpage:
    print("a once-a-minute judge over the same stories would page %.0f s after the black screen began" % mpage[0])
d = [l for l in f if re.search(r"knowable-to-consumers n [1-9]", l)]
if d:
    print("knowable-to-consumers (deadline or last outcome to 3 consumers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
        st.median(nums(r"consumers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if sum(nums(r"SLI snapshots ([0-9,]+)", f)) < 30:
    raise SystemExit("the Datadog exporter did not get the per-second SLI snapshots")
e = [l for l in open("client_events.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("client telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[client-events\] done" client_events.log || { echo "the client telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
