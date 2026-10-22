#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the spend-to-pacing loop at small scale for about a minute, with a
# bid-feedback log next to it, and pass only if the spend lane reports PASS, ad groups reach their budget and
# are paused, and the pauses reach every bid server.
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
python -m py_compile common.py pacing.py bidders.py feedback_flood.py kafka_bridge.py
# Pacing starts first and creates the topics; the bid servers and the neighbour follow.
# 3,000 wins a second over 2,000 ad groups and 4 bid servers, 20 s flights; 2 MB/s of bid feedback from second 20.
timeout 78 python -u pacing.py --ad-groups 2000 --wins-per-s 3000 --flight-s 20 > pacing.log 2>&1 &
sleep 3
timeout 72 python -u bidders.py --servers 4 --ad-groups 2000 --wins-per-s 3000 --flight-s 20 > bidders.log 2>&1 &
timeout 70 python -u feedback_flood.py --mbps 2 --at 20 --duration 35 > feedback.log 2>&1 &
wait || true
echo "--- bidders.log";  head -n 2 bidders.log; tail -n 4 bidders.log
echo "--- pacing.log";   tail -n 8 pacing.log
echo "--- feedback.log"; head -n 2 feedback.log; tail -n 2 feedback.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" pacing.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[pacing\] +[1-9][0-9,]* spend events/s' pacing.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the pacing checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("pacing.log") if re.search(r"\[pacing\] +[1-9][0-9,]* spend events/s", l)][10:]
print("steady-state medians: spend events/s %.0f  spend-to-pacing p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) spend events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("pacing.log") if "spend events/s" in l])
if len(lane) > 50:
    print("spend-to-pacing p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of bid feedback %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
b = [l for l in open("pacing.log") if l.startswith("[budget]")]
paused = max(nums(r"paused ([0-9,]+)/", b) or [0])
flights = {}
for l in b:
    m = re.search(r"flight (\d+): spent \$([0-9,.]+) .*overspend \$([0-9,.]+) \(([0-9.]+)% of spend\)", l)
    if m:
        flights[m.group(1)] = (float(m.group(2).replace(",", "")), float(m.group(3).replace(",", "")), float(m.group(4)))
full = list(flights.items())[1:-1]          # flights seen from start to end
for f, (spent, over, pct) in full:
    print("flight %s: spent $%.2f  overspend $%.4f (%.3f%% of spend)" % (f, spent, over, pct))
print("most ad groups paused in one flight: %d of 2,000" % paused)
if paused < 100:
    raise SystemExit("pacing did not pause ad groups at their budget")
d = [l for l in open("bidders.log") if re.search(r"decision-to-bid-servers n [1-9]", l)][5:]
if not d:
    raise SystemExit("no pacing decisions reached the bid servers")
print("decision-to-bid-servers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with decisions, %d s)" % (
    st.median(nums(r"servers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
w = nums(r"withheld by pacing ([0-9,]+)", [l for l in open("bidders.log") if l.startswith("[bidders]")])
if not w or max(w) == 0:
    raise SystemExit("the bid servers never applied a pause or throttle")
e = [l for l in open("feedback.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("bid-feedback neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[feedback\] done" feedback.log || { echo "the bid-feedback neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
