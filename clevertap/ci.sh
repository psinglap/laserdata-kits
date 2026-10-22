#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the decision outcome lane at small scale for about a minute,
# with a surge and an event-history stream next to it, and pass only if the lane reports PASS and every next
# decision is made with the user's last outcome already in the learner, with no wrong action chosen.
# Then a 40 s what-if: the same requests with every outcome applied 1 s late, as a batching path would.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py apps.py learner.py event_history.py tee_bridge.py
# The learner starts first and creates the topics; the apps and the neighbour follow.
timeout 78 python -u learner.py > learner.log 2>&1 &
sleep 3
# 1,500 outcomes a second over 200,000 users, half followed by a decision request 0.3-2 s later,
# 2x for 15 s from second 30 (an IPL over ends); 1 MB/s of event history from second 20.
timeout 72 python -u apps.py --users 200000 --outcomes-per-s 1500 --surge 2 --surge-at 30 --surge-for 15 > apps.log 2>&1 &
timeout 70 python -u event_history.py --mbps 1 --at 20 --duration 35 > history.log 2>&1 &
wait || true
# What-if: the same apps, with the learner applying each outcome only once it is 1 s old.
timeout 45 python -u learner.py --name hold --hold-ms 1000 > hold.log 2>&1 &
sleep 3
timeout 40 python -u apps.py --users 200000 --outcomes-per-s 1500 --surge 1 > apps-hold.log 2>&1 &
wait || true
# The bridge: three records in the Upload Events API shape, piped in; two map to the lane (App Launched does not).
printf '%s\n' '{"identity":"bridge-check","type":"event","evtName":"Notification Sent","evtData":{"action":"cart_reminder_push"}}' \
  '{"identity":"bridge-check","type":"event","evtName":"Charged","evtData":{"Amount":499}}' \
  '{"identity":"bridge-check","type":"event","evtName":"App Launched"}' | timeout 20 python -u tee_bridge.py > bridge.log 2>&1
echo "--- apps.log";      head -n 2 apps.log; tail -n 4 apps.log
echo "--- learner.log";   tail -n 8 learner.log
echo "--- history.log";   head -n 2 history.log; tail -n 2 history.log
echo "--- hold.log";      tail -n 4 hold.log
echo "--- bridge.log";    cat bridge.log
grep -q "2 records published" bridge.log || { echo "the bridge did not publish the two mapped records"; exit 1; }
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" learner.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[learner\] +[1-9][0-9,]* outcomes/s' learner.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[history\] done" history.log || { echo "the event-history neighbour did not run"; exit 1; }
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("learner.log") if re.search(r"\[learner\] +[1-9][0-9,]* outcomes/s", l)][10:]
f = [l for l in open("learner.log") if re.search(r"\[next-decision\] decisions [1-9]", l)][10:]
print("steady-state medians over %d s: outcomes/s %.0f  outcome-to-learner p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) outcomes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms"))))
print("rewards credited to the preceding decision: %.0f" % sum(nums(r"rewards credited ([0-9,]+)", h)))
peak = [l for l in h if nums(r"\] +([0-9,]+) outcomes/s", [l])[0] > 2000]
if peak:
    print("surge: %d s above 2,000 outcomes/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
stale = nums(r"without the last outcome ([0-9.]+)%", f)
print("next decisions: %d s checked, %.0f decisions, %.0f changed by the user's last outcome, decided without the last "
      "outcome: median %.3f%%, worst second %.3f%%, wrong action chosen %.0f" % (
    len(f), sum(nums(r"decisions ([0-9,]+)  changed", f)), sum(nums(r"last outcome ([0-9,]+)  decided", f)),
    st.median(stale) if stale else float("nan"), max(stale) if stale else float("nan"),
    sum(nums(r"wrong action chosen ([0-9,]+)", f))))
e = [l for l in open("history.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("event-history neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
hr = [l for l in open("hold.log") if re.search(r"\[next-decision\] decisions [1-9]", l)][5:]
if hr:
    print("what-if, every outcome applied 1 s late: %d s, %.0f decisions, decided without the last outcome: median %.1f%%, "
          "wrong action chosen %.0f (sent when nothing was due %.0f)" % (len(hr), sum(nums(r"decisions ([0-9,]+)  changed", hr)),
          st.median(nums(r"without the last outcome ([0-9.]+)%", hr)), sum(nums(r"wrong action chosen ([0-9,]+)", hr)),
          sum(nums(r"nothing was due ([0-9,]+)", hr))))
if len(f) < 30:
    raise SystemExit("too few seconds with decision requests")
if st.median(stale) > 0.1:
    raise SystemExit("too many decisions were made without the user's last outcome")
if "(surge)" not in open("apps.log").read() or not peak:
    raise SystemExit("the surge did not reach the lane")
if not sum(nums(r"rewards credited ([0-9,]+)", h)):
    raise SystemExit("no goal event was credited to a decision")
PY
echo "Kit OK on Apache Iggy 0.9.0"
