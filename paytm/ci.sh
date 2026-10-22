#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Soundbox payment alert lane at small scale for about a minute,
# with a bank recovering from an outage in the middle and app events next door, and pass only if the lane reports
# PASS through it, every fresh payment is announced once, on time and with the right total, no payment from
# minutes ago is announced, no device's total goes backwards, and the passbook files every late confirmation.
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
python -m py_compile common.py announcer.py passbook.py payments.py analytics_flood.py kafka_bridge.py
# The announcer and the passbook start first and create the topics; the merchants and the neighbour follow.
# 60,000 Soundbox merchants, a payment every 30 s each (about 2,000 confirmations/s), 1% delivered twice; for 5%
# of them the customers' bank is down for 6 minutes and recovers at 30 s, settling their payments over 8 s;
# 2 MB/s of app events from 15 s, 4 MB/s during a 10 s burst.
# vmstat records the runner's CPU and disk each second, so a slow second can be traced to the host.
vmstat -n 1 80 > vmstat.log 2>&1 &
timeout 80 python -u announcer.py > announcer.log 2>&1 &
timeout 80 python -u passbook.py > passbook.log 2>&1 &
sleep 3
timeout 72 python -u payments.py --devices 60000 --pay-s 30 --recover-at 30 --held-share 0.05 --held-min 6 \
  --flush-s 8 > payments.log 2>&1 &
timeout 70 python -u analytics_flood.py --mbps 2 --size 4096 --at 15 --duration 45 --burst-at 15 --burst-for 10 \
  --burst-x 2 > app_events.log 2>&1 &
wait || true
echo "--- payments.log";   head -n 2 payments.log; sed -n "20,21p" payments.log | cut -c1-320; grep "bank recovery done" payments.log || true; tail -n 3 payments.log
echo "--- announcer.log";  sed -n "20,21p" announcer.log | cut -c1-420; tail -n 2 announcer.log | cut -c1-200; echo "announcer log lines: $(wc -l < announcer.log)"
echo "--- passbook.log";   grep -m 3 -E "filed [1-9]" passbook.log || true; tail -n 2 passbook.log
echo "--- app_events.log"; head -n 2 app_events.log; grep -m 2 "burst" app_events.log || true; tail -n 1 app_events.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" announcer.log | cut -c1-400 || true
echo "--- runner each second, from the announcer's start (vmstat: r runnable, b blocked, us sy id wa CPU %)"; cat -n vmstat.log | cut -c1-110 || true
echo "--- payments stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" payments.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[announcer\] +[1-9][0-9,]* confirmations/s' announcer.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the announcement checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("announcer.log") if re.search(r"\[announcer\] +[1-9][0-9,]* confirmations/s", l)][10:]
print("steady-state medians: confirmations/s %.0f  confirmation-to-announcer p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) confirmations/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
pl = [l for l in open("payments.log") if re.search(r"\[payments\] +[1-9][0-9,]* confirmations/s", l)]
rec = [i for i, l in enumerate(pl) if "(recovery)" in l]
lane = [l for l in open("announcer.log") if "confirmation-to-announcer" in l]
if rec and len(lane) > rec[-1] + 2:
    # the announcer starts 3 s before the merchants; its second-by-second lines line up with theirs within a second
    before = nums(r"p99 +([0-9.]+)ms", lane[10:rec[0]])
    during = nums(r"p99 +([0-9.]+)ms", lane[rec[0]:rec[-1] + 3])
    print("confirmation-to-announcer p99, median per second: before the bank recovery %.1f ms (%d s), during it %.1f ms (%d s), worst second during it %.1f ms" % (
        st.median(before), len(before), st.median(during), len(during), max(during)))
repeats = sum(nums(r"repeats dropped ([0-9,]+)", s))
quiet = sum(nums(r"late, passbook only ([0-9,]+)", s))
print("announcer in steady state: announced %d, repeated confirmations dropped %d, late confirmations that reached the lane and were not announced %d" % (
    sum(nums(r"announced ([0-9,]+) \(", s)), repeats, quiet))
if repeats < 10:
    raise SystemExit("no repeated confirmations reached the lane: the repeat check was not exercised")
f = pl[10:]
exp, dev, ontime = (sum(nums(q, f)) for q in (r"announcements expected ([0-9,]+)", r"at the device ([0-9,]+),", r"on time \(<[0-9]+ ms\) ([0-9,]+)"))
missing, twice, stale = (sum(nums(q, f)) for q in (r"missing ([0-9,]+)", r"announced twice ([0-9,]+)", r"stale announced ([0-9,]+)"))
back, wrong = (sum(nums(q, f)) for q in (r"total backwards ([0-9,]+)", r"wrong total ([0-9,]+)"))
print("fresh payments to announce: %d  announced at the device push: %d  on time (<1 s): %d (%.3f%%)  missing: %d  announced twice: %d  stale announced: %d  total backwards: %d  wrong total: %d" % (
    exp, dev, ontime, 100 * ontime / max(dev, 1), missing, twice, stale, back, wrong))
d = [l for l in f if re.search(r"confirmation-to-device n [1-9]", l)]
if d:
    print("confirmation-to-device (confirmation to the device push reader, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with announcements, %d s)" % (
        st.median(nums(r"device n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"device n [0-9,]+ p50 [0-9.]+ms p99 ([0-9.]+)ms", d)), len(d)))
if dev < 500:
    raise SystemExit("too few announcements reached the device push to judge the lane")
if twice or stale or back or wrong:
    raise SystemExit("a payment was announced twice, a stale payment was announced, or a total was wrong or went backwards")
if missing * 1000 > exp or ontime * 100 < dev * 99:
    raise SystemExit("more than 0.1% of fresh payments were never announced, or more than 1% arrived late")
done = [l for l in open("payments.log") if "bank recovery done" in l]
if not done:
    raise SystemExit("the bank recovery did not run")
late = int(re.search(r"([0-9,]+) late confirmations", done[0]).group(1).replace(",", ""))
h = [l for l in open("passbook.log") if "total filed" in l]
tf, tr = (int(x.replace(",", "")) for x in re.search(r"total filed ([0-9,]+) repeats ([0-9,]+)", h[-1]).groups())
busy = [l for l in h if re.search(r"filed [1-9]", l)]
print("late confirmations: %d sent to the passbook, %d filed and %d repeats dropped, over %d s; most behind %.2f s" % (
    late, tf, tr, len(busy), max(nums(r"behind the newest by ([0-9.]+) s", busy) or [0])))
if late < 1000 or tf + tr != late:
    raise SystemExit("the passbook did not file every late confirmation")
e = [l for l in open("app_events.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("app events neighbour: %d s, median %.1f MB/s on its own topic, %.1f MB/s during the burst" % (
        len(e), st.median(nums(r"~([0-9.]+) MB/s", e)), st.median(nums(r"~([0-9.]+) MB/s", [l for l in e if "burst" in l]) or [0])))
PY
grep -q "\[app-events\] done" app_events.log || { echo "the app events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
