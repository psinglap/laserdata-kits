#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the order-update lane at small scale for about a minute
# with a sale-day campaign burst next to it, and pass only if the lane reports PASS, receipts are
# checked against the truth, and the campaign queues behind its rate limit.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py order_services.py meecom_sender.py campaign.py confluent_bridge.py
# Meecom's sender starts first and creates the topics; the services and the campaign follow.
# Order updates and campaign sends are read by separate processes, like transactional and campaign workers.
timeout 75 python -u meecom_sender.py --only orders > meecom.log 2>&1 &
timeout 75 python -u meecom_sender.py --only campaign --campaign-per-s 2500 > meecom-campaign.log 2>&1 &
sleep 3
timeout 72 python -u order_services.py --orders-per-min 12000 > orders.log 2>&1 &
# A campaign of 100,000 recipients queued in 20 s (5,000 sends/s) from second 25, drained at 2,500/s.
timeout 70 python -u campaign.py --recipients 100000 --minutes 0.3333 --at 25 > campaign.log 2>&1 &
wait || true
echo "--- orders.log";   grep -v "receipts checked 0 " orders.log | tail -n 6
echo "--- meecom.log";   tail -n 10 meecom.log
echo "--- meecom-campaign.log"; grep -v " 0 campaign sends/s" meecom-campaign.log | tail -n 3
echo "--- campaign.log"; head -n 3 campaign.log; tail -n 2 campaign.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" meecom.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[meecom\] +[1-9][0-9,]* order updates/s' meecom.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
m = [l for l in open("meecom.log") if re.search(r"\[meecom\] +[1-9][0-9,]* order updates/s", l)][10:]
o = [l for l in open("orders.log") if re.search(r"receipts checked [1-9]", l)][10:]
b = [l for l in open("meecom.log") if l.startswith("[broker]")][10:]
c = [l for l in open("meecom-campaign.log") if re.search(r"\] +[1-9][0-9,]* campaign sends/s", l)]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: order updates/s %.0f  change-to-sender p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(p, m)) for p in (r"\] +([0-9,]+) order updates/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
busy = [l for l in m if re.search(r"campaign topic \+[1-9]", l)]
quiet = [l for l in m if "campaign topic +0/s" in l]
if busy and quiet:
    print("order-update p99, median per second: campaign topic quiet %.1f ms (%d s), campaign being queued next door %.1f ms (%d s)" % (
        st.median(num(r"p99 +([0-9.]+)ms", quiet)), len(quiet), st.median(num(r"p99 +([0-9.]+)ms", busy)), len(busy)))
if c:
    print("campaign: drained at a median %.0f sends/s for %d s; longest wait in the queue %.1f s" % (
        st.median(num(r"\] +([0-9,]+) campaign sends/s", c)), len(c), max(num(r"max +([0-9.]+) s", c))))
if b:
    print("broker: CPU %.1f%% of one core, %.0f messages written/s on all topics (median per second)" % (
        st.median(num(r"CPU +([0-9.]+)%", b)), st.median(num(r"  ([0-9,]+) messages written", b))))
on_time, stale = num(r"sent within 1 s +([0-9.]+)%", o), num(r"out of date when sent ([0-9.]+)%", o)
print("receipts: sent within 1 s %.2f%%  out of date when sent %.3f%%  (median per second, %d s)" % (
    st.median(on_time), st.median(stale), len(o)))
if len(o) < 30:
    raise SystemExit("too few seconds with receipts checked against the truth")
if st.median(stale) > 0.5:
    raise SystemExit("too many order updates out of date when sent")
if not busy or len(c) < 10:
    raise SystemExit("the campaign did not run next to the lane")
PY
grep -q "\[campaign\] done" campaign.log || { echo "campaign did not finish queueing"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
