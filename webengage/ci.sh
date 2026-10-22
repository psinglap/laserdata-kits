#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Contextual Journeys trip lane at small scale for about a minute:
# tracked events from 200,000 users keyed by user on 16 partitions (2x for the first 15 s), about 30% of new trips
# going to a user already in one, two journey workers owning 8 partitions each, reminders 2 s after entry (compressed),
# 35% of trips exiting 0.1-0.8 s before their reminder is due, the send side auditing every reminder, and a one-time
# campaign of 10,000 messages/s next door from second 30. Pass only if each worker holds the 50 ms p99 budget in 95%
# of its steady-state seconds, reminders flow and exits cancel them, no event is out of order, missing or on another
# partition, no trip is reminded twice or advanced by two workers, and at most 0.1% of reminders reach a user who
# had already exited the trip.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the shared runner that burst, about 30 s
# after a heavy neighbour starts, stalls every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py sdk_events.py journey_engine.py send_audit.py campaign_blast.py kafka_bridge.py
# The workers and the send side start first and create the topics; the SDK events write into them.
for w in 0 1; do
  timeout 85 python -u journey_engine.py --worker $w --workers 2 --partitions 16 --send-partitions 8 --wait-s 2 \
    > journey-$w.log 2>&1 &
done
timeout 85 python -u send_audit.py --partitions 16 --send-partitions 8 > send.log 2>&1 &
sleep 3
timeout 80 python -u sdk_events.py --rate 3000 --peak 2 --peak-s 15 --users 200000 --partitions 16 --wait-s 2 \
  > sdk.log 2>&1 &
timeout 80 python -u campaign_blast.py --rate 10000 --at 30 --partitions 8 > campaign.log 2>&1 &
wait || true
echo "--- sdk.log";      head -n 2 sdk.log; tail -n 2 sdk.log
echo "--- campaign.log"; head -n 1 campaign.log; tail -n 1 campaign.log
for w in 0 1; do echo "--- journey-$w.log"; head -n 3 journey-$w.log; tail -n 4 journey-$w.log; done
echo "--- send.log"; head -n 3 send.log; tail -n 3 send.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" journey-*.log | cut -c1-170 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for w in 0 1; do
  steady=$(grep -E "\[journey-$w\] +[1-9][0-9,]* events/s applied" journey-$w.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "journey-$w steady-state seconds with live events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
  last=$(grep -E "\[journey-$w\] .* events/s applied" journey-$w.log | tail -n 1)
  if ! grep -q "out of order 0  missing 0  on another partition 0 " <<<"$last"; then
    echo "journey-$w saw events out of order, missing or on another partition"; status=1; fi
  if ! grep -Eq "reminders +[1-9][0-9,]*/s  cancelled by exit +[1-9]" <<<"$steady"; then
    echo "journey-$w sent no reminders or cancelled none"; status=1; fi
  if ! grep -Eq "users in 2\+ trips [1-9]" <<<"$steady"; then echo "journey-$w had no users in parallel trips"; status=1; fi
done
[ "$status" -eq 0 ] || { echo "trip lane check failed"; exit 1; }
grep -Eq "\[send\] +[1-9][0-9,]* reminders/s queued" send.log || { echo "no reminders were queued"; exit 1; }
last=$(grep -E "\[send\] .* reminders/s queued" send.log | tail -n 1)
grep -q "trips reminded twice 0  users advanced by 2+ workers 0  exit events seen [1-9]" <<<"$last" || {
  echo "duplicate reminders, shared users, or no exit events seen by the audit"; exit 1; }
python - "$last" <<'PY' || { echo "too many reminders reached users who had already exited"; exit 1; }
import re, sys
m = re.search(r"exited ([0-9,]+) \(([0-9.]+)% of ([0-9,]+) judged", sys.argv[1])
stale, judged = int(m.group(1).replace(",", "")), int(m.group(3).replace(",", ""))
print(f"reminders judged {judged:,}, sent after the trip had exited {stale:,}")
sys.exit(0 if judged >= 1000 and stale * 1000 <= judged else 1)
PY
grep -Eq "\[campaign\] [1-9]" campaign.log || { echo "the campaign blast did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for w in (0, 1):
    t = [l for l in open(f"journey-{w}.log") if re.search(r"\] +[1-9][0-9,]* events/s applied", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("journey-%d steady-state medians over %d s: events/s %.0f  event-to-trip p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms  trips entered/s %.0f  reminders/s %.0f  cancelled by exit/s %.0f  "
          "users in 2+ trips %.0f" % (
          w, len(t), st.median(num(r"\] +([0-9,]+) events/s", t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99),
          st.median(num(r"entered +([0-9,]+)/s", t)), st.median(num(r"reminders +([0-9,]+)/s", t)),
          st.median(num(r"exit +([0-9,]+)/s", t)), st.median(num(r"2\+ trips ([0-9,]+)", t))))
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"journey-{w}.log") if "events/s applied" in l])
    before, during = allp[10:28], allp[30:]
    if before and during:
        print("journey-%d p99 median before the campaign blast %.1f ms (%d s), during it %.1f ms (%d s)" % (
              w, st.median(before), len(before), st.median(during), len(during)))
s = [l for l in open("send.log") if re.search(r"\] +[1-9][0-9,]* reminders/s", l)][5:]
if s:
    print("send side medians over %d s: reminders/s %.0f  sent after due p50 %.1f ms  p99 %.1f ms" % (
          len(s), st.median(num(r"\] +([0-9,]+) reminders/s", s)), st.median(num(r"due p50 +([0-9.]+)ms", s)),
          st.median(num(r"p99 +([0-9.]+)ms", s))))
    print("send side at the end:", s[-1].split("ms  ", 2)[-1].strip())
f = [l for l in open("sdk.log") if "tracked events/s" in l]
print("sdk: median %.0f tracked events/s, %.0f trips started/s, %.0f of them a user's 2nd+ trip" % (
      st.median(num(r"\] +([0-9,]+) tracked", f)), st.median(num(r"([0-9,]+) trips started", f)),
      st.median(num(r"([0-9,]+) of them", f))))
PY
echo "Kit OK on Apache Iggy 0.9.0"
