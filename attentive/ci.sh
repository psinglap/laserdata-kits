#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the subscriber decision lane at small scale for about a minute:
# shopping events from 200,000 subscribers keyed by subscriber on 16 partitions (2x flash-sale peak for the first
# 15 s), two decision workers owning 8 partitions each, the send side auditing every queued text, and a campaign
# blast of 10,000 texts/s next door from second 30. Pass only if each worker holds the 50 ms p99 budget in 95% of
# its steady-state seconds, texts flow, and no subscriber ever got a duplicate text, was decided by two workers,
# or had an event out of order, missing, or on another partition.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py storefront.py decision.py send_audit.py bfcm_flood.py pulsar_bridge.py
# The workers and the send side start first and create the topics; the storefront writes into them.
for w in 0 1; do
  timeout 85 python -u decision.py --worker $w --workers 2 --partitions 16 --send-partitions 8 > decide-$w.log 2>&1 &
done
timeout 85 python -u send_audit.py --send-partitions 8 > send.log 2>&1 &
sleep 3
timeout 80 python -u storefront.py --rate 3000 --peak 2 --peak-s 15 --subscribers 200000 --brands 400 \
  --partitions 16 > storefront.log 2>&1 &
timeout 80 python -u bfcm_flood.py --rate 10000 --at 30 --partitions 8 > bfcm.log 2>&1 &
wait || true
echo "--- storefront.log"; head -n 2 storefront.log; tail -n 2 storefront.log
echo "--- bfcm.log";       head -n 1 bfcm.log; tail -n 1 bfcm.log
for w in 0 1; do echo "--- decide-$w.log"; head -n 3 decide-$w.log; tail -n 4 decide-$w.log; done
echo "--- send.log"; head -n 3 send.log; tail -n 3 send.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" decide-*.log | cut -c1-170 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for w in 0 1; do
  steady=$(grep -E "\[decide-$w\] +[1-9][0-9,]* events/s decided" decide-$w.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "decide-$w steady-state seconds with live events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
  last=$(grep -E "\[decide-$w\] .* events/s decided" decide-$w.log | tail -n 1)
  if ! grep -q "out of order 0  missing 0  on another partition 0 " <<<"$last"; then
    echo "decide-$w saw events out of order, missing or on another partition"; status=1; fi
done
[ "$status" -eq 0 ] || { echo "decision lane check failed"; exit 1; }
grep -Eq "\[send\] +[1-9][0-9,]* texts/s queued" send.log || { echo "no texts were queued"; exit 1; }
last=$(grep -E "\[send\] .* texts/s queued" send.log | tail -n 1)
grep -q "(0 total)  subscribers decided by 2+ workers 0 " <<<"$last" || { echo "duplicate texts or shared subscribers"; exit 1; }
grep -Eq "\[bfcm\] [1-9]" bfcm.log || { echo "the campaign blast did not run"; exit 1; }
grep -Eq "[1-9][0-9,]* sessions/s with 2\+ triggers" storefront.log || { echo "no multi-trigger sessions"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
bfcm_on = None
for w in (0, 1):
    t = [l for l in open(f"decide-{w}.log") if re.search(r"\] +[1-9][0-9,]* events/s decided", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("decide-%d steady-state medians over %d s: events/s %.0f  event-to-decision p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms  texts/s %.0f  repeat triggers held/s %.0f" % (
          w, len(t), st.median(num(r"\] +([0-9,]+) events/s", t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99),
          st.median(num(r"texts +([0-9,]+)/s", t)), st.median(num(r"held +([0-9,]+)/s", t))))
    # p99 before and during the campaign blast (it starts about 27 s into the workers' steady lines)
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"decide-{w}.log") if "events/s decided" in l])
    before, during = allp[10:28], allp[30:]
    if before and during:
        print("decide-%d p99 median before the campaign blast %.1f ms (%d s), during it %.1f ms (%d s)" % (
              w, st.median(before), len(before), st.median(during), len(during)))
s = [l for l in open("send.log") if re.search(r"\] +[1-9][0-9,]* texts/s", l)][10:]
if s:
    print("send side medians over %d s: texts/s %.0f  event-to-send-intent p50 %.1f ms  p99 %.1f ms" % (
          len(s), st.median(num(r"\] +([0-9,]+) texts/s", s)), st.median(num(r"p50 +([0-9.]+)ms", s)),
          st.median(num(r"p99 +([0-9.]+)ms", s))))
    print("send side at the end:", s[-1].split("ms  ", 2)[-1].strip())
f = [l for l in open("storefront.log") if "shopping events/s" in l]
print("storefront: median %.0f shopping events/s, %.0f triggers/s, %.0f sessions/s with 2+ triggers" % (
      st.median(num(r"\] +([0-9,]+) shopping", f)), st.median(num(r"([0-9,]+) triggers/s", f)),
      st.median(num(r"([0-9,]+) sessions/s with", f))))
PY
echo "Kit OK on Apache Iggy 0.9.0"
