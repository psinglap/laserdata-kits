#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Osprey action lane at small scale for about a minute:
# actions from 200,000 users keyed by user on 16 partitions (3% of sessions are spam runs from fresh accounts,
# 2x raid peak for the first 15 s), two rules workers owning 8 partitions each, the effect side auditing every
# execution result, and a search reindex backfill of 8,000 message documents/s next door from second 30.
# Pass only if each worker holds the 50 ms p99 budget in 95% of its steady-state seconds, labels and rejections
# flow, no message was let through after its sender's label, no user was labelled twice or by two workers,
# and no action was out of order, missing or on another partition.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst that stalls every partition at once.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py actions.py rules_worker.py effects_audit.py reindex_backfill.py pubsub_bridge.py
# The workers and the effect side start first and create the topics; the producers write into them.
for w in 0 1; do
  timeout 85 python -u rules_worker.py --worker $w --workers 2 --partitions 16 --result-partitions 8 > rules-$w.log 2>&1 &
done
timeout 85 python -u effects_audit.py --result-partitions 8 > effects.log 2>&1 &
sleep 3
timeout 80 python -u actions.py --rate 3000 --peak 2 --peak-s 15 --users 200000 --partitions 16 > actions.log 2>&1 &
timeout 80 python -u reindex_backfill.py --rate 8000 --at 30 --partitions 8 > backfill.log 2>&1 &
wait || true
echo "--- actions.log";  head -n 2 actions.log; tail -n 2 actions.log
echo "--- backfill.log"; head -n 1 backfill.log; tail -n 1 backfill.log
for w in 0 1; do echo "--- rules-$w.log"; head -n 3 rules-$w.log; tail -n 4 rules-$w.log; done
echo "--- effects.log"; head -n 3 effects.log; tail -n 3 effects.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" rules-*.log | cut -c1-170 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for w in 0 1; do
  steady=$(grep -E "\[rules-$w\] +[1-9][0-9,]* actions/s evaluated" rules-$w.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "rules-$w steady-state seconds with live actions: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
  last=$(grep -E "\[rules-$w\] .* actions/s evaluated" rules-$w.log | tail -n 1)
  if ! grep -q "out of order 0  missing 0  on another partition 0 " <<<"$last"; then
    echo "rules-$w saw actions out of order, missing or on another partition"; status=1; fi
done
[ "$status" -eq 0 ] || { echo "action lane check failed"; exit 1; }
grep -Eq "\[effects\] .* [1-9][0-9,]* labels added" effects.log || { echo "no labels were added"; exit 1; }
grep -Eq "\[rules-[01]\] .*rejected with label in place +[1-9]" rules-*.log || { echo "no rejections with the label in place"; exit 1; }
last=$(grep -E "\[effects\] .* results/s" effects.log | tail -n 1)
grep -q "(0 total)  users labelled twice or by 2+ workers 0 " <<<"$last" || { echo "messages let through after the label, or users labelled twice"; exit 1; }
grep -Eq "\[backfill\] [1-9]" backfill.log || { echo "the backfill did not run"; exit 1; }
grep -Eq " [1-9][0-9,]* spam runs/s" actions.log || { echo "no spam runs"; exit 1; }
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
    t = [l for l in open(f"rules-{w}.log") if re.search(r"\] +[1-9][0-9,]* actions/s evaluated", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("rules-%d steady-state medians over %d s: actions/s %.0f  action-to-verdict p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms  labels/s %.0f  rejected with label/s %.0f" % (
          w, len(t), st.median(num(r"\] +([0-9,]+) actions/s", t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99),
          st.median(num(r"labels added +([0-9,]+)/s", t)), st.median(num(r"in place +([0-9,]+)/s", t))))
    # p99 before and during the backfill (it starts about 27 s into the workers' steady lines)
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"rules-{w}.log") if "actions/s evaluated" in l])
    before, during = allp[10:28], allp[30:]
    if before and during:
        print("rules-%d p99 median before the backfill %.1f ms (%d s), during it %.1f ms (%d s)" % (
              w, st.median(before), len(before), st.median(during), len(during)))
s = [l for l in open("effects.log") if re.search(r"\] +[1-9][0-9,]* results/s", l)][10:]
if s:
    print("effect side medians over %d s: results/s %.0f  action-to-result p50 %.1f ms  p99 %.1f ms" % (
          len(s), st.median(num(r"\] +([0-9,]+) results/s", s)), st.median(num(r"p50 +([0-9.]+)ms", s)),
          st.median(num(r"p99 +([0-9.]+)ms", s))))
    print("effect side at the end:", s[-1].split("ms  ", 2)[-1].strip())
f = [l for l in open("actions.log") if "actions/s" in l]
print("actions: median %.0f actions/s, %.0f send_message/s, %.0f spam runs/s" % (
      st.median(num(r"\] +([0-9,]+) actions/s", f)), st.median(num(r"([0-9,]+) send_message/s", f)),
      st.median(num(r"([0-9,]+) spam runs/s", f))))
PY
echo "Kit OK on Apache Iggy 0.9.0"
