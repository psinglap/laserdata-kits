#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the card state lane at small scale for about a minute with two
# stand-in replicas, a market-open burst and a settlement backfill next door. Pass only if each stand-in holds the
# 50 ms change-to-cache p99 in 95% of its steady-state seconds, stand-in decisions flow back and are compared with
# the primary, and the lagged-cache comparison is measured.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ledger.py standin.py settlement_backfill.py kafka_bridge.py
# 50,000 card accounts, 1,200 changes/s with a 2x market open (2,400/s) from second 30 to 45, 150 other
# authorization requests/s plus follow-ups, and 2 MB/s of settlement records from second 20 for 35 s.
timeout 82 python -u ledger.py --accounts 50000 --rate 1200 --auths 150 --open 2 --open-at 30 --open-for 15 > ledger.log 2>&1 &
sleep 4
for n in 1 2; do
  timeout 72 python -u standin.py --name standin-$n > standin-$n.log 2>&1 &
done
timeout 70 python -u settlement_backfill.py --mbps 2 --at 20 --duration 35 > settlement.log 2>&1 &
wait || true
echo "--- ledger.log"; head -n 1 ledger.log; sed -n '40,41p' ledger.log; tail -n 2 ledger.log
for n in 1 2; do echo "--- standin-$n.log"; head -n 3 standin-$n.log; tail -n 3 standin-$n.log; done
echo "--- settlement.log"; head -n 2 settlement.log; tail -n 2 settlement.log
for n in 1 2; do
  echo "--- standin-$n FAIL seconds (line: output)"; grep -n "FAIL (" standin-$n.log | cut -c1-150 || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[standin-$n\] +[1-9][0-9,]* changes/s" standin-$n.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "standin-$n steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for n in (1, 2):
    p = [l for l in open(f"standin-{n}.log") if re.search(r"\] +[1-9][0-9,]* changes/s", l)][10:]
    print("standin-%d steady-state medians: changes/s %.0f  change-to-cache p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst p99 %.1f ms" % (
        (n,) + tuple(st.median(nums(q, p)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                                     r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
l = [x for x in open("ledger.log") if "decisions compared" in x][12:]
opening = [x for x in l if "(open)" in x]
c, f, stale, wrong, ab, db, lag, lab, ldb = (sum(nums(q, l)) for q in (
    r"compared ([0-9,]+) \(", r"follow-ups ([0-9,]+)\)", r"stale state ([0-9,]+)", r"the primary ([0-9,]+) ",
    r"the primary [0-9,]+ \(approved ([0-9,]+)", r"the primary [0-9,]+ \(approved [0-9,]+ declined ([0-9,]+)",
    r"disagree on ([0-9,]+)", r"disagree on [0-9,]+ \(approved ([0-9,]+)", r"disagree on [0-9,]+ \(approved [0-9,]+ declined ([0-9,]+)"))
print("after warm-up: stand-in decisions compared %d (%d follow-ups)  on stale state %d  disagreed with the primary %d "
      "(approved %d, declined %d)" % (c, f, stale, wrong, ab, db))
print("a cache 0.5 s behind would have disagreed on %d (%.2f%% of decisions; approved %d it should have declined, "
      "declined %d it should have approved)" % (lag, 100 * lag / max(c, 1), lab, ldb))
print("market-open seconds: %d at a median %.0f changes/s" % (len(opening), st.median(nums(r"\] +([0-9,]+) changes/s", opening)) if opening else 0))
if c < 1000 or lag < 1:
    raise SystemExit("stand-in decisions or the lagged-cache comparison did not flow")
PY
grep -q "\[settlement\] done" settlement.log || { echo "the settlement neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
