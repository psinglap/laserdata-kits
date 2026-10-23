#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the payment token lane at small scale for about a minute with two
# subscribers, a scheduled card updater run and a warehouse feed next door. Pass only if each subscriber holds the
# 50 ms change-to-cache p99 in 95% of its steady-state seconds, every record verifies and carries no card number,
# the guard masked the card numbers it was given, and instalment charges flow back and are checked against the vault.
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
python -m py_compile common.py token_jobs.py instalments.py warehouse_feed.py kafka_bridge.py
# The signature and the card number guard, on their own: a changed record must fail, a masked one must be clean.
python - <<'PY'
from common import load, mask_record, msg, sign, strings_with_card_numbers, verify
signed = sign(msg(k="C00000001", seq=2, token="tok_1", note="replaced 4111111111111111"))
assert verify(signed) is not None and verify(signed.replace(b"tok_1", b"tok_2")) is None
d = load(verify(signed))
assert strings_with_card_numbers(d) == 1 and mask_record(d) == 1 and strings_with_card_numbers(d) == 0
print("signature and card number guard: OK")
PY
# 100,000 customers, 600 token changes/s with a 4x card updater run (2,400/s) from second 30 to 45, one updater
# note in 500 carrying a raw card number, two subscribers charging 400 instalments/s each, and 2 MB/s of
# warehouse events from second 20 for 35 s.
timeout 82 python -u token_jobs.py --customers 100000 --rate 600 --run 4 --run-at 30 --run-for 15 --pan-every 500 > tokens.log 2>&1 &
sleep 4
for n in 1 2; do
  timeout 72 python -u instalments.py --name collections-$n --charges 400 > collections-$n.log 2>&1 &
done
timeout 70 python -u warehouse_feed.py --mbps 2 --at 20 --duration 35 > warehouse.log 2>&1 &
wait || true
echo "--- tokens.log"; head -n 1 tokens.log; sed -n '40,41p' tokens.log; tail -n 2 tokens.log
for n in 1 2; do echo "--- collections-$n.log"; head -n 3 collections-$n.log; tail -n 3 collections-$n.log; done
echo "--- token job seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" tokens.log | sed -E 's/\(card_updater.*would send [0-9,]+//' || true
echo "--- warehouse.log"; head -n 2 warehouse.log; tail -n 2 warehouse.log
for n in 1 2; do
  echo "--- collections-$n FAIL seconds (line: output)"; grep -n "FAIL (" collections-$n.log | sed -E 's/  instalments charged.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[collections-$n\] +[1-9][0-9,]* changes/s" collections-$n.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "collections-$n steady-state seconds with live traffic: PASS $live, FAIL $fail"
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
bad = pan = 0
for n in (1, 2):
    every = [l for l in open(f"collections-{n}.log") if "change-to-cache" in l]
    p = [l for l in every if re.search(r"\] +[1-9][0-9,]* changes/s", l)][10:]
    print("collections-%d steady-state medians: changes/s %.0f  change-to-cache p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst p99 %.1f ms" % (
        (n,) + tuple(st.median(nums(q, p)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                                     r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
    bad += sum(nums(r"bad signatures ([0-9]+)", every))
    pan += sum(nums(r"card numbers seen ([0-9]+)", every))
l = [x for x in open("tokens.log") if "charges checked" in x][12:]
run = [x for x in l if "(updater run)" in x]
c, stale, lag, masked = (sum(nums(q, l)) for q in (r"charges checked ([0-9,]+)", r"superseded token ([0-9,]+)",
                                                  r"behind would send ([0-9,]+)", r"masked ([0-9,]+)"))
print("after warm-up: instalment charges checked against the vault %d  sent on a superseded token %d" % (c, stale))
print("a subscriber 5 s behind would have sent %d on a superseded token (%.2f%% of charges)" % (lag, 100 * lag / max(c, 1)))
print("card numbers masked by the guard %d  seen by subscribers %d  records failing the signature check %d" % (masked, pan, bad))
print("updater-run seconds: %d at a median %.0f changes/s" % (len(run), st.median(nums(r"\] +([0-9,]+) changes/s", run)) if run else 0))
if c < 1000 or lag < 1:
    raise SystemExit("instalment charges or the lagged-subscriber comparison did not flow")
if stale * 100 > lag:
    raise SystemExit("the lane sent more than 1% as many charges on a superseded token as a 5 s lag would")
if masked < 1 or pan or bad:
    raise SystemExit("the card number guard or the signature check did not hold")
PY
grep -q "\[warehouse\] done" warehouse.log || { echo "the warehouse neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
