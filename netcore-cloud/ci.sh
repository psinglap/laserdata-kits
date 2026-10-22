#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the delivery status lane at small scale for about a minute:
# 1,000 outbound messages a second across 40 campaigns, their Sent, Delivered and Read webhooks on three
# status topics (with 2% provider retries), an uncapped Delivered reporter and a Read reporter capped at
# 1,500 webhooks/s, and a Read surge (+2,500 Read webhooks/s for 12 s) on the largest campaign.
# Pass only if the Delivered reporter holds its 100 ms p99 budget in 95% of its steady-state seconds,
# Delivered counts land in the report within 1 s, retries are counted once, and the Read surge is held
# at its cap on its own topic and then drains.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst stalled every
# partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py webhooks.py reporter.py status_bridge.py
# The reporters start first; each creates its status topic if it is missing.
timeout 80 python -u reporter.py --status delivered --readers 2 --budget-ms 100 > delivered.log 2>&1 &
timeout 80 python -u reporter.py --status read --readers 1 --max-rate 1500 --budget-ms 1000 > read.log 2>&1 &
sleep 2
# The largest campaign's Read surge runs from the receiver's second 20 to 32.
timeout 76 python -u webhooks.py --send-rate 1000 --campaigns 40 --read-after 1 8 \
  --surge-rate 2500 --surge-at 20 --surge-for 12 > webhooks.log 2>&1 &
wait || true
echo "--- webhooks.log"; head -n 2 webhooks.log; grep -m 2 "Read surge" webhooks.log || true; tail -n 2 webhooks.log
for s in delivered read; do echo "--- $s.log"; head -n 3 $s.log; tail -n 4 $s.log; done
echo "--- Read held at its cap (first, last) and drain"; (grep "held at its write cap" read.log | sed -n '1p;$p'; grep "back inside" read.log) || true
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" delivered.log | cut -c1-170 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E "\[delivered\] +[1-9][0-9,]* delivered statuses/s" delivered.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "delivered steady-state seconds with live webhooks: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "held at its write cap" read.log || { echo "the Read surge never reached the Read writer's cap"; exit 1; }
grep -q "back inside its write cap" read.log || { echo "the Read backlog did not drain"; exit 1; }
if grep -q "held at its write cap" delivered.log; then echo "the Delivered writer was held"; exit 1; fi
python - <<'PY'
import re, statistics as st
row = r"\[delivered\] +([0-9,]+) delivered statuses/s"
def num(pat, ls):
    out = []
    for l in ls:                                   # skip lines where a value is missing, never crash the summary
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
d = [l for l in open("delivered.log") if re.search(row, l) and not re.search(r"\] +0 delivered", l)][10:]
p99 = num(r"p99 +([0-9.]+)ms", d)
print("delivered steady-state medians over %d s: statuses/s %.0f  webhook-to-reporter p50 %.1f ms  p99 %.1f ms  "
      "p99.9 %.1f ms  worst-second p99 %.1f ms  report rows/s %.0f  in report within 1 s %.2f%%  retries dropped/s %.0f" % (
      len(d), st.median(num(row, d)), st.median(num(r"p50 +([0-9.]+)ms", d)), st.median(p99),
      st.median(num(r"p99.9 +([0-9.]+)ms", d)), max(p99), st.median(num(r"report rows ([0-9,]+)/s", d)),
      st.median(num(r"within 1 s +([0-9.]+)%", d)), st.median(num(r"retries dropped ([0-9,]+)", d))))
r = open("read.log").read().splitlines()
held = [l for l in r if "held at its write cap" in l]
held_t = {int(x) for x in num(r"t=([0-9]+)", held)}
during = [p for l, p in zip(d, p99) if int(re.search(r"t=([0-9]+)", l).group(1)) in held_t]
other = [p for l, p in zip(d, p99) if int(re.search(r"t=([0-9]+)", l).group(1)) not in held_t]
if during and other:
    print("delivered p99, median per second: while Read was held at its cap %.1f ms (%d s), otherwise %.1f ms (%d s)" % (
        st.median(during), len(during), st.median(other), len(other)))
print("Read surge: held for %d s, largest backlog %s on status-read, longest wait %.1f s; %s" % (
    len(held), f"{max(num(r'backlog ([0-9,]+)', held)):,.0f}", max(num(r"oldest waiting +([0-9.]+) s", held)),
    next(l.split("] ", 1)[1] for l in r if "back inside" in l)))
if st.median(num(r"within 1 s +([0-9.]+)%", d)) < 99.9:
    raise SystemExit("too few Delivered statuses reached the report within 1 s")
if sum(num(r"retries dropped ([0-9,]+)", d)) < 1:
    raise SystemExit("no provider retries were dropped")
if st.median(num(r"report rows ([0-9,]+)/s", d)) < 1:
    raise SystemExit("no report rows written")
PY
grep -Eq "\[webhooks\] +[1-9]" webhooks.log || { echo "the webhook receiver did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
