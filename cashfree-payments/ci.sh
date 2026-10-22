#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Kill Bill usage lane at small scale for about a minute:
# 1,500 payments/s (2x for ten seconds) over 20,000 subscriptions, 1% of usage messages sent twice, the live meter
# and the Kill Bill writer each reading the lane on their own connections, and Kill Bill down for 12 s mid-run.
# Pass only if the meter holds its 100 ms p99 budget in 95% of steady-state seconds, every subscription reconciles
# to the paisa, retries are absorbed rather than counted, day totals flow, and the Kill Bill writer pauses during
# the outage, drains the backlog afterwards and ends with every subscription's Kill Bill usage equal to its totals.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which on shared runners stalled every reader
# for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py payments.py meter.py killbill.py kafka_bridge.py
# The meter and the writer start first and create the topics; the payment services follow.
timeout 80 python -u meter.py > meter.log 2>&1 &
timeout 80 python -u killbill.py --outage-at 28 --outage-for 12 > killbill.log 2>&1 &
sleep 3
timeout 74 python -u payments.py --tps 1500 --subscriptions 20000 --resend-pct 1 --peak 2 --peak-at 12 --peak-for 10 \
  > payments.log 2>&1 &
wait || true
echo "--- payments.log"; head -n 2 payments.log; tail -n 2 payments.log
echo "--- meter.log";    tail -n 4 meter.log
echo "--- killbill.log"; grep -E "down for|outage over" killbill.log | sed -n '1p;$p' || true; tail -n 3 killbill.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" meter.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[meter\] +[1-9][0-9,]* usage msgs/s' meter.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
allm = [l for l in open("meter.log") if re.search(r"\[meter\] +[1-9][0-9,]* usage msgs/s", l)]
p = allm[10:]
print("steady-state medians: usage msgs/s %.0f  payment-to-usage p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) usage msgs/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", allm)
if len(lane) > 45:                          # the meter's view while Kill Bill was down (about s 25-37 of its lines)
    print("meter p99, median per second: before the outage %.1f ms (s 12-22), while Kill Bill was down %.1f ms (s 27-37)" % (
        st.median(lane[12:23]), st.median(lane[27:38])))
u = [l for l in open("meter.log") if l.startswith("[usage]")]
if not u:
    raise SystemExit("no usage lines from the meter")
m = re.search(r"subscriptions ([0-9,]+)  reconciled to the paisa ([0-9,]+)/([0-9,]+)  retries absorbed ([0-9,]+)  "
              r"without dedupe \+([0-9,]+) txns \(\+₹([0-9,]+), ([0-9.]+)%\)", u[-1])
if not m:
    raise SystemExit("could not read the last usage line")
subs, rec, seen, absorbed, over_n, over_rs, over_pct = (float(x.replace(",", "")) for x in m.groups())
print(u[-1].strip())
print("subscriptions reconciled to the paisa: %d of %d; retries absorbed: %d; a meter without dedupe would have "
      "counted +%d payments, +Rs %d (+%.2f%% of volume)" % (rec, seen, absorbed, over_n, over_rs, over_pct))
if seen < 1000 or rec != seen:
    raise SystemExit("subscriptions did not reconcile")
if absorbed < 100 or over_n <= 0:
    raise SystemExit("retries were not seen and absorbed")
d = nums(r"daily totals sent ([0-9,]+)", p)
if not d or st.median(d) == 0:
    raise SystemExit("no day totals were published")
print("day totals published: median %d a second" % st.median(d))
k = open("killbill.log").read().splitlines()
down = [l for l in k if "Kill Bill down for" in l]
over = [l for l in k if "outage over" in l]
if not down or not over:
    raise SystemExit("the Kill Bill outage was not seen and drained")
print("Kill Bill down for %d s of log lines; %s" % (len(down), over[-1].split("] ", 1)[1]))
last = [l for l in k if "subscriptions reconciled" in l][-1]
m = re.search(r"duplicates refused ([0-9,]+)  subscriptions reconciled ([0-9,]+)/([0-9,]+)", last)
ref, kok, kall = (int(x.replace(",", "")) for x in m.groups())
print("Kill Bill at the end: %d of %d subscriptions equal to their day totals, %d duplicates refused" % (kok, kall, ref))
if "live:" not in last or kall < 1000 or kok != kall or ref < 100:
    raise SystemExit("Kill Bill did not end live and reconciled")
lv = nums(r"usage-to-Kill-Bill p50 +[0-9.]+ms  p99 +([0-9.]+)ms", [l for l in k if "live:" in l])
if lv:
    print("usage-to-Kill-Bill p99 when live, median per second: %.1f ms (stand-in Kill Bill, 50 ms batches)" % st.median(lv))
PY
echo "Kit OK on Apache Iggy 0.9.0"
