#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the candle lane at small scale for about 80 s:
# the price capture, a candle builder from the start, an order-event burst next door, and a DR
# builder that starts mid-session and rebuilds the open candle from the log. Candles are 15 s here
# (1 minute by default) so several close inside one CI run; the math is the same.
# Pass only if each builder holds the 25 ms tick-to-candle p99 budget in 95% of its steady-state
# seconds with no sequence gaps and no late ticks, and both builders close identical candles.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which stalls every reader on a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py capture.py candles.py orders.py kafka_bridge.py
N=8000
timeout 92 python -u capture.py --scrips $N --rate 1000 > capture.log 2>&1 &
sleep 3
timeout 82 python -u orders.py --rate 300 --x 10 --at 40 --for 15 > orders.log 2>&1 &
timeout 82 python -u candles.py --name candles-mumbai --tf-s 15 --replay first > candles-mumbai.log 2>&1 &
# The DR builder starts 30 s in, replays from the start of the open candle, then runs live beside the first.
sleep 30
timeout 50 python -u candles.py --name candles-dr --tf-s 15 --replay period > candles-dr.log 2>&1 || true
wait || true
echo "--- capture.log"; head -n 1 capture.log; tail -n 2 capture.log
echo "--- orders.log";  grep -m 1 burst orders.log || true; tail -n 1 orders.log
for f in candles-mumbai candles-dr; do
  echo "--- $f.log"; grep -E "rebuilt|closed" $f.log | head -n 8 || true; tail -n 3 $f.log
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" candles-*.log | cut -c1-170 || true
status=0
# Skip each builder's first 10 live seconds (warm-up); a shared CI runner may blip, so require 95% of the
# remaining seconds to pass, at least 30 seconds for the first builder and 25 for the DR builder.
for b in candles-mumbai:30 candles-dr:25; do
  f=${b%%:*}.log; need=${b##*:}
  s=$(grep -E '\] +[1-9][0-9,]* ticks/s' $f | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$f steady-state seconds with live ticks: PASS $live, FAIL $fail"
  if [ "$live" -lt "$need" ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
  last=$(grep -E 'late [0-9]+  gaps [0-9]+' $f | tail -n 1)
  late=$(grep -Eo 'late [0-9]+' <<<"$last" | cut -d' ' -f2); gaps=$(grep -Eo 'gaps [0-9]+' <<<"$last" | cut -d' ' -f2)
  echo "$f late ticks: ${late:-none}  sequence gaps: ${gaps:-none}"
  if ! ( [ "${late:-1}" = "0" ] && [ "${gaps:-1}" = "0" ] ); then status=1; fi
done
[ "$status" -eq 0 ] || { echo "candle lane check failed"; exit 1; }
grep -q "rebuilt the open candles" candles-dr.log || { echo "the DR builder did not catch up"; exit 1; }
grep -q "on the chart within 1 s" candles-mumbai.log || { echo "no business metric"; exit 1; }
python - <<'PY'
import re, statistics as st, sys
close = re.compile(r"candle (\S+) closed: ([0-9,]+) scrips, digest (\w+), final ([0-9.-]+) ms")
def closed(f):
    out = {}
    for l in open(f):
        m = close.search(l)
        if m:
            out[m.group(1)] = (m.group(3), float(m.group(4)), m.group(2))
    return out
a, b = closed("candles-mumbai.log"), closed("candles-dr.log")
both = sorted(set(a) & set(b))
same = [k for k in both if a[k][0] == b[k][0]]
print("candles closed by both builders: %d, identical: %d (%s)" % (len(both), len(same),
      ", ".join("%s %s scrips" % (k, a[k][2]) for k in both)))
live = re.compile(r"\] +([0-9,]+) ticks/s")
for name in ("candles-mumbai", "candles-dr"):
    rows = [l for l in open(f"{name}.log") if live.search(l) and not re.search(r"\] +0 ticks/s", l)][10:]
    def num(pat):
        out = []
        for l in rows:
            m = re.search(pat, l)
            if m:
                out.append(float(m.group(1).replace(",", "")))
        return out
    p99 = num(r"p99 +([0-9.]+)ms")
    if p99:
        print("%s steady-state medians over %d s: ticks/s %.0f  tick-to-candle p50 %.2f ms  p95 %.2f ms  p99 %.2f ms  "
              "p99.9 %.2f ms  worst-second p99 %.2f ms  on the chart within 1 s %.3f%%  store upserts/s %.0f" % (
              name, len(rows), st.median(num(live.pattern)), st.median(num(r"p50 +([0-9.]+)ms")),
              st.median(num(r"p95 +([0-9.]+)ms")), st.median(p99), st.median(num(r"p99\.9 +([0-9.]+)ms")), max(p99),
              st.median(num(r"within 1 s ([0-9.]+)%")), st.median(num(r"store upserts ([0-9,]+)/s"))))
fin = [v[1] for k, v in a.items()]
if fin:
    print("candles-mumbai: %d candles closed, final %.1f to %.1f ms after the candle ended (median %.1f ms)" % (
        len(fin), min(fin), max(fin), st.median(fin)))
rows = [float(m.group(1)) for l in open("candles-mumbai.log") for m in [re.search(r"p99 +([0-9.]+)ms", l)] if m]
if len(rows) > 60:   # the order-event burst runs from its second 40 to 55, about the builder's seconds 37 to 52
    print("candles-mumbai p99, median per second: before the order burst %.2f ms (s 18-35), during it %.2f ms (s 39-51)" % (
        st.median(rows[18:36]), st.median(rows[39:52])))
if len(both) < 2 or len(same) != len(both):
    print("the DR builder's candles differ from the first builder's"); sys.exit(1)
PY
echo "Kit OK on Apache Iggy 0.9.0"
