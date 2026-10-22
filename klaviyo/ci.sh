#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the failed-writes lane at small scale for about a minute:
# a publisher at 6,000 events/s with 1% of publishes failing, then 50% failing for 30 s (Klaviyo's "half of all
# events" surge, shortened), two retrier shards re-publishing from failed-writes, and a bulk S3 replay next door.
# Pass only if each retrier holds the 100 ms capture-to-retry p99 in 95% of its steady-state seconds, P10 events
# are re-published inside their 5 s SLO, no event is lost, and every captured event is re-published or in the DLQ.
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
python -m py_compile common.py publisher.py retrier.py s3_replay.py kafka_bridge.py
# The retriers start first and wait for failed-writes; the publisher creates it. They outlive the publisher
# by 8 s so the last retries and DLQ writes finish.
for s in 0 1; do
  timeout 88 python -u retrier.py --shard $s --shards 2 > retrier-$s.log 2>&1 &
done
sleep 2
timeout 85 python -u publisher.py --duration 76 --rate 6000 --blip-share 0.01 --outage-at 25 --outage-for 30 --outage-share 0.5 \
  --reject-share 0.002 --partitions 8 --profiles 2000000 > publisher.log 2>&1 &
timeout 78 python -u s3_replay.py --mbps 4 > replay.log 2>&1 &
wait || true
echo "--- publisher.log"; head -n 2 publisher.log; sed -n '28,30p' publisher.log; tail -n 2 publisher.log
echo "--- replay.log"; tail -n 2 replay.log
for s in 0 1; do echo "--- retrier-$s.log"; head -n 3 retrier-$s.log; tail -n 2 retrier-$s.log; done
echo "--- a blip second and a surge second on each retrier"
for s in 0 1; do
  (grep -E "\] +[1-9][0-9]? failed writes/s" retrier-$s.log | sed -n '15p'
   grep -E "\] +[1-9],[0-9]{3} failed writes/s" retrier-$s.log | sed -n '15p') || true
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" retrier-*.log | cut -c1-170 || true
status=0
# Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of seconds to pass.
for s in 0 1; do
  steady=$(grep -E "\[retrier-$s\] +[1-9][0-9,]* failed writes/s" retrier-$s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "retrier-$s steady-state seconds with live failed writes: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "MSK timeouts 50%" publisher.log || { echo "the surge never happened"; exit 1; }
if grep -E "failed writes/s picked up" retrier-*.log | grep -vq " lost 0 "; then echo "events were lost"; exit 1; fi
python - <<'PY'
import re, statistics as st, sys
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
pub = open("publisher.log").read().splitlines()
cap = num(r"total captured ([0-9,]+)", pub)
rej = num(r"rejected payloads ([0-9,]+)", pub)
ok = dlq = 0
for s in (0, 1):
    lines = [l for l in open(f"retrier-{s}.log") if "failed writes/s picked up" in l]
    t = [l for l in lines if not re.search(r"\] +0 failed", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if p99:
        surge = [p for p, n in zip(p99, num(r"\] +([0-9,]+) failed", t)) if n >= 500]
        calm = [p for p, n in zip(p99, num(r"\] +([0-9,]+) failed", t)) if n < 500]
        print("retrier-%d steady-state medians over %d s: capture-to-retry p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst-second p99 %.1f ms; p99 in surge seconds %s (%d s), otherwise %s (%d s)" % (
              s, len(t), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)),
              max(p99), f"{st.median(surge):.1f} ms" if surge else "n/a", len(surge),
              f"{st.median(calm):.1f} ms" if calm else "n/a", len(calm)))
    last = lines[-1] if lines else ""
    m = re.search(r"total re-published ([0-9,]+) to DLQ ([0-9,]+)", last)
    if m:
        ok += int(m.group(1).replace(",", ""))
        dlq += int(m.group(2).replace(",", ""))
    p10 = re.search(r"on time P10 +([0-9.]+)%", last)
    print(f"retrier-{s} final: {last.split('re-published', 1)[-1].strip()[:200] if last else 'no lines'}")
    if not p10 or float(p10.group(1)) < 99.9:
        print("P10 events missed their 5 s SLO"); sys.exit(1)
ack = num(r"capture ack p50 +[0-9.]+ms +p99 +([0-9.]+)ms", pub[10:])
peak = max(num(r"MSK timeouts 50%: +([0-9,]+) failed", pub) or [0])
print("publisher: capture ack p99 median %.1f ms (worst second %.1f ms); peak %d failed writes/s captured" % (
      st.median(ack) if ack else float("nan"), max(ack) if ack else float("nan"), peak))
captured, rejected = (int(cap[-1]), int(rej[-1])) if cap else (0, 0)
print(f"captured {captured:,} failed writes ({rejected:,} with payloads MSK rejects); re-published {ok:,}, to DLQ {dlq:,}")
if not captured or ok + dlq != captured or dlq < rejected:
    print("not every captured event was re-published or sent to the DLQ"); sys.exit(1)
PY
grep -Eq "\[s3-replay\] [1-9]" replay.log || { echo "the S3 replay did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
