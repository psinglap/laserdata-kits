#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Currents connector lane at small scale for about a minute:
# 40 apps with 68 connectors on their own topics, two connector shards, the event firehose next door, and one
# partner outage: app-0002-storage stops reading for 15 s, then drains from its own offset at a 1,500 events/s
# partner rate limit. Pass only if each shard holds the 100 ms p99 budget in 95% of its steady-state seconds,
# only app-0002-storage is ever down or at its limit, it catches up afterwards, and no event is lost.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py router.py connector.py firehose.py kafka_bridge.py
N=40
# Two connector shards start first and wait for the connector topics; router.py creates them.
# app-0002-storage (shard 0) loses its partner from its second 25 to 40.
for s in 0 1; do
  timeout 85 python -u connector.py --shard $s --shards 2 --apps $N --rate-limit 1500 \
    --outage app-0002-storage --outage-at 25 --outage-for 15 > conn-$s.log 2>&1 &
done
sleep 2
timeout 81 python -u router.py --apps $N --rate 3000 --users 400000 > router.log 2>&1 &
timeout 79 python -u firehose.py --mbps 4 --apps $N --partitions 8 > firehose.log 2>&1 &
wait || true
echo "--- router.log";   head -n 2 router.log; tail -n 2 router.log
echo "--- firehose.log"; tail -n 2 firehose.log
for s in 0 1; do echo "--- conn-$s.log"; head -n 3 conn-$s.log; tail -n 6 conn-$s.log; done
echo "--- outage (first, last), rate limit (first, last) and catch-up"
(grep "partner down" conn-0.log | sed -n '1p;$p'; grep "at partner rate limit" conn-0.log | sed -n '1p;$p'
 grep "caught up:" conn-*.log) || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" conn-*.log | cut -c1-150 || true
status=0
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for s in 0 1; do
  steady=$(grep -E "\[conn-$s\] +[1-9][0-9,]* events/s exported" conn-$s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "conn-$s steady-state seconds with live events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "partner down: app-0002-storage" conn-0.log || { echo "the partner outage never happened"; exit 1; }
if grep -E "partner down|at partner rate limit" conn-*.log | grep -vq "app-0002-storage"; then
  echo "a connector other than app-0002-storage was down or at its limit"; exit 1; fi
grep -q "app-0002-storage caught up: .*, 0 events lost" conn-0.log || { echo "app-0002-storage did not catch up cleanly"; exit 1; }
if grep -E "events/s exported" conn-*.log | grep -vq " lost 0 "; then echo "events were lost"; exit 1; fi
python - <<'PY'
import re, statistics as st
row = r"\[conn-[01]\] +([0-9,]+) events/s exported"
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for s in (0, 1):
    t = [l for l in open(f"conn-{s}.log") if re.search(row, l) and not re.search(r"\] +0 events", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("conn-%d steady-state medians over %d s: events/s %.0f  event-to-connector p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms" % (
          s, len(t), st.median(num(row, t)), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99),
          st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
lines = open("conn-0.log").read().splitlines()
rows = []
for l in lines:                                   # tag each per-second row: was app-0002-storage in trouble that second?
    if ("partner down: app-0002" in l or "rate limit: app-0002" in l) and rows:
        rows[-1] = (rows[-1][0], True)
    else:
        m = re.search(r"\[conn-0\] +[1-9][0-9,]* events/s exported.*p99 +([0-9.]+)ms", l)
        if m:
            rows.append((float(m.group(1)), False))
rows = rows[10:]
calm = [p for p, h in rows if not h]
busy = [p for p, h in rows if h]
if calm and busy:
    print("conn-0 p99 for the other connectors, median per second: app-0002-storage healthy %.1f ms (%d s), "
          "app-0002-storage down or draining %.1f ms (%d s)" % (st.median(calm), len(calm), st.median(busy), len(busy)))
down = [l for l in lines if "partner down: app-0002" in l]
lim = [l for l in lines if "rate limit: app-0002" in l]
back = num(r"backlog ([0-9,]+)", down + lim)
waits = num(r"oldest waiting +([0-9.]+) s", lim)
print("app-0002-storage outage: down %d s, drained at its limit %d s, largest backlog %s on its own topic, longest wait %s; %s" % (
    len(down), len(lim), f"{max(back):,.0f}" if back else "n/a", f"{max(waits):.1f} s" if waits else "n/a",
    next((l.split("] ", 1)[1] for l in lines if "caught up:" in l), "no catch-up line")))
PY
grep -Eq "\[firehose\] [1-9]" firehose.log || { echo "the firehose did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
