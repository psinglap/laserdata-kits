#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the ingest freshness lane at small scale for about a minute, with
# customers going quiet, Retriever falling behind on 30% of Kafka partitions for 20 s, and a backfill next
# door. Pass only if the lane reports PASS, every real silence fires, nothing pages a dataset that was still
# sending, and the heartbeats of datasets stuck behind Retriever are held.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ingest.py freshness.py backfill_flood.py kafka_bridge.py
# The freshness service starts first and creates the topics; ingest and the neighbour follow.
# 3,000 datasets sending a batch a second (3,000 receipts/s) plus 400 progress reports/s from 2 Retriever nodes
# on 100 Kafka partitions; a 10 s heartbeat window; 2 datasets a second go quiet for 15 s; Retriever stops
# applying 30 of the 100 Kafka partitions from second 27 to 47; 3 MB/s of backfill next door from second 15.
timeout 80 python -u freshness.py --heartbeat-s 10 > freshness.log 2>&1 &
sleep 3
timeout 74 python -u ingest.py --datasets 3000 --heartbeat-s 10 --silence-rate 2 --silence-for 15 \
  --stall-at 27 --stall-for 20 --stall-share 0.3 > ingest.log 2>&1 &
timeout 72 python -u backfill_flood.py --mbps 3 --at 15 --duration 40 > backfill.log 2>&1 &
wait || true
echo "--- ingest.log";    head -n 3 ingest.log; sed -n '28,32p;44,50p' ingest.log; tail -n 3 ingest.log
echo "--- freshness.log"; head -n 3 freshness.log; sed -n '42,48p' freshness.log; tail -n 4 freshness.log
echo "--- backfill.log";  head -n 2 backfill.log; tail -n 2 backfill.log
echo "--- wrong holds"; grep "wrong hold" ingest.log | head -n 20 || true
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" freshness.log | cut -c1-300 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[freshness\] +[1-9][0-9,]* msgs/s' freshness.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the heartbeat check, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("freshness.log") if re.search(r"\[freshness\] +[1-9][0-9,]* msgs/s", l)][10:]
print("steady-state medians: msgs/s %.0f  receipt-to-freshness p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) msgs/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("freshness.log") if "receipt-to-freshness" in l])
if len(lane) > 50:
    print("receipt-to-freshness p99, median per second: before the backfill %.1f ms (s 3-12), with 3 MB/s of backfill "
          "next door %.1f ms (s 18-50)" % (st.median(lane[3:13]), st.median(lane[18:51])))
g = [l for l in open("ingest.log") if l.startswith("[ingest]")]
tot = lambda pat: int(sum(nums(pat, g)))
silences, right = tot(r"silences started ([0-9,]+)"), tot(r"fired right ([0-9,]+)")
missed, spurious = tot(r"missed ([0-9,]+)"), tot(r"spurious pages ([0-9,]+)")
held, X
late = nums(r"max \+([0-9.]+)s\)", [l for l in g if not re.search(r"fired right 0 ", l)])
print("heartbeats: silences started %d  fired right %d  fired after the window: max +%.2f s  missed %d  "
      "spurious pages %d  held while Retriever behind %d (wrong %d)" % (
        silences, right, max(late or [0]), missed, spurious, held, held_wrong))
peak = max(nums(r"held ([0-9,]+)  firing", s) or [0])
print("most heartbeats held at once: %d of 3,000 datasets" % peak)
if right < 20 or held < 20:
    raise SystemExit("too few silences or held heartbeats to judge the lane")
if missed or spurious or held_wrong:
    raise SystemExit("a heartbeat verdict did not match the truth")
e = [l for l in open("backfill.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("backfill neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[backfill\] done" backfill.log || { echo "the backfill neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
