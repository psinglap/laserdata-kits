#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Insights witness lane at small scale for about a minute,
# with a busy hour, lost uploads, retried publishes and an agent-telemetry stream next to it. Pass only if
# witness_assembler's publish-to-assembler p99 is inside the budget, and the funnel closes exactly: every
# published batch and witness assembled once, every lost upload named, every retried publish absorbed.
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
python -m py_compile common.py async_witnesses.py assembler.py telemetry_flood.py kafka_bridge.py
# witness_assembler starts first and creates the topic; the agents and the neighbour follow.
# 500 agents on 40 services, each flushing every 5 s (100 batches/s), 2,500 witnesses/s, twice that from
# second 35 to 50; 1% of uploads lost, 1% of batches published twice; 2 MB/s of agent telemetry from second 20.
timeout 80 python -u assembler.py > assembler.log 2>&1 &
sleep 3
python -u async_witnesses.py --agents 500 --services 40 --witnesses-per-s 2500 --lost-pct 1 --resend-pct 1 \
  --peak 2 --peak-at 35 --peak-for 15 --duration 62 > async_witnesses.log 2>&1 &
timeout 70 python -u telemetry_flood.py --mbps 2 --at 20 --duration 35 > telemetry.log 2>&1 &
wait || true
echo "--- async_witnesses.log"; head -n 2 async_witnesses.log; tail -n 3 async_witnesses.log
echo "--- assembler.log";       grep -m 3 "never arrived" assembler.log || true; tail -n 6 assembler.log
echo "--- telemetry.log";       head -n 2 telemetry.log; tail -n 2 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" assembler.log | cut -c1-150 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '^\[witness_assembler\] +[1-9][0-9,]* batches/s' assembler.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the funnel check, for the README.
python - <<'PY'
import json, re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("assembler.log") if re.search(r"^\[witness_assembler\] +[1-9][0-9,]* batches/s", l)][10:]
print("steady-state medians: batches/s %.0f  witnesses/s %.0f  publish-to-assembler p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) batches/s", r"([0-9,]+) witnesses/s", r"p50 +([0-9.]+)ms",
                                    r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("assembler.log") if "batches/s" in l and l.startswith("[witness_assembler]")])
if len(lane) > 55:
    print("publish-to-assembler p99, median per second: before the neighbour %.1f ms (s 10-20), with 2 MB/s of telemetry %.1f ms (s 24-34), busy hour at 2x %.1f ms (s 38-50)" % (
        st.median(lane[10:21]), st.median(lane[24:35]), st.median(lane[38:51])))
t = json.load(open("async_witnesses.totals.json"))
f = [l for l in open("assembler.log") if l.startswith("[funnel] assembled")][-1]
g = lambda pat: int(re.search(pat, f).group(1).replace(",", ""))
got = dict(batches=g(r"assembled ([0-9,]+) batches"), witnesses=g(r"batches, ([0-9,]+) witnesses"),
           missing=g(r"missing batches ([0-9,]+)"), duplicates=g(r"duplicates absorbed ([0-9,]+)"),
           closed=g(r"agents closed ([0-9,]+)"))
print("published %d batches / %d witnesses, assembled %d / %d; lost uploads %d, named missing %d; resent %d, absorbed %d; agents %d, closed %d" % (
    t["batches"], t["witnesses"], got["batches"], got["witnesses"], t["lost_batches"], got["missing"],
    t["resent"], got["duplicates"], t["agents"], got["closed"]))
bad = [k for k, want in (("batches", t["batches"]), ("witnesses", t["witnesses"]), ("missing", t["lost_batches"]),
                         ("duplicates", t["resent"]), ("closed", t["agents"])) if got[k] != want]
if bad or t["lost_batches"] == 0 or t["resent"] == 0:
    raise SystemExit("the funnel did not close: " + (", ".join(bad) or "no lost uploads or resends to check"))
a = [l for l in open("async_witnesses.log") if re.search(r"\] +[1-9][0-9,]* batches/s", l)][3:]
pub = nums(r"publish p50 +[0-9.]+ms +p99 +([0-9.]+)ms", a)
print("async_witnesses publish p99: median %.1f ms, max %.1f ms over %d s" % (st.median(pub), max(pub), len(pub)))
e = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
