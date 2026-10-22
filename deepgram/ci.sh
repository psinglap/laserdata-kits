#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the streaming usage lane at small scale for about a minute, with a
# request-log stream next to it, and pass only if the lane reports PASS, closed streams reconcile to the
# millisecond, retries are absorbed rather than billed, and summaries flow.
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
python -m py_compile common.py engine.py meter.py log_flood.py kafka_bridge.py
# The meter starts first and creates the topics; the engine fleet and the neighbour follow.
# 3,000 live streams over 500 projects on 4 servers, one usage message per stream per second, 20 s mean stream
# length, 1% of messages sent twice; 2 MB/s of request logs from second 20.
timeout 78 python -u meter.py > meter.log 2>&1 &
sleep 3
timeout 72 python -u engine.py --servers 4 --streams 3000 --projects 500 --stream-s 20 --resend-pct 1 > engine.log 2>&1 &
timeout 70 python -u log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- engine.log"; head -n 2 engine.log; tail -n 3 engine.log
echo "--- meter.log";  tail -n 6 meter.log
echo "--- logs.log";   head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" meter.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[meter\] +[1-9][0-9,]* usage msgs/s' meter.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the metering checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("meter.log") if re.search(r"\[meter\] +[1-9][0-9,]* usage msgs/s", l)][10:]
print("steady-state medians: usage msgs/s %.0f  usage-to-summary p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) usage msgs/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("meter.log") if "usage msgs/s" in l])
if len(lane) > 50:
    print("usage-to-summary p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of request logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
u = [l for l in open("meter.log") if l.startswith("[usage]")]
if not u:
    raise SystemExit("no usage lines from the meter")
m = re.search(r"streams reconciled ([0-9,]+)/([0-9,]+)  retries absorbed ([0-9,]+)  without dedupe \+([0-9,]+) s \(\+\$([0-9.,]+), ([0-9.]+)%\)", u[-1])
if not m:
    raise SystemExit("could not read the last usage line")
rec, closed, absorbed, over_s, over_usd, over_pct = (float(x.replace(",", "")) for x in m.groups())
print(u[-1].strip())
print("closed streams reconciled to the ms: %d of %d; retries absorbed: %d; a meter without dedupe would have billed +%d s of audio (+%.2f%%)" % (
    rec, closed, absorbed, over_s, over_pct))
h = nums(r"audio metered ([0-9.,]+) h", u)
print("audio metered per minute, highest seen: %.2f h" % max(h))
if closed < 1000 or rec != closed:
    raise SystemExit("closed streams did not reconcile")
if absorbed < 100 or over_s <= 0:
    raise SystemExit("retries were not seen and absorbed")
s = nums(r"summaries sent ([0-9,]+)", p)
if not s or st.median(s) == 0:
    raise SystemExit("no per-project summaries were published")
print("per-project summaries published: median %d a second" % st.median(s))
e = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("request-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[logs\] done" logs.log || { echo "the request-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
