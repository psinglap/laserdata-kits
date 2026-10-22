#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Forge usage alerts lane at small scale for about a minute,
# with a product-analytics stream next to it, and pass only if the usage lane reports PASS, retries are
# absorbed rather than counted, apps cross their allowance levels, and the alerts reach the alert service.
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
python -m py_compile common.py runtime.py tracker.py notifier.py analytics_flood.py sqs_bridge.py
# The tracker starts first and creates the topics; the alert service, the runtime fleet and the neighbour follow.
# 3,000 invocations a second over 1,000 apps on 4 runtime hosts, 20 s cycles, 1% of records sent twice;
# 2 MB/s of product analytics events from second 20.
timeout 78 python -u tracker.py --apps 1000 --invocations-per-s 3000 --cycle-s 20 > tracker.log 2>&1 &
sleep 3
timeout 74 python -u notifier.py > alerts.log 2>&1 &
timeout 72 python -u runtime.py --hosts 4 --apps 1000 --invocations-per-s 3000 --cycle-s 20 --resend-pct 1 > runtime.log 2>&1 &
timeout 70 python -u analytics_flood.py --mbps 2 --at 20 --duration 35 > analytics.log 2>&1 &
wait || true
echo "--- runtime.log";   head -n 2 runtime.log; tail -n 3 runtime.log
echo "--- tracker.log";   tail -n 6 tracker.log
echo "--- alerts.log";    tail -n 4 alerts.log
echo "--- analytics.log"; head -n 2 analytics.log; tail -n 2 analytics.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" tracker.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[tracker\] +[1-9][0-9,]* usage events/s' tracker.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the allowance checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("tracker.log") if re.search(r"\[tracker\] +[1-9][0-9,]* usage events/s", l)][10:]
print("steady-state medians: usage events/s %.0f  usage-to-tracker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) usage events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("tracker.log") if "usage events/s" in l])
if len(lane) > 50:
    print("usage-to-tracker p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of analytics events %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
u = [l for l in open("tracker.log") if l.startswith("[usage]")]
cycles = {}
for l in u:
    m = re.search(r"cycle (\d+): .* past 50% ([0-9,]+) +75% ([0-9,]+) +90% ([0-9,]+) +100% ([0-9,]+) +budget ([0-9,]+)/", l)
    if m:
        cycles[m.group(1)] = tuple(int(x.replace(",", "")) for x in m.groups()[1:])
for k, v in list(cycles.items())[1:-1]:      # cycles seen from start to end
    print("cycle %s: apps past 50%% %d, 75%% %d, 90%% %d, 100%% %d, budget %d" % ((k,) + v))
full = [v[3] for v in list(cycles.values())[1:-1]]
if not full or max(full) < 50:
    raise SystemExit("too few apps crossed 100% of their allowance in a full cycle")
absorbed = max(nums(r"retries absorbed ([0-9,]+)", u) or [0])
extra = nums(r"without dedupe \+[0-9,.]+ GB-s \(([0-9.]+)%\)", u)
early = nums(r"early alerts ([0-9,]+)", u)
print("retries absorbed: %d  without dedupe: +%.2f%% GB-s  most early alerts a no-dedupe counter would send in a cycle: %d" % (
    absorbed, extra[-1] if extra else 0, max(early or [0])))
if absorbed == 0:
    raise SystemExit("no retries were absorbed")
d = [l for l in open("alerts.log") if re.search(r"usage-to-alert n [1-9]", l)][5:]
if not d:
    raise SystemExit("no alerts reached the alert service")
print("usage-to-alert: p50 %.1f ms  p99 %.1f ms   tracker-to-alert: p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
    st.median(nums(r"usage-to-alert n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"usage-to-alert n [0-9,]+ p50 [0-9.]+ms p99 ([0-9.]+)ms", d)),
    st.median(nums(r"tracker-to-alert p50 ([0-9.]+)ms", d)), st.median(nums(r"tracker-to-alert p50 [0-9.]+ms p99 ([0-9.]+)ms", d)), len(d)))
e = [l for l in open("analytics.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("analytics neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
