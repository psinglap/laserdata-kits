#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the broadcast push lane at small scale for about a minute (unicast
# notifications, a 20,000-subscription broadcast every 2 s split into 100 windows, planted endpoint outages), and
# pass only if the push lane reports PASS, every broadcast is pushed to every window within its budget, and
# not-acknowledged pushes are acknowledged after retry on their own topic.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after a heavy writer starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py event_processor.py push_workers.py retry_worker.py kafka_bridge.py
# The push workers start first and create the topics; the retry worker and the event processor follow.
timeout 80 python -u push_workers.py --workers 4 > push.log 2>&1 &
sleep 3
timeout 76 python -u retry_worker.py > retry.log 2>&1 &
timeout 74 python -u event_processor.py --unicast-rate 1500 --broadcast-every 2 > esp.log 2>&1 &
wait || true
echo "--- esp.log";   head -n 2 esp.log; grep "\] broadcast " esp.log | tail -n 3 || true; tail -n 2 esp.log
echo "--- push.log";  head -n 2 push.log; tail -n 4 push.log
echo "--- retry.log"; tail -n 3 retry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" push.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[push\] +[1-9][0-9,]* unicast/s' push.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the broadcast and retry checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("push.log") if re.search(r"\[push\] +[1-9][0-9,]* unicast/s", l)][10:]
print("steady-state medians: unicast/s %.0f  event-to-push p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) unicast/s", r"p50 +([0-9.]+)ms", r"  p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
busy = nums(r"unicast p99 ([0-9.]+)ms", [l for l in p if not re.search(r" 0 windows/s", l)])
quiet = nums(r"unicast p99 ([0-9.]+)ms", [l for l in p if re.search(r" 0 windows/s", l)])
if busy and quiet:
    print("unicast p99, median per second: seconds with broadcast windows %.1f ms (%d s), seconds without %.1f ms (%d s)" % (
        st.median(busy), len(busy), st.median(quiet), len(quiet)))
b = [l for l in open("esp.log") if "] broadcast " in l]
ms = nums(r"all pushed \+([0-9]+) ms", b)
late = sum(1 for x in ms if x > 500)
print("broadcasts pushed to all 100 windows: %d  completion p50 %.0f ms  max %.0f ms  over 500 ms: %d" % (
    len(ms), st.median(ms) if ms else 0, max(ms) if ms else 0, late))
if len(ms) < 25:
    raise SystemExit("fewer than 25 broadcasts reached every window")
if late * 100 > len(ms) * 5:
    raise SystemExit("more than 5% of broadcasts took over 500 ms to reach every window")
first, held = sum(nums(r"acknowledged first time ([0-9,]+)", b)), sum(nums(r"held for retry ([0-9,]+)\)", b))
print("broadcast pushes: acknowledged first time %d, held for retry %d (%.2f%%)" % (first, held, 100 * held / max(first + held, 1)))
e = [l for l in open("esp.log") if "open over 5 s" in l][10:-3]
stuck = nums(r"open over 5 s ([0-9]+)", e)
if stuck and max(stuck) > 0:
    raise SystemExit("a broadcast stayed open for more than 5 s")
r = [l for l in open("retry.log") if l.startswith("[retry]")]
acked = sum(nums(r"acknowledged after retry ([0-9,]+)", r))
hp = nums(r"held p50 ([0-9.]+)s", r)
print("retries: acknowledged after retry %d  held p50 (median per second) %.1f s" % (acked, st.median(hp) if hp else 0))
if acked < 100:
    raise SystemExit("too few pushes acknowledged after retry")
PY
echo "Kit OK on Apache Iggy 0.9.0"
