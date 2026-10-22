#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the link health alert loop at small scale for about a minute, with a
# slow consumer on the lane and log shipping next to it, and pass only if the lane reports PASS, the rules send
# degraded and recovered alerts, and degraded alerts reach the console while the link is still degraded.
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
python -m py_compile common.py link_rules.py links.py slow_reader.py log_flood.py redpanda_bridge.py
# The rules start first and create the topics; the links and the neighbours follow.
# 50 links, a sample every 20 ms each (about 2,500 samples/s), a fade every 20 s per link, weather fades up to 8 s
# (shortened so fades keep starting in a one-minute run);
# a slow reader on the lane at 1,000 samples/s from second 12, and 2 MB/s of log shipping from second 20.
timeout 78 python -u link_rules.py > rules.log 2>&1 &
sleep 3
timeout 72 python -u links.py --links 50 --sample-ms 20 --fade-every-s 20 --long-max-s 8 --consumers 3 > links.log 2>&1 &
timeout 70 python -u slow_reader.py --rate 1000 --at 12 --duration 55 > slow.log 2>&1 &
timeout 70 python -u log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- links.log";  head -n 3 links.log; tail -n 4 links.log
echo "--- rules.log";  tail -n 6 rules.log
echo "--- slow.log";   head -n 2 slow.log; tail -n 3 slow.log
echo "--- logs.log";   head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" rules.log | cut -c1-400 || true
echo "--- links stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" links.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[link-rules\] +[1-9][0-9,]* samples/s' rules.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the alert checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
r = [l for l in open("rules.log") if re.search(r"\[link-rules\] +[1-9][0-9,]* samples/s", l)][10:]
print("steady-state medians: samples/s %.0f  sample-to-rules p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) samples/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("rules.log") if "sample-to-rules" in l])
if len(lane) > 50:
    print("sample-to-rules p99, median per second: alone %.1f ms (s 3-11), with the slow reader %.1f ms (s 13-19), "
          "with the slow reader and 2 MB/s of logs %.1f ms (s 23-50)" % (
        st.median(lane[3:12]), st.median(lane[13:20]), st.median(lane[23:51])))
sent = sum(nums(r"alerts sent ([0-9,]+)", r))
deg = sum(nums(r"degraded ([0-9,]+),", r))
rec = sum(nums(r"recovered ([0-9,]+)\)", r))
print("alerts sent in steady state: %d  degraded: %d  recovered: %d" % (sent, deg, rec))
if sent < 100 or deg < 50 or rec < 50:
    raise SystemExit("the rules did not send enough degraded and recovered alerts")
f = [l for l in open("links.log") if re.search(r"\[links\] +[1-9][0-9,]* samples/s", l)][10:]
alerts = sum(nums(r"at the console ([0-9,]+),", f))
still = sum(nums(r"still degraded ([0-9,]+) \(", f))
short = sum(nums(r"short bursts ([0-9,]+);", f))
slow = sum(nums(r"it would be ([0-9,]+)\)", f))
print("degraded alerts at the console: %d (short bursts %d)  while the link was still degraded: %d (%.2f%%)  "
      "at 150 ms: %d (%.2f%%)" % (alerts, short, still, 100 * still / max(alerts, 1), slow, 100 * slow / max(alerts, 1)))
if alerts < 50:
    raise SystemExit("too few degraded alerts to judge the loop")
d = [l for l in f if re.search(r"fade-to-consumers n [1-9]", l)]
if not d:
    raise SystemExit("no alerts reached the consumers")
print("fade-to-consumers (first bad sample to 3 consumers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
    st.median(nums(r"consumers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if still * 100 < alerts * 99:
    raise SystemExit("more than 1% of degraded alerts reached the console after the link had recovered")
s = [l for l in open("slow.log") if "behind the lane" in l]
if s:
    print("slow reader: %d s, median %.0f samples/s read, behind the lane by %.1f s at the end" % (
        len(s), st.median(nums(r"\] ([0-9,]+) samples/s", s)), nums(r"by ([0-9.]+) s", s)[-1]))
e = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[logs\] done" logs.log || { echo "the log neighbour did not run"; exit 1; }
grep -q "\[slow-reader\] done" slow.log || { echo "the slow reader did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
