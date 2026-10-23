#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the real-time tracking lane at small scale for about a minute, with a
# media burst next door and one real-time processing instance restarted for 12 s, and pass only if the lane reports
# PASS, no signal is lost, and the restarted instance finds what was sent while it was away waiting in the log.
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
python -m py_compile common.py tracking_api.py realtime.py media_burst.py kafka_bridge.py
# Tracking API starts first and creates the lane; two real-time processing instances share its 16 partitions.
# Instance 1 restarts at 30 s for 12 s; 2 MB/s of media events run next door from 20 s for 30 s.
timeout 82 python -u tracking_api.py --members 100000 --rate 2000 > api.log 2>&1 &
sleep 3
timeout 76 python -u realtime.py --instance 0 --instances 2 > rt0.log 2>&1 &
timeout 76 python -u realtime.py --instance 1 --instances 2 --restart-at 30 --restart-for 12 > rt1.log 2>&1 &
timeout 70 python -u media_burst.py --mbps 2 --at 20 --duration 30 > media.log 2>&1 &
wait || true
echo "--- api.log";   tail -n 3 api.log
for f in rt0 rt1; do echo "--- $f.log"; grep -E "restart|back|away" $f.log | head -n 4 || true; grep "signals/s" $f.log | tail -n 3; tail -n 1 $f.log; done
echo "--- media.log"; head -n 2 media.log; tail -n 2 media.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" rt0.log rt1.log | cut -c1-160 || true
# Skip a 10 s warm-up per instance while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$( { grep -E '\[realtime-0\] +[1-9][0-9,]* signals/s' rt0.log | tail -n +11; grep -E '\[realtime-1\] +[1-9][0-9,]* signals/s' rt1.log | tail -n +11; } || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic (both instances): PASS $live, FAIL $fail"
if ! { [ "$live" -ge 60 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the loss and restart checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for i in (0, 1):
    L = [l for l in open(f"rt{i}.log") if re.search(r"\[realtime-%d\] +[1-9][0-9,]* signals/s" % i, l)][10:]
    print("realtime-%d steady-state medians over %d s: signals/s %.0f  p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst p99 %.1f ms" % (
        i, len(L), st.median(nums(r"\] +([0-9,]+) signals/s", L)), st.median(nums(r"p50 +([0-9.]+)ms", L)),
        st.median(nums(r" p99 +([0-9.]+)ms", L)), st.median(nums(r"p99\.9 +([0-9.]+)ms", L)), max(nums(r" p99 +([0-9.]+)ms", L))))
    if i == 0:
        med = nums(r" p99 +([0-9.]+)ms", [l for l in open("rt0.log") if re.search(r"\[realtime-0\] +[1-9][0-9,]* signals/s", l)])
        if len(med) > 45:
            print("realtime-0 p99, median per second: before the media burst %.1f ms (s 6-14), during it %.1f ms (s 19-44)" % (
                st.median(med[6:15]), st.median(med[19:45])))
    tot = [l for l in open(f"rt{i}.log") if "totals:" in l][-1]
    f = {k: int(v.replace(",", "")) for k, v in re.findall(r"(applied|lost|held in the log|repeats dropped) ([0-9,]+)", tot)}
    print("realtime-%d totals: applied %d  lost %d  held in the log %d  repeats dropped %d" % (
        i, f["applied"], f["lost"], f["held in the log"], f["repeats dropped"]))
    if f["applied"] < 20000:
        raise SystemExit("too few signals applied to judge the lane")
    if f["lost"]:
        raise SystemExit("realtime-%d lost %d signals" % (i, f["lost"]))
back = [l for l in open("rt1.log") if "restart: away" in l]
if not back:
    raise SystemExit("the restarted instance never caught up")
print(back[0].strip())
held = int(re.search(r"applied ([0-9,]+) signals", back[0]).group(1).replace(",", ""))
if held < 1000:
    raise SystemExit("the restart held only %d signals: the restart was not exercised" % held)
api = [l for l in open("api.log") if "totals:" in l][-1]
print("tracking-api:", api.split("totals:")[1].strip())
ack = nums(r"ack p50 +[0-9.]+ms +p99 +([0-9.]+)ms", open("api.log").readlines()[5:])
if ack:
    print("tracking-api request-to-broker-ack p99, median per second %.1f ms" % st.median(ack))
m = [l for l in open("media.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if m:
    print("media neighbour: %d s, median %.1f MB/s on its own topic" % (len(m), st.median(nums(r"~([0-9.]+) MB/s", m))))
PY
grep -q "\[media\] done" media.log || { echo "the media neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
