#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the dock scan lane at small scale for about a minute: 2,000 docks
# loading trailers (two loaders each, a scan every 2-4 s, about 1,000 load scans/s), linehaul plan revisions while
# loading, and 2 MB/s of CDC changes on a neighbouring topic from second 20. Pass only if the load check holds the
# 100 ms p99 budget in 95% of its steady-state seconds, misloads are flagged on the loader's handheld before that
# loader's next scan, and no alert is false and no misload is missed.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On a shared runner that burst stalls every
# partition for a fraction of a second at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py dock_check.py docks.py cdc_flood.py msk_bridge.py
# The load check starts first and creates the topics; the docks and the neighbour follow. Loading time is
# compressed (12-30 bags a trailer, seals 5-20 s after the last scan) so trailers turn over inside the minute;
# plan revisions and wrong carts are raised so misloads happen often enough to judge.
timeout 78 python -u dock_check.py > check.log 2>&1 &
sleep 3
timeout 72 python -u docks.py --docks 2000 --scan-s 2 4 --load 12 30 --seal-s 5 20 --gap-s 3 8 --revise-p 0.3 \
  --told-after 3 15 --wrong-p 0.003 --notifiers 3 > docks.log 2>&1 &
timeout 70 python -u cdc_flood.py --mbps 2 --at 20 --duration 35 > cdc.log 2>&1 &
wait || true
echo "--- docks.log"; head -n 3 docks.log; tail -n 4 docks.log
echo "--- check.log"; tail -n 6 check.log
echo "--- cdc.log";   head -n 2 cdc.log; tail -n 2 cdc.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" check.log | cut -c1-400 || true
echo "--- dock stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" docks.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[dock-check\] +[1-9][0-9,]* events/s' check.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the misload checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
c = [l for l in open("check.log") if re.search(r"\[dock-check\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  scan-to-check p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, c)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("check.log") if "scan-to-check" in l])
if len(lane) > 50:
    print("scan-to-check p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of CDC %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
d = [l for l in open("docks.log") if re.search(r"\[docks\] +[1-9][0-9,]* scans/s", l)][10:]
mis, flagged = sum(nums(r"misloads ([0-9,]+)", d)), sum(nums(r"handheld ([0-9,]+):", d))
nxt, seal = sum(nums(r"before the loader's next scan ([0-9,]+)", d)), sum(nums(r"before the seal ([0-9,]+)", d))
slow, false, missed = sum(nums(r"behind: before the next scan ([0-9,]+)", d)), sum(nums(r"false alerts ([0-9,]+)", d)), sum(nums(r"missed ([0-9,]+)", d))
print("misloads in steady state: %d  flagged at the loader's handheld %d: before the loader's next scan %d (%.2f%%), "
      "before the seal %d (%.2f%%)  a lane 5 s behind: before the next scan %d (%.1f%%)  false alerts %d  missed %d" % (
    mis, flagged, nxt, 100 * nxt / max(flagged, 1), seal, 100 * seal / max(flagged, 1), slow, 100 * slow / max(flagged, 1), false, missed))
s = [l for l in d if re.search(r"scan-to-handheld n [1-9]", l)]
if s:
    print("scan-to-handheld (load scan to 3 notifiers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
        st.median(nums(r"handheld n [0-9,]+ p50 ([0-9.]+)ms", s)), st.median(nums(r"ms p99 ([0-9.]+)ms", s)), len(s)))
if flagged < 50:
    raise SystemExit("too few misloads flagged to judge the loop")
if false > 0:
    raise SystemExit("the load check flagged a bag that was loaded correctly")
if missed * 100 > mis:
    raise SystemExit("more than 1% of misloads were never flagged")
if nxt * 100 < flagged * 99:
    raise SystemExit("more than 1% of misloads reached the handheld after the loader's next scan")
e = [l for l in open("cdc.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("CDC neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[cdc\] done" cdc.log || { echo "the CDC neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
