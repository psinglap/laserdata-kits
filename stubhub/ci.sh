#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the listing lane at small scale for about a minute, with an onsale
# burst and a clickstream flood next to it, and pass only if the event pages report PASS, seat picks flow end to
# end, and the lagged-view comparison is measured.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py listings.py event_pages.py clickstream_flood.py shadow_bridge.py
# 200 events x 250 listings, 1,500 changes/s, a 2x onsale (3,000/s) from second 30 to 45,
# 150 seat picks/s, and 2 MB/s of clickstream from second 20 for 35 s.
timeout 80 python -u listings.py --events 200 --listings 250 --rate 1500 --onsale 2 --onsale-at 30 --onsale-for 15 > listings.log 2>&1 &
sleep 5
timeout 70 python -u clickstream_flood.py --mbps 2 --at 20 --duration 35 > clicks.log 2>&1 &
timeout 70 python -u event_pages.py --picks-per-s 150 > pages.log 2>&1 || true
wait || true
echo "--- listings.log"; head -n 1 listings.log; tail -n 4 listings.log
echo "--- pages.log";    head -n 3 pages.log; tail -n 6 pages.log
echo "--- clicks.log";   head -n 2 clicks.log; tail -n 2 clicks.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" pages.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[pages\] +[1-9][0-9,]* changes/s' pages.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("pages.log") if re.search(r"\[pages\] +[1-9][0-9,]* changes/s", l)][10:]
print("steady-state medians: changes/s %.0f  change-to-page p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) changes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
all_p = [l for l in open("pages.log") if re.search(r"\[pages\] +[1-9][0-9,]* changes/s", l)]
p99 = nums(r"p99 +([0-9.]+)ms", all_p)
rate = nums(r"\] +([0-9,]+) changes/s", all_p)
if len(p99) > 45:
    # approximate windows (pages starts 5 s after listings): quiet, clickstream only, onsale + clickstream
    print("p99 median per second: quiet %.1f ms (%.0f changes/s), with 2 MB/s clickstream %.1f ms, onsale + clickstream %.1f ms (%.0f changes/s)" % (
        st.median(p99[10:18]), st.median(rate[10:18]), st.median(p99[19:24]), st.median(p99[26:37]), st.median(rate[26:37])))
picks, stale = sum(nums(r"seat picks ([0-9,]+)", p)), sum(nums(r"would list ([0-9,]+) already-sold", p))
share = nums(r"already-sold listings \(([0-9.]+)%\)", p)
print("seat picks sent: %d  listings a 2 s-behind page would still show as available, already sold: %d (median %.2f%% of listings shown)" % (
    picks, stale, st.median(share) if share else 0))
if picks < 1000 or stale < 1:
    raise SystemExit("seat picks or the lagged-view comparison did not flow")
l = [x for x in open("listings.log") if "seat picks confirmed" in x][15:]
ok, rej, moved = (sum(nums(q, l)) for q in (r"confirmed ([0-9,]+)  rej", r"already sold ([0-9,]+)", r"moved price ([0-9,]+)"))
print("checkout after warm-up: seat picks confirmed %d  rejected as already sold %d (%.2f%%)  confirmed at a moved price %d" % (
    ok, rej, 100 * rej / max(ok + rej, 1), moved))
if ok < 1000:
    raise SystemExit("seat picks did not reach checkout")
PY
grep -q "\[clicks\] done" clicks.log || { echo "the clickstream neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
