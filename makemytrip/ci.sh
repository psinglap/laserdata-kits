#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the last-searched lane at small scale for about a minute, B2C and B2B
# together on 64 partitions, with a sale-opening surge and a clickstream next to it, and pass only if the lane
# reports PASS and the search-bar taps are served a list that holds the traveller's last search.
# Then a 40 s what-if: the same taps with every search applied 1 s late, as a micro-batch path would.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py app.py recents.py clickstream.py kafka_bridge.py
# The last-searched service starts first and creates the topics; the apps and the neighbour follow.
timeout 78 python -u recents.py > recents.log 2>&1 &
sleep 3
# 1,500 hotel searches and views a second over 200,000 travellers (15% B2B), half followed by a tap on the search
# bar 0.3-3 s later, 2x for 15 s from second 30 (a sale opening); 1 MB/s of clickstream from second 20.
timeout 72 python -u app.py --users 200000 --searches-per-s 1500 --surge 2 --surge-at 30 --surge-for 15 > app.log 2>&1 &
timeout 70 python -u clickstream.py --mbps 1 --at 20 --duration 35 > clickstream.log 2>&1 &
wait || true
# What-if: the same apps, with the service applying each search only once it is 1 s old.
timeout 45 python -u recents.py --name hold --hold-ms 1000 > hold.log 2>&1 &
sleep 3
timeout 40 python -u app.py --users 200000 --searches-per-s 1500 --surge 1 > app-hold.log 2>&1 &
wait || true
echo "--- app.log";         head -n 2 app.log; tail -n 4 app.log
echo "--- recents.log";     tail -n 8 recents.log
echo "--- clickstream.log"; head -n 2 clickstream.log; tail -n 2 clickstream.log
echo "--- hold.log";        tail -n 4 hold.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" recents.log | cut -c1-170 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[recents\] +[1-9][0-9,]* searches/s' recents.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[clickstream\] done" clickstream.log || { echo "the clickstream neighbour did not run"; exit 1; }
# Steady-state summary: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("recents.log") if re.search(r"\[recents\] +[1-9][0-9,]* searches/s", l)][10:]
f = [l for l in open("recents.log") if re.search(r"\[search-bar\] taps [1-9]", l)][10:]
print("steady-state medians over %d s: searches/s %.0f  search-to-list p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) searches/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms"))))
print("segments through the one lane: B2C %.0f, B2B %.0f events" % (sum(nums(r"B2C ([0-9,]+)", h)),
                                                                   sum(nums(r"B2B ([0-9,]+)", h))))
peak = [l for l in h if nums(r"\] +([0-9,]+) searches/s", [l])[0] > 2000]
if peak:
    print("surge: %d s above 2,000 searches/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(peak), st.median(nums(r"p99 +([0-9.]+)ms", peak)), max(nums(r"p99 +([0-9.]+)ms", peak))))
stale = nums(r"without the last search ([0-9.]+)%", f)
print("search-bar taps: %d s checked, %.0f taps, %.0f after a new hotel, served without the last search: "
      "median %.3f%%, worst second %.3f%%, last-searched hotel missing %.0f" % (
    len(f), sum(nums(r"taps ([0-9,]+)  after", f)), sum(nums(r"new hotel ([0-9,]+)  served", f)),
    st.median(stale) if stale else float("nan"), max(stale) if stale else float("nan"),
    sum(nums(r"hotel missing ([0-9,]+)", f))))
e = [l for l in open("clickstream.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("clickstream neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
hr = [l for l in open("hold.log") if re.search(r"\[search-bar\] taps [1-9]", l)][5:]
if hr:
    print("what-if, every search applied 1 s late: %d s, %.0f taps, served without the last search: median %.1f%%, "
          "last-searched hotel missing %.0f" % (len(hr), sum(nums(r"taps ([0-9,]+)  after", hr)),
          st.median(nums(r"without the last search ([0-9.]+)%", hr)), sum(nums(r"hotel missing ([0-9,]+)", hr))))
if len(f) < 30:
    raise SystemExit("too few seconds with search-bar taps")
if st.median(stale) > 0.1:
    raise SystemExit("too many taps were served a list without the traveller's last search")
if "(surge)" not in open("app.log").read() or not peak:
    raise SystemExit("the surge did not reach the lane")
if not sum(nums(r"B2B ([0-9,]+)", h)):
    raise SystemExit("no B2B searches reached the lane")
PY
echo "Kit OK on Apache Iggy 0.9.0"
