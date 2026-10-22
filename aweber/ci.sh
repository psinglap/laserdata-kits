#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Imbi notifications lane for about a minute: deployment and
# mention notifications at 200/s (a 5x release wave from 20 s to 30 s), 1% published twice, read by slackbot and
# email on their own connections, with Imbi's analytics rows (2,000 rows/s, mostly bulky webhook rows) on the same
# server from 30 s. The email reader is stalled on purpose for 8 s (it polls and applies nothing). Pass only if each
# reader holds the 50 ms p99 budget in 95% of its steady-state seconds, nothing is missing or posted twice,
# re-sent notifications are suppressed, and the watchdog flags the email stall within 6 s and nothing else.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the shared runner that burst stalled every
# reader for 0.2-0.8 s at once on other kits; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py imbi_api.py notify.py watchdog.py analytics_flood.py iggy_bridge.py
# The readers and the watchdog start first and create the topics; Imbi's services write into them.
timeout 88 python -u notify.py --reader slackbot > slackbot.log 2>&1 &
timeout 88 python -u notify.py --reader email --stall-at 48 --stall-for 8 > email.log 2>&1 &
timeout 86 python -u watchdog.py > watchdog.log 2>&1 &
sleep 3
timeout 80 python -u imbi_api.py --rate 200 --duration 75 > imbi_api.log 2>&1 &
timeout 80 python -u analytics_flood.py --rate 2000 --at 30 --duration 45 > analytics.log 2>&1 &
wait || true
echo "--- imbi_api.log";  head -n 2 imbi_api.log; sed -n '20,23p' imbi_api.log; tail -n 2 imbi_api.log
echo "--- analytics.log"; head -n 2 analytics.log; tail -n 1 analytics.log
for r in slackbot email; do echo "--- $r.log"; head -n 3 $r.log; tail -n 3 $r.log; done
echo "--- email.log around the injected stall"; grep -n -E "stall injected|catching up" email.log | sed -n '2,20p' || true
echo "--- watchdog.log flags"; grep -E "STALL|SILENT|RECOVERED" watchdog.log || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" slackbot.log email.log | cut -c1-200 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for r in slackbot email; do
  steady=$(grep -E "\[$r\] +[1-9][0-9,]* notifications/s" $r.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$r steady-state seconds with live notifications: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
  last=$(grep -E "\[$r\] .*missing" $r.log | tail -n 1)
  if ! grep -q "posted twice 0  missing 0" <<<"$last"; then echo "$r posted a notification twice or missed one"; status=1; fi
  if ! grep -Eq "\(([1-9][0-9,]*) total\)" <<<"$last"; then echo "$r suppressed no re-sent notifications"; status=1; fi
done
[ "$status" -eq 0 ] || { echo "notifications lane check failed"; exit 1; }
flag=$(grep -E "STALL email:" watchdog.log | head -n 1 || true)
[ -n "$flag" ] || { echo "the watchdog did not flag the email stall"; exit 1; }
secs=$(sed -E 's/.*flagged ([0-9.]+) s after.*/\1/' <<<"$flag")
python -c "import sys; sys.exit(0 if float('$secs') <= 6 else 1)" || { echo "email stall flagged after $secs s (> 6 s)"; exit 1; }
if grep -Eq "STALL slackbot|SILENT" watchdog.log; then echo "the watchdog flagged a healthy reader"; exit 1; fi
grep -q "RECOVERED email" watchdog.log || { echo "the watchdog never saw email recover"; exit 1; }
grep -Eq "\[analytics\] [1-9]" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for r in ("slackbot", "email"):
    t = [l for l in open(f"{r}.log") if re.search(r"\] +[1-9][0-9,]* notifications/s", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("%s steady-state over %d s: notifications/s median %.0f  publish-to-%s p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms (medians)  worst-second p99 %.1f ms" % (
          r, len(t), st.median(num(r"\] +([0-9,]+) notifications/s", t)), r, st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"{r}.log") if re.search(r"\] +[0-9,]+ notifications/s", l)])
    print("%s p99 median before the analytics neighbour %.1f ms, with it %.1f ms" % (
          r, st.median(allp[8:28]) if allp[8:28] else float("nan"), st.median(allp[32:]) if allp[32:] else float("nan")))
    print("%s at the end:" % r, [l for l in open(f"{r}.log") if "missing" in l][-1].split("ms  ", 4)[-1].strip())
print("watchdog:", " | ".join(l.strip() for l in open("watchdog.log") if re.search("STALL|RECOVERED", l)))
PY
echo "Kit OK on Apache Iggy 0.9.0"
