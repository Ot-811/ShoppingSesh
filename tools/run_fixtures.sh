#!/bin/sh
# usage: tools/run_fixtures.sh "cmd taking DATA OUT"   (cmd must write user_daily.csv/campaign_daily.csv in OUT)
fail=0
for d in fixtures/*/; do
  n=$(basename "$d"); rm -rf /tmp/fx_$n
  $1 "$d/input" /tmp/fx_$n >/dev/null 2>/tmp/fx_$n.err || { echo "$n: RUN FAIL"; fail=1; continue; }
  python3 tools/cmpcsv.py /tmp/fx_$n "$d/expected" >/tmp/fx_$n.diff 2>&1 && echo "$n ok" || { echo "$n FAIL"; head -3 /tmp/fx_$n.diff; fail=1; }
done
exit $fail
