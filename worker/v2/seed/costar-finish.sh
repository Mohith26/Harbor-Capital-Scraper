#!/bin/bash
# After costar-load2 finishes: reload rows lost to D1 overload, retag, tidy players, dedupe report.
cd /Users/mohithgajjela/HarborCapital/Harbor-Capital-Scraper
L=/Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2/costar-load.log
until grep -q "lease finished" $L; do sleep 60; done
echo "load finished $(date)"
for pass in 1 2; do
  worker/v2/seed/costar-have.sh /Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2/have.json
  rm -f /Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2/costar-reconcile-progress.json
  node worker/v2/seed/costar-load2.mjs /Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2 --kinds=sale,lease --conc=2 --have=/Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2/have.json
done
node worker/v2/seed/backfill.mjs retag
curl -s -X POST -H "CF-Access-Client-Id: $(python3 -c "import json;print(json.load(open('/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json'))['client_id'])")" -H "CF-Access-Client-Secret: $(python3 -c "import json;print(json.load(open('/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json'))['client_secret'])")" -H 'content-type: application/json' --data '{}' https://comps.harborcap.dev/api/v2/admin/players-maint; echo
node worker/v2/seed/sweep.mjs dry /Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/seed2/sweep-dry.json
npx wrangler d1 execute harbor-comps --remote --json --command "select source_kind, record_kind, count(*) n from v2_comps where status='active' group by 1,2" 2>/dev/null
echo ALLDONE $(date)
