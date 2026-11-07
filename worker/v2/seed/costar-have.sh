#!/bin/bash
# Writes the JSON list of CoStar comp labels already in harbor-comps (primary label or merged source).
cd /Users/mohithgajjela/HarborCapital/Harbor-Capital-Scraper
OUT=$1
npx wrangler d1 execute harbor-comps --remote --json --command "select source_label l from v2_comps where source_label like 'CoStar % comp %' union select j.value ->> 'label' from v2_comps c, json_each(c.sources_json) j where c.sources_json like '%CoStar % comp %' and (j.value ->> 'label') like 'CoStar % comp %'" 2>/dev/null | python3 -c "import sys,json;d=json.load(sys.stdin);print(json.dumps([r['l'] for x in d for r in x['results']]))" > $OUT
python3 -c "import json;print('have', len(json.load(open('$OUT'))))"
