// Re-run CoStar/CAD/parcel enrichment (no aerial/web) for properties with an active comp.
// Usage: node enrich-all.mjs [minPropertyId]
import fs from "node:fs";
import { execSync } from "node:child_process";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
const min = Number(process.argv[2] || 0);
const out = execSync(`npx wrangler d1 execute harbor-comps --remote --json --command "SELECT id FROM v2_properties p WHERE p.id > ${min} AND latitude IS NOT NULL AND EXISTS (SELECT 1 FROM v2_comps c WHERE c.property_id = p.id AND c.status = 'active') ORDER BY id"`, { cwd: "/Users/mohithgajjela/HarborCapital/Harbor-Capital-Scraper", encoding: "utf8", maxBuffer: 1e8 });
const ids = JSON.parse(out)[0].results.map((r) => r.id);
console.log("properties", ids.length);
const chunks = [];
for (let i = 0; i < ids.length; i += 25) chunks.push(ids.slice(i, i + 25));
let done = 0, fail = 0, obs = 0;
async function worker() {
  while (chunks.length) {
    const c = chunks.shift();
    for (let a = 0; a < 3; a++) {
      try {
        const r = await fetch("https://comps.harborcap.dev/api/v2/enrich", { method: "POST", headers: H, body: JSON.stringify({ ids: c }) });
        const j = await r.json();
        if (j.error) throw new Error(j.error);
        done += c.length; obs += j.enriched.reduce((s, e) => s + (e.observations || 0), 0);
        break;
      } catch (e) { if (a === 2) { fail += c.length; console.log("fail", c[0], e.message); } else await new Promise((s) => setTimeout(s, 4000)); }
    }
    console.log(`done ${done} fail ${fail} obs ${obs}`);
  }
}
await Promise.all([worker(), worker(), worker(), worker()]);
console.log("FINISHED", done, fail, obs);
