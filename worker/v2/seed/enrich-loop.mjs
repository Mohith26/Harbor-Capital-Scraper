import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
let total = 0, idle = 0;
for (let i = 0; i < 400; i++) {
  const r = await fetch("https://comps.harborcap.dev/api/v2/enrich", { method: "POST", headers: H, body: JSON.stringify({ limit: 25 }) }).then((x) => x.json()).catch((e) => ({ error: String(e) }));
  const n = r.enriched?.length || 0;
  total += n;
  console.log(new Date().toISOString().slice(11, 19), "batch", n, "total", total, r.error || "");
  if (!n) { if (++idle > 30) break; await new Promise((s) => setTimeout(s, 60000)); } else idle = 0;
}
