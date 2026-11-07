import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
const items = JSON.parse(fs.readFileSync(process.argv[2], "utf8")).map(({ property_id, obs }) => ({ property_id, obs }));
let done = 0;
for (let i = 0; i < items.length; i += 30) {
  const r = await fetch("https://comps.harborcap.dev/api/v2/admin/observations", { method: "POST", headers: H, body: JSON.stringify({ items: items.slice(i, i + 30), replace_source: process.argv[3] || "om" }) });
  const j = await r.json().catch(() => ({ error: "bad json " + r.status }));
  if (j.error) { console.log("ERR", i, j.error); continue; }
  done += j.done;
}
console.log("pushed", done);
