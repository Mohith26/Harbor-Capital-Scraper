import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
async function loop(path) {
  let after = 0, n = 0;
  for (;;) {
    const r = await fetch("https://comps.harborcap.dev" + path, { method: "POST", headers: H, body: JSON.stringify({ after }) }).then((x) => x.json());
    if (r.error) { console.log(path, "ERR", r.error); return; }
    n += r.done || r.scanned || 0;
    if (!r.last) break;
    after = r.last;
  }
  console.log(path, "done", n);
}
await loop("/api/v2/admin/revalidate");
await loop("/api/v2/admin/refresh");
