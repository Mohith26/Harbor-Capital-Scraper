import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
const which = process.argv[2];
let after = Number(process.argv[3] || 0), n = 0;
for (;;) {
  const r = await fetch(`https://comps.harborcap.dev/api/v2/admin/${which}`, { method: "POST", headers: H, body: JSON.stringify({ after }) });
  const j = await r.json().catch(async () => ({ error: (await r.text()).slice(0, 200) }));
  if (j.error) { console.log("ERR", after, j.error); await new Promise((s) => setTimeout(s, 3000)); continue; }
  n += j.done;
  if (!j.done || j.last == null) break;
  after = j.last;
  process.stdout.write(`\r${which} ${n} (last ${after})   `);
}
console.log(`\n${which} done: ${n}`);
