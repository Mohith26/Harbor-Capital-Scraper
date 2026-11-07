// Seed the v2 database: extract -> resolve -> commit each file through the live API.
// usage: node run-seed.mjs <logdir> <file...>     (identical files by sha256 are skipped)
import fs from "node:fs"; import path from "node:path"; import crypto from "node:crypto";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "x-harbor-user": "seed@harborcap.com" };
const BASE = "https://comps.harborcap.dev";
const [logdir, ...files] = process.argv.slice(2); fs.mkdirSync(logdir, { recursive: true });
async function j(p, o = {}) {
  for (let a = 0; a < 4; a++) {
    try { const r = await fetch(BASE + p, { ...o, headers: { ...H, ...(o.headers || {}) } }); const t = await r.text(); const d = JSON.parse(t); if (r.status >= 500 && a < 3) { await new Promise((s) => setTimeout(s, 3000)); continue; } return d; }
    catch (e) { if (a === 3) throw e; await new Promise((s) => setTimeout(s, 3000)); }
  }
}
const seen = new Set(); const list = [];
for (const f of files) { const h = crypto.createHash("sha256").update(fs.readFileSync(f)).digest("hex"); if (seen.has(h)) { console.log(`SKIP identical ${path.basename(f)}`); continue; } seen.add(h); list.push(f); }
let i = 0;
async function one(f) {
  const name = path.basename(f); const t0 = Date.now();
  try {
    const fd = new FormData(); fd.append("file", new Blob([fs.readFileSync(f)]), name);
    const up = await j("/api/v2/upload", { method: "POST", body: fd }); if (up.error) throw new Error(up.error);
    if (up.scanned) { console.log(`${name}: SCANNED PDF (needs browser vision upload)`); return; }
    const sv = await j(`/api/v2/jobs/${up.job_id}/survey`, { method: "POST" }); if (sv.error) throw new Error("survey: " + sv.error);
    let k = 0; const errs = [];
    await Promise.all(Array.from({ length: Math.min(3, sv.chunks) }, async () => { while (k < sv.chunks) { const n = k++; const r = await j(`/api/v2/jobs/${up.job_id}/chunk/${n}`, { method: "POST" }); if (r.error) errs.push(r.error.slice(0, 120)); } }));
    const rs = await j(`/api/v2/jobs/${up.job_id}/resolve`, { method: "POST" }); if (rs.error) throw new Error("resolve: " + rs.error);
    const cm = await j(`/api/v2/jobs/${up.job_id}/commit`, { method: "POST", body: "{}", headers: { "content-type": "application/json" } }); if (cm.error) throw new Error("commit: " + cm.error);
    const line = `${name}: ${rs.records} recs -> +${cm.added} new, ${cm.merged} merged, ${cm.review_left} held, ${cm.skipped} skipped | ${((Date.now() - t0) / 1000).toFixed(0)}s${errs.length ? " | chunk errs " + errs.length + ": " + errs[0] : ""} | job ${up.job_id}`;
    console.log(line);
  } catch (e) { console.log(`${name}: FAILED ${e.message}`); }
}
const conc = Number(process.env.CONC || 2);
await Promise.all(Array.from({ length: conc }, async () => { while (i < list.length) await one(list[i++]); }));
console.log("DONE");
