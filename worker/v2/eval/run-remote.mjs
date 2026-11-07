// Run the deployed v2 pipeline (comps.harborcap.dev) on local files, save staged records.
// usage: node run-remote.mjs <outdir> <provider> <model> <file...>
import fs from "node:fs";
import path from "node:path";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret };
const BASE = process.env.BASE || "https://comps.harborcap.dev";
const [outdir, provider, model, ...files] = process.argv.slice(2);
fs.mkdirSync(outdir, { recursive: true });
async function j(p, o = {}) {
  for (let a = 0; a < 3; a++) {
    const r = await fetch(BASE + p, { ...o, headers: { ...H, ...(o.headers || {}) } });
    const t = await r.text();
    try { const d = JSON.parse(t); if (r.status >= 500 && a < 2) { await new Promise((s) => setTimeout(s, 2000)); continue; } return d; } catch { if (a === 2) throw new Error(`${r.status} ${t.slice(0, 200)}`); await new Promise((s) => setTimeout(s, 2000)); }
  }
}
let i = 0;
async function one(f) {
  const name = path.basename(f);
  const t0 = Date.now();
  try {
    const fd = new FormData();
    fd.append("file", new Blob([fs.readFileSync(f)]), name);
    const up = await j("/api/v2/upload", { method: "POST", body: fd });
    if (up.error) throw new Error(up.error);
    const q = `?provider=${provider}&model=${encodeURIComponent(model)}`;
    const sv = await j(`/api/v2/jobs/${up.job_id}/survey${q}`, { method: "POST" });
    if (sv.error) throw new Error("survey: " + sv.error);
    let k = 0; const errs = [];
    await Promise.all(Array.from({ length: Math.min(4, sv.chunks) }, async () => { while (k < sv.chunks) { const n = k++; const r = await j(`/api/v2/jobs/${up.job_id}/chunk/${n}`, { method: "POST" }); if (r.error) errs.push(r.error); } }));
    const job = await j(`/api/v2/jobs/${up.job_id}`);
    const records = job.records.map((r) => r.record);
    fs.writeFileSync(path.join(outdir, name + ".json"), JSON.stringify({ file: name, job: up.job_id, survey: job.survey, records, errs }, null, 1));
    console.log(`${name}: ${records.length} records, ${sv.chunks} chunks, ${((Date.now() - t0) / 1000).toFixed(1)}s${errs.length ? " ERR " + errs[0] : ""}`);
  } catch (e) { console.log(`${name}: FAILED ${e.message}`); }
}
const conc = Number(process.env.CONC || 4);
await Promise.all(Array.from({ length: conc }, async () => { while (i < files.length) await one(files[i++]); }));
