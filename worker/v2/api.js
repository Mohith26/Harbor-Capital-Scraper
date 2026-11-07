// HTTP API for the v2 comp database (served by the Worker, no container).

import { renderDocument } from "./render.js";
import { makeLLM, pool } from "./llm.js";
import { surveyDocument, planChunks, extractChunk, dedupeChunkOverlap, verifyRecord } from "./extract.js";
import { validateRecord, BOUNDS, dateToNumber } from "./validate.js";
import { geocode, marketFor, inTexas } from "./geo.js";
import { dedupeBatch, tagPortfolios, samePropertyScore, sameDealScore, mergeMissing } from "./dedupe.js";
import { bbox as bboxOf } from "./geo.js";
import { findPropertyMatch, findDealMatch, commitRecord, refreshProperty, insertObservations, COMP_FIELDS, txnDate as txnDateOf } from "./store.js";
import { findComps, explainComps, geoCache, ALIASES } from "./finder.js";
import { matchCostar, costarObservations, parcelObservations, aerialObservations, webObservations } from "./enrich.js";
import { RECORD_KEYS, SCHEMA_PROMPT } from "./schema.js";
import UI_SHELL from "./ui.html";
import UI_DEALS from "./ui-deals.html";
import UI_LIB from "./ui-lib.html";
import LOGO_BLACK from "./logo-black-svg.html";
const UI_HTML = UI_SHELL.replace("<!--EXTRA_JS-->", () => UI_DEALS + "\n" + UI_LIB);
export { LOGO_BLACK };
import * as XLSX from "xlsx";
import { handleDeals, checkDeals } from "./deals.js";
import { handleLibrary, checkSaved } from "./library.js";
import { linkPlayers, coreName } from "./players.js";
import { productTags } from "./tags.js";
import { rentBasis } from "./store.js";
import { handleMcp } from "./mcp.js";

const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
const bad = (msg, status = 400) => json({ error: msg }, status);

function userOf(req) {
  const email = req.headers.get("cf-access-authenticated-user-email");
  if (email) return email;
  const jwt = req.headers.get("cf-access-jwt-assertion");
  if (jwt) {
    try {
      const payload = JSON.parse(atob(jwt.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
      if (payload.email) return payload.email;
      if (payload.common_name) return req.headers.get("x-harbor-user") ? `${req.headers.get("x-harbor-user")} (automation)` : "service-token";
    } catch {}
  }
  return "unknown";
}
const isService = (u) => u === "service-token" || /\(automation\)$/.test(u);

async function sha256(buf) {
  const h = await crypto.subtle.digest("SHA-256", buf);
  return [...new Uint8Array(h)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function llmFor(env, url) {
  const provider = url?.searchParams?.get("provider") || env.LLM_PROVIDER || (env.OPENAI_API_KEY ? "openai" : "deepseek");
  return makeLLM(env, { provider, strong: url?.searchParams?.get("model") || env.LLM_MODEL_STRONG || undefined });
}

async function loadDoc(env, job) {
  const plan = JSON.parse(job.plan_json || "{}");
  const obj = await env.FILES.get(plan.rendered_key);
  if (!obj) throw new Error("rendered document missing");
  return { doc: await obj.json(), plan };
}

export async function handleApi(req, env, ctx) {
  const url = new URL(req.url);
  const path = url.pathname.replace(/\/+$/, "");
  const db = env.COMPS_DB;
  const m = (re) => re.exec(path);
  let mm;
  // Admin / maintenance routes: service token or named admins only.
  if (path.startsWith("/api/v2/admin/") || path === "/api/v2/llmtest") {
    const who = userOf(req);
    const admins = String(env.ADMIN_EMAILS || "mohith@harborcap.com,isaac@harborcap.com").split(",").map((x) => x.trim().toLowerCase());
    if (!isService(who) && !admins.includes(who.toLowerCase())) return bad("admin only", 403);
  }
  try {
    {
      const who = userOf(req);
      const r1 = await handleDeals(req, env, ctx, path, who);
      if (r1) return r1;
      const r2 = await handleLibrary(req, env, ctx, path, who);
      if (r2) return r2;
      if (path === "/api/v2/mcp") return await handleMcp(req, env, ctx, who);
    }
    // ---------------- meta ----------------
    if (path === "/api/v2/health") {
      const c = await db.prepare("SELECT (SELECT count(*) FROM v2_comps WHERE status='active') AS comps, (SELECT count(*) FROM v2_properties) AS properties").first();
      return json({ ok: true, ...c, provider: env.LLM_PROVIDER || (env.OPENAI_API_KEY ? "openai" : "deepseek") });
    }
    if (path === "/api/v2/me") return json({ user: userOf(req) });
    if (path === "/api/v2/llmtest") {
      const out = {};
      for (const provider of ["openai", "deepseek"]) {
        try {
          const llm = makeLLM(env, { provider, noFallback: true, strong: url.searchParams.get("model") || undefined });
          const t0 = Date.now();
          const r = await llm.chat({ user: 'Return JSON {"ok": true}', tier: url.searchParams.get("tier") || "strong", reasoning: "off", maxTokens: 50 });
          out[provider] = { ok: !!r.ok, ms: Date.now() - t0, models: llm.models };
        } catch (e) {
          out[provider] = { error: String(e.message || e).slice(0, 300) };
        }
      }
      if (env.OPENAI_API_KEY && url.searchParams.get("raw")) {
        const r = await fetch("https://api.openai.com/v1/chat/completions", { method: "POST", headers: { Authorization: `Bearer ${env.OPENAI_API_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ model: url.searchParams.get("model") || "gpt-4.1", messages: [{ role: "user", content: "hi" }], max_completion_tokens: 5 }) });
        out.raw = { status: r.status, headers: Object.fromEntries([...r.headers].filter(([k]) => /ratelimit|retry/.test(k))), body: (await r.text()).slice(0, 600) };
      }
      if (env.OPENAI_API_KEY && url.searchParams.get("models")) {
        const r = await fetch("https://api.openai.com/v1/models", { headers: { Authorization: `Bearer ${env.OPENAI_API_KEY}` } });
        const d = await r.json();
        out.openai_models = (d.data || []).map((m) => m.id).filter((id) => /gpt|o\d/.test(id)).sort();
      }
      return json(out);
    }
    if (path === "/api/v2/bounds") return json(BOUNDS);

    // ---------------- upload / extraction ----------------
    if (path === "/api/v2/upload" && req.method === "POST") {
      const form = await req.formData();
      let file = form.get("file");
      const pasted = form.get("text");
      if ((!file || typeof file === "string") && pasted) file = new File([pasted], `${(form.get("title") || "Pasted comps").toString().slice(0, 60)}.txt`, { type: "text/plain" });
      if (!file || typeof file === "string") return bad("no file");
      const buf = await file.arrayBuffer();
      const name = file.name || "upload";
      if (buf.byteLength > 30 * 1024 * 1024) return bad("file too large (30 MB max)");
      const sha = await sha256(buf);
      const prior = await db.prepare("SELECT id, filename, uploaded_at, uploaded_by, status FROM v2_files WHERE sha256 = ? ORDER BY id DESC LIMIT 1").bind(sha).first();
      const doc = await renderDocument(buf, name);
      const fileKey = `files/${sha}/${name}`;
      const renderedKey = `rendered/${sha}.json`;
      await env.FILES.put(fileKey, buf);
      await env.FILES.put(renderedKey, JSON.stringify(doc));
      const f = await db.prepare("INSERT INTO v2_files (filename, sha256, size, kind, broker, uploaded_by, status, r2_key) VALUES (?, ?, ?, ?, ?, ?, 'new', ?) RETURNING id").bind(name, sha, buf.byteLength, doc.kind, form.get("broker") || null, userOf(req), fileKey).first();
      const jobId = crypto.randomUUID();
      await db.prepare("INSERT INTO v2_jobs (id, file_id, status, plan_json, created_by) VALUES (?, ?, 'surveying', ?, ?)").bind(jobId, f.id, JSON.stringify({ rendered_key: renderedKey, filename: name }), userOf(req)).run();
      const sheets = doc.sheets.map((s) => ({ name: s.name, rows: s.rows.length, hidden: s.hidden }));
      return json({ job_id: jobId, file_id: f.id, filename: name, kind: doc.kind, scanned: !!doc.scanned, sheets, prior });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/survey$/)) && req.method === "POST") {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const { doc, plan } = await loadDoc(env, job);
      const llm = llmFor(env, url);
      const survey = await surveyDocument(doc, llm, { surveyTier: "strong", surveyReasoning: "low" });
      const chunks = planChunks(doc, survey);
      plan.survey = survey;
      plan.chunks = chunks;
      plan.provider = llm.provider;
      plan.model = llm.models.strong;
      await db.batch([
        db.prepare("UPDATE v2_jobs SET status = 'extracting', plan_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(JSON.stringify(plan), job.id),
        db.prepare("UPDATE v2_files SET status = 'surveyed', summary = ?, survey_json = ? WHERE id = ?").bind(survey.document_summary || null, JSON.stringify(survey), job.file_id),
        db.prepare("DELETE FROM v2_staged WHERE job_id = ?").bind(job.id),
      ]);
      return json({ survey, chunks: chunks.length, usage: llm.usage });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/chunk\/(\d+)$/)) && req.method === "POST") {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const { doc, plan } = await loadDoc(env, job);
      const n = Number(mm[2]);
      const ch = plan.chunks?.[n];
      if (!ch) return bad("no such chunk");
      // Layout-heavy PDFs / pasted text read better with the larger DeepSeek model.
      const pdfLike = doc.kind === "pdf" || doc.kind === "text";
      const model = plan.provider === "deepseek" && pdfLike ? env.DEEPSEEK_PDF_MODEL || "deepseek-v4-pro" : plan.model;
      const llm = makeLLM(env, { provider: plan.provider, strong: model });
      const { records, error } = await extractChunk(doc, plan.survey, ch, llm, {});
      const validated = records.map((r) => validateRecord(r));
      const stmts = validated.map((r, i) => db.prepare("INSERT OR REPLACE INTO v2_staged (job_id, idx, chunk, record_json) VALUES (?, ?, ?, ?)").bind(job.id, n * 1000 + i, n, JSON.stringify(r)));
      await db.prepare("DELETE FROM v2_staged WHERE job_id = ? AND chunk = ?").bind(job.id, n).run();
      for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
      return json({ chunk: n, records: validated.length, error: error || null, usage: llm.usage });
    }

    // Scanned PDF pages rendered to images in the browser.
    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/vision$/)) && req.method === "POST") {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const body = await req.json();
      const llm = llmFor(env, url);
      const page = Number(body.page || 1);
      const out = await llm.chat({
        tier: "vision",
        reasoning: "off",
        maxTokens: 12000,
        system: `You extract commercial real estate comps from a scanned page image. Every value must be visible on the page.\n${SCHEMA_PROMPT}\nReturn JSON only.`,
        user: `File: ${body.filename || ""}, page ${page}. Extract every comp on this page as {"records":[...]} (omit null keys; source_sheet "page ${page}"; source_rows [] ). If the page has no comp table return {"records":[]}.`,
        images: [body.image],
      });
      const recs = (out.records || []).map((r) => {
        const o = {};
        for (const k of RECORD_KEYS) o[k] = r[k] ?? null;
        o.source_sheet = `page ${page}`;
        o.source_rows = [];
        o._flags = [{ field: "source", code: "vision", severity: "info", msg: "read from a scanned image - numbers not cell-verified" }];
        return validateRecord(o);
      });
      const base = 900000 + page * 100;
      const stmts = recs.map((r, i) => db.prepare("INSERT OR REPLACE INTO v2_staged (job_id, idx, chunk, record_json) VALUES (?, ?, ?, ?)").bind(job.id, base + i, 900 + page, JSON.stringify(r)));
      for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
      return json({ page, records: recs.length });
    }

    // Geocode + property match + dedupe for every staged record.
    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/resolve$/)) && req.method === "POST") {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const { results } = await db.prepare("SELECT idx, record_json, decision FROM v2_staged WHERE job_id = ? ORDER BY idx").bind(job.id).all();
      const committedIdx = new Set(results.filter((r) => r.decision === "committed").map((r) => r.idx));
      let recs = dedupeChunkOverlap(
        results
          .filter((r) => r.decision !== "committed")
          .map((r) => {
            const x = JSON.parse(r.record_json);
            // re-validate with current rules (keeps cell-verification flags)
            return { ...validateRecord({ ...x, _flags: (x._flags || []).filter((f) => ["unverified", "vision"].includes(f.code)) }), _idx: r.idx, _edited: x._edited };
          }),
      );
      const keep = new Set([...recs.map((r) => r._idx), ...committedIdx]);
      // context city from the survey helps geocoding rows without a city
      const plan = JSON.parse(job.plan_json || "{}");
      const ctxCity = plan.survey?.context?.city || null;
      const cache = geoCache(db);
      await pool(recs, 8, async (r) => {
        if (r.record_kind === "subject") return;
        if (r.latitude != null && r.longitude != null && inTexas(r.latitude, r.longitude)) {
          r.geocode_quality = "source";
          return;
        }
        if (!r.address && !r.property_name) {
          const sm = await locateArea(db, r, ctxCity, plan.survey?.context?.market);
          if (sm) { Object.assign(r, { latitude: sm.lat, longitude: sm.lng, geocode_quality: "submarket" }); return; }
        }
        const g = await geocode(env, { address: r.address, city: r.city || ctxCity, zip: r.zip, propertyName: r.property_name }, { cache });
        r._geo = g;
        if (g.lat != null) {
          r.latitude = g.lat;
          r.longitude = g.lng;
          r.geocode_quality = g.quality;
          if (!r.city && g.city) r.city = g.city;
          if (!r.zip && g.zip) r.zip = g.zip;
          if (!r.county && g.county) r.county = g.county;
          if (["city", "postal"].includes(g.quality)) (r._flags = r._flags || []).push({ field: "address", code: "geocode_low", severity: "warn", msg: `located only to ${g.quality} level (${g.formatted})` });
        } else (r._flags = r._flags || []).push({ field: "address", code: "geocode_failed", severity: "warn", msg: `could not locate "${g.query || r.address}"` });
      });
      recs = tagPortfolios(dedupeBatch(recs));
      // Summary + detailed tables of the same comps: fold the repeat's extra
      // fields into the first occurrence, then skip the repeat.
      for (const r of recs) {
        if (!r._batchDup) continue;
        const head = recs.find((x) => x._idx === recs[r._batchDup.of]?._idx) || recs[r._batchDup.of];
        if (!head || r._batchDup.verdict !== "duplicate") continue;
        for (const k of RECORD_KEYS) if ((head[k] == null || head[k] === "") && r[k] != null && r[k] !== "" && !["source_sheet", "source_rows", "record_kind"].includes(k)) head[k] = r[k];
      }
      const out = [];
      await pool(recs, 6, async (r) => {
        if (r.record_kind === "subject") {
          out.push({ idx: r._idx, rec: r, match: { verdict: "subject" }, decision: "skip" });
          return;
        }
        const pm = await findPropertyMatch(db, r);
        let deal = null;
        if (pm && !pm.weak) deal = await findDealMatch(db, pm.property.id, r);
        const sev = r._flags?.some((f) => f.severity === "error") ? "error" : r._flags?.some((f) => f.severity === "warn") ? "warn" : "ok";
        let verdict = "new";
        if (r._batchDup) verdict = "batch_duplicate";
        else if (deal?.verdict === "duplicate") verdict = "duplicate";
        else if (deal?.verdict === "conflict") verdict = "conflict";
        else if (pm && !pm.weak) verdict = "same_property";
        // A row with no deal numbers and no size is not a usable comp (blank listing rows, headers read from images).
        const thin = !["sale_price", "price_psf", "price_per_acre", "rent_psf_yr", "rent_per_acre_mo", "asking_rent_psf_yr", "asking_price", "leased_sf", "available_sf", "building_sf", "cap_rate_pct"].some((f) => r[f] != null);
        if (thin) (r._flags = r._flags || []).push({ field: "record", code: "thin", severity: "warn", msg: "no price, rent or size - skipped by default" });
        let decision = verdict === "batch_duplicate" || verdict === "duplicate" ? (verdict === "duplicate" ? "merge" : "skip") : verdict === "conflict" || sev === "error" ? "review" : "add";
        if (thin && decision === "add") decision = "skip";
        const match = {
          verdict,
          property: pm ? { id: pm.property.id, address: pm.property.address, name: pm.property.property_name, score: +pm.score.toFixed(2), reasons: pm.reasons, weak: !!pm.weak } : null,
          deal: deal ? { comp_id: deal.comp.id, verdict: deal.verdict, score: +deal.score.toFixed(2), reasons: deal.reasons, conflicts: deal.conflicts || [], comp: pick(deal.comp) } : null,
          batch: r._batchDup || null,
        };
        r._severity = sev;
        out.push({ idx: r._idx, rec: r, match, decision });
      });
      const stmts = out.map((o) => db.prepare("UPDATE v2_staged SET record_json = ?, match_json = ?, decision = ? WHERE job_id = ? AND idx = ?").bind(JSON.stringify(o.rec), JSON.stringify(o.match), o.decision, job.id, o.idx));
      stmts.push(db.prepare(`DELETE FROM v2_staged WHERE job_id = ? AND idx NOT IN (${[...keep].join(",") || "-1"})`).bind(job.id));
      for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
      await db.prepare("UPDATE v2_jobs SET status = 'review', updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(job.id).run();
      return json({ records: out.length });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/source$/)) && req.method === "GET") {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const { doc } = await loadDoc(env, job);
      const ci = (s) => [...s].reduce((a, ch) => a * 26 + ch.charCodeAt(0) - 64, 0) - 1;
      const sheets = (doc.sheets || []).slice(0, 16).map((s) => {
        const rows = (s.rows || []).slice(0, 700);
        const maxC = Math.min(40, Math.max(0, ...rows.flatMap((r) => r.cells.map((c) => (c.col === "L" ? 0 : ci(c.col))))) + 1);
        return { name: s.name, truncated: (s.rows || []).length > 700, rows: rows.map((r) => { const c = Array(maxC).fill(""); for (const x of r.cells) { const i = x.col === "L" ? 0 : ci(x.col); if (i < maxC) c[i] = String(x.text).slice(0, 120); } return { n: r.row, c }; }) };
      });
      return json({ kind: doc.kind, sheets });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)$/)) && req.method === "GET") {
      const job = await db.prepare("SELECT j.*, f.filename, f.summary FROM v2_jobs j LEFT JOIN v2_files f ON f.id = j.file_id WHERE j.id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const { results } = await db.prepare("SELECT idx, record_json, match_json, decision FROM v2_staged WHERE job_id = ? ORDER BY idx").bind(job.id).all();
      const plan = JSON.parse(job.plan_json || "{}");
      return json({ id: job.id, status: job.status, filename: job.filename, survey: plan.survey || null, chunks: plan.chunks?.length || 0, records: results.map((r) => ({ idx: r.idx, record: JSON.parse(r.record_json), match: r.match_json ? JSON.parse(r.match_json) : null, decision: r.decision })) });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/records\/(\d+)$/)) && req.method === "PATCH") {
      const body = await req.json();
      const row = await db.prepare("SELECT record_json FROM v2_staged WHERE job_id = ? AND idx = ?").bind(mm[1], Number(mm[2])).first();
      if (!row) return bad("not found", 404);
      let rec = JSON.parse(row.record_json);
      if (body.record) {
        const edited = { ...rec, ...body.record };
        const flagsKept = (rec._flags || []).filter((f) => f.code === "vision");
        rec = validateRecord({ ...edited, _flags: flagsKept });
        rec._edited = true;
      }
      await db.prepare("UPDATE v2_staged SET record_json = ?, decision = COALESCE(?, decision) WHERE job_id = ? AND idx = ?").bind(JSON.stringify(rec), body.decision || null, mm[1], Number(mm[2])).run();
      return json({ record: rec });
    }

    if ((mm = m(/^\/api\/v2\/jobs\/([\w-]+)\/commit$/)) && req.method === "POST") {
      const job = await db.prepare("SELECT j.*, f.filename FROM v2_jobs j LEFT JOIN v2_files f ON f.id = j.file_id WHERE j.id = ?").bind(mm[1]).first();
      if (!job) return bad("job not found", 404);
      const body = await req.json().catch(() => ({}));
      const only = body.indices ? new Set(body.indices.map(Number)) : null;
      const { results } = await db.prepare("SELECT idx, record_json, match_json, decision FROM v2_staged WHERE job_id = ? ORDER BY idx").bind(job.id).all();
      const summary = { added: 0, merged: 0, skipped: 0, review_left: 0, properties: new Set() };
      for (const row of results) {
        if (only && !only.has(row.idx)) continue;
        if (row.decision === "committed") continue;
        if (row.decision === "review") {
          summary.review_left++;
          continue;
        }
        const rec = JSON.parse(row.record_json);
        const match = row.match_json ? JSON.parse(row.match_json) : null;
        const res = await commitRecord(db, rec, match ? { property: match.property && !match.property.weak ? match.property : null, deal: match.deal ? { comp: match.deal.comp, verdict: match.deal.verdict } : null } : null, row.decision || "add", { fileId: job.file_id, sourceKind: "broker", sourceLabel: job.filename, user: userOf(req) });
        if (res.action === "added") summary.added++;
        else if (res.action === "merged") summary.merged++;
        else summary.skipped++;
        if (res.property_id) summary.properties.add(res.property_id);
        await db.prepare("UPDATE v2_staged SET decision = 'committed' WHERE job_id = ? AND idx = ?").bind(job.id, row.idx).run();
      }
      await db.batch([
        db.prepare("UPDATE v2_jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(summary.review_left ? "review" : "committed", job.id),
        db.prepare("UPDATE v2_files SET status = ? WHERE id = ?").bind(summary.review_left ? "review" : "committed", job.file_id),
      ]);
      return json({ ...summary, properties: [...summary.properties] });
    }

    // Bulk ingest of already-structured records (CoStar exports, legacy rows).
    // Same validation, geocoding, property matching and dedupe as uploads.
    if (path === "/api/v2/admin/ingest" && req.method === "POST") {
      // The first CoStar loader ran too hot for D1; it is retired in favour of costar-load-2.
      if (userOf(req) === "costar-load (automation)") return json({ error: "paused - use costar-load-2" }, 503);
      const body = await req.json();
      const touched = new Set();
      const sourceKind = body.source_kind || "costar";
      const label = body.source_label || sourceKind;
      let fileId = body.file_id || null;
      if (!fileId) {
        const f = await db.prepare("INSERT INTO v2_files (filename, kind, source_kind, uploaded_by, status, summary) VALUES (?, 'bulk', ?, ?, 'committed', ?) RETURNING id").bind(label, sourceKind, userOf(req), body.summary || null).first();
        fileId = f.id;
      }
      const cache = geoCache(db);
      const out = { file_id: fileId, added: 0, merged: 0, conflicts: 0, skipped: 0, failed: 0, errors: [] };
      for (const raw of body.records || []) {
        try {
          const r = validateRecord(raw);
          if (r.latitude == null || !inTexas(r.latitude, r.longitude)) {
            const g = await geocode(env, { address: r.address, city: r.city, zip: r.zip, propertyName: r.property_name }, { cache });
            if (g.lat != null) Object.assign(r, { latitude: g.lat, longitude: g.lng, geocode_quality: g.quality, _geo: g });
          } else r.geocode_quality = r.geocode_quality || "source";
          if (r.latitude == null) { out.skipped++; continue; }
          const pm = await findPropertyMatch(db, r);
          const deal = pm && !pm.weak ? await findDealMatch(db, pm.property.id, r) : null;
          let decision = "add";
          if (deal?.verdict === "duplicate") decision = "merge";
          if (deal?.verdict === "conflict") { out.conflicts++; r._flags = r._flags || []; r._flags.push({ field: "dedupe", code: "possible_duplicate", severity: "warn", msg: `possible duplicate of comp #${deal.comp.id} (${(deal.conflicts || []).join("; ")})` }); }
          const res = await commitRecord(db, r, { property: pm && !pm.weak ? pm.property : null, deal: deal && deal.verdict === "duplicate" ? deal : null }, decision, { fileId, sourceKind, sourceLabel: raw._source_label || label, user: userOf(req), deferRefresh: true });
          if (res.property_id) touched.add(res.property_id);
          if (res.action === "merged") out.merged++; else if (res.action === "added") out.added++; else out.skipped++;
        } catch (e) {
          out.failed++;
          if (out.errors.length < 5) out.errors.push(String(e.message || e));
        }
      }
      for (const pid of touched) await refreshProperty(db, pid).catch(() => null);
      return json(out);
    }

    if (path === "/api/v2/admin/backfill-players" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const { results } = await db.prepare("SELECT id, buyer, seller, tenant, landlord, txn_date FROM v2_comps WHERE id > ? AND status = 'active' ORDER BY id LIMIT 150").bind(Number(body.after || 0)).all();
      for (const c of results) await linkPlayers(db, c.id, c, c.txn_date);
      return json({ done: results.length, last: results.length ? results[results.length - 1].id : null });
    }
    // Push feature observations from an outside source (OMKB OMs, CoStar archive) and re-resolve.
    if (path === "/api/v2/admin/observations" && req.method === "POST") {
      const body = await req.json();
      const out = [];
      for (const it of (body.items || []).slice(0, 60)) {
        const obs = (it.obs || []).filter((o) => o.field && o.value != null && o.source);
        if (!obs.length) continue;
        if (body.replace_source) await db.prepare("DELETE FROM v2_feature_obs WHERE property_id = ? AND source = ?").bind(it.property_id, body.replace_source).run();
        await insertObservations(db, it.property_id, obs);
        await refreshProperty(db, it.property_id);
        out.push(it.property_id);
      }
      return json({ done: out.length });
    }
    // Player upkeep: merge "X Properties" into "X", recompute counts and last-active dates.
    if (path === "/api/v2/admin/players-maint" && req.method === "POST") {
      const { results } = await db.prepare("SELECT id, name, norm, n_deals FROM v2_players").all();
      const byCore = new Map();
      for (const p of results) { const k = coreName(p.norm); (byCore.get(k) || byCore.set(k, []).get(k)).push(p); }
      let merged = 0;
      for (const [, ps] of byCore) {
        ps.sort((a, b) => b.n_deals - a.n_deals);
        const keep = ps[0];
        if (ps.length < 2) continue;
        await db.prepare("INSERT OR REPLACE INTO v2_player_aliases (alias_norm, player_id) VALUES (?, ?)").bind(`core:${coreName(keep.norm)}`, keep.id).run();
        for (const a of ps.slice(1)) {
          await db.batch([
            db.prepare("INSERT OR IGNORE INTO v2_comp_players (comp_id, player_id, role) SELECT comp_id, ?, role FROM v2_comp_players WHERE player_id = ?").bind(keep.id, a.id),
            db.prepare("DELETE FROM v2_comp_players WHERE player_id = ?").bind(a.id),
            db.prepare("INSERT OR REPLACE INTO v2_player_aliases (alias_norm, player_id) VALUES (?, ?)").bind(a.norm, keep.id),
            db.prepare("UPDATE v2_player_aliases SET player_id = ? WHERE player_id = ?").bind(keep.id, a.id),
            db.prepare("UPDATE v2_players SET kinds = COALESCE(kinds,'') || COALESCE((SELECT kinds FROM v2_players WHERE id = ?), '') WHERE id = ?").bind(a.id, keep.id),
            db.prepare("DELETE FROM v2_players WHERE id = ?").bind(a.id),
          ]);
          merged++;
        }
      }
      await db.prepare(`UPDATE v2_players SET n_deals = (SELECT count(DISTINCT cp.comp_id) FROM v2_comp_players cp JOIN v2_comps c ON c.id = cp.comp_id WHERE cp.player_id = v2_players.id AND c.status = 'active'),
        last_date = (SELECT max(c.txn_date_sort) FROM v2_comp_players cp JOIN v2_comps c ON c.id = cp.comp_id WHERE cp.player_id = v2_players.id AND c.status = 'active' AND c.txn_date_sort <= date('now'))`).run();
      return json({ merged });
    }
    if (path === "/api/v2/admin/run-watch" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      if (body.reset_deal) await db.prepare("UPDATE v2_deals SET last_checked_at = NULL, last_seen_comp_id = ? WHERE id = ?").bind(body.seen ?? 0, body.reset_deal).run();
      if (body.reset_saved) await db.prepare("UPDATE v2_saved SET last_run_at = NULL, last_seen_comp_id = ? WHERE id = ?").bind(body.seen ?? 0, body.reset_saved).run();
      return json({ deals: await checkDeals(env, db), saved: await checkSaved(env, db) });
    }
    if (path === "/api/v2/admin/retag" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const { results } = await db.prepare("SELECT p.*, (SELECT group_concat(COALESCE(c.notes,'') || ' ' || COALESCE(c.source_label,'') || ' ' || COALESCE(c.source_sheet,''), ' ') FROM v2_comps c WHERE c.property_id = p.id AND c.status = 'active') AS txt, (SELECT group_concat(o.value, ' ') FROM v2_feature_obs o WHERE o.property_id = p.id AND (o.field LIKE '%note%' OR o.field IN ('power','cranes'))) AS obs FROM v2_properties p WHERE p.id > ? ORDER BY p.id LIMIT 300").bind(Number(body.after || 0)).all();
      const stmts = results.map((p) => db.prepare("UPDATE v2_properties SET product_tags = ? WHERE id = ?").bind(productTags(p, `${p.txt || ""} ${p.obs || ""}`), p.id));
      for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
      return json({ done: results.length, last: results.length ? results[results.length - 1].id : null });
    }

    // Re-run validation on stored comps (after rule changes).
    if (path === "/api/v2/admin/revalidate" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const after = Number(body.after || 0);
      const { results } = await db.prepare("SELECT c.id, c.record_kind, c.snapshot_json, c.flags_json FROM v2_comps c WHERE c.id > ? ORDER BY c.id LIMIT 400").bind(after).all();
      const stmts = [];
      for (const c of results) {
        const snap = JSON.parse(c.snapshot_json || "{}");
        const prev = JSON.parse(c.flags_json || "[]").filter((f) => ["unverified", "vision", "portfolio", "possible_duplicate", "geocode_low", "geocode_failed"].includes(f.code));
        const r = validateRecord({ ...snap, record_kind: c.record_kind, _flags: prev });
        if (r.rent_basis == null) r.rent_basis = rentBasis(r.lease_structure);
        const td = txnDateOf(r);
        stmts.push(db.prepare(`UPDATE v2_comps SET ${COMP_FIELDS.map((f) => `${f} = ?`).join(", ")}, txn_date = ?, txn_year = ?, flags_json = ?, severity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`).bind(...COMP_FIELDS.map((f) => (r[f] === undefined ? null : r[f])), td, dateToNumber(td), JSON.stringify(r._flags), r._severity, c.id));
      }
      for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
      return json({ done: results.length, last: results.length ? results[results.length - 1].id : null });
    }

    // Global dedupe sweep: merge duplicate properties, then duplicate deals.
    // Safe to re-run; catches races between concurrent uploads and old data.
    if (path === "/api/v2/admin/dedupe-sweep" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const after = Number(body.after || 0);
      const dry = !!body.dry;
      const { results: props } = await db.prepare("SELECT * FROM v2_properties WHERE id > ? ORDER BY id LIMIT 120").bind(after).all();
      const log = [];
      for (const p of props) {
        const still = await db.prepare("SELECT id FROM v2_properties WHERE id = ?").bind(p.id).first();
        if (!still || p.latitude == null) continue;
        const b = bboxOf(p.latitude, p.longitude, 0.25);
        const { results: near } = await db.prepare("SELECT * FROM v2_properties WHERE id > ? AND latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?").bind(p.id, b.minLat, b.maxLat, b.minLng, b.maxLng).all();
        for (const q of near) {
          const sc = samePropertyScore(p, q);
          if (sc.score < 0.85) continue;
          log.push({ merge_property: q.id, into: p.id, a: p.address, b: q.address, reasons: sc.reasons });
          if (!dry) {
            await db.batch([
              db.prepare("UPDATE v2_comps SET property_id = ?, updated_at = CURRENT_TIMESTAMP WHERE property_id = ?").bind(p.id, q.id),
              db.prepare("UPDATE v2_feature_obs SET property_id = ? WHERE property_id = ?").bind(p.id, q.id),
              db.prepare(`UPDATE v2_properties SET property_name = COALESCE(property_name, ?), zip = COALESCE(zip, ?), submarket = COALESCE(submarket, ?), costar_prop_id = COALESCE(costar_prop_id, ?), parcel_id = COALESCE(parcel_id, ?) WHERE id = ?`).bind(q.property_name, q.zip, q.submarket, q.costar_prop_id, q.parcel_id, p.id),
              db.prepare("DELETE FROM v2_properties WHERE id = ?").bind(q.id),
            ]);
          }
        }
        // deal-level duplicates at this property
        const { results: comps } = await db.prepare("SELECT * FROM v2_comps WHERE property_id = ? AND status = 'active' ORDER BY id").bind(p.id).all();
        const dead = new Set();
        for (let i = 0; i < comps.length; i++) {
          if (dead.has(comps[i].id)) continue;
          for (let k = i + 1; k < comps.length; k++) {
            if (dead.has(comps[k].id)) continue;
            const d = sameDealScore(comps[i], comps[k]);
            if (d.verdict !== "duplicate") continue;
            dead.add(comps[k].id);
            log.push({ duplicate_comp: comps[k].id, of: comps[i].id, reasons: d.reasons });
            if (!dry) {
              const fill = mergeMissing(comps[i], comps[k], COMP_FIELDS);
              const src = [...JSON.parse(comps[i].sources_json || "[]"), ...JSON.parse(comps[k].sources_json || "[]")];
              const keys = Object.keys(fill);
              await db.batch([
                db.prepare(`UPDATE v2_comps SET ${[...keys.map((x) => `${x} = ?`), "sources_json = ?", "updated_at = CURRENT_TIMESTAMP"].join(", ")} WHERE id = ?`).bind(...keys.map((x) => fill[x]), JSON.stringify(src), comps[i].id),
                db.prepare("UPDATE v2_comps SET status = 'duplicate', dup_of = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(comps[i].id, comps[k].id),
              ]);
              Object.assign(comps[i], fill);
            }
          }
        }
        if (!dry && log.some((l) => l.into === p.id)) await refreshProperty(db, p.id);
      }
      return json({ scanned: props.length, last: props.length ? props[props.length - 1].id : null, actions: log });
    }

    // Recompute resolved features for properties (after resolver changes).
    if (path === "/api/v2/admin/refresh" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const { results } = await db.prepare("SELECT id FROM v2_properties WHERE id > ? ORDER BY id LIMIT 150").bind(Number(body.after || 0)).all();
      for (const r of results) await refreshProperty(db, r.id);
      return json({ done: results.length, last: results.length ? results[results.length - 1].id : null });
    }

    // Re-run resolve + commit for jobs with held records.
    if (path === "/api/v2/admin/jobs" && req.method === "GET") {
      const { results } = await db.prepare("SELECT j.id, j.status, f.filename, (SELECT count(*) FROM v2_staged s WHERE s.job_id = j.id AND s.decision = 'review') AS held FROM v2_jobs j LEFT JOIN v2_files f ON f.id = j.file_id ORDER BY j.created_at").all();
      return json({ jobs: results });
    }

    // ---------------- read ----------------
    if (path === "/api/v2/comps" && req.method === "GET") {
      const q = url.searchParams;
      const where = ["c.status = 'active'"];
      const binds = [];
      const kind = q.get("kind");
      if (kind) {
        where.push(kind === "sale" ? "c.record_kind IN ('sale','land_sale')" : "c.record_kind = ?");
        if (kind !== "sale") binds.push(kind);
      }
      if (q.get("market")) { where.push("p.market = ?"); binds.push(q.get("market")); }
      if (q.get("source")) { where.push("c.source_kind = ?"); binds.push(q.get("source")); }
      if (q.get("severity")) { where.push("c.severity = ?"); binds.push(q.get("severity")); }
      if (q.get("since")) { where.push("c.txn_year >= ?"); binds.push(Number(q.get("since"))); }
      if (q.get("tag")) for (const tg of q.get("tag").split(",")) { where.push("p.product_tags LIKE ?"); binds.push(`%|${tg}|%`); }
      if (q.get("player")) { where.push("c.id IN (SELECT comp_id FROM v2_comp_players WHERE player_id = ?)"); binds.push(Number(q.get("player"))); }
      if (q.get("precision")) { where.push("c.date_precision = ?"); binds.push(q.get("precision")); }
      if (q.get("property")) { where.push("c.property_id = ?"); binds.push(Number(q.get("property"))); }
      if (q.get("min_sf")) { where.push("COALESCE(c.leased_sf, c.available_sf, p.building_sf) >= ?"); binds.push(Number(q.get("min_sf"))); }
      if (q.get("max_sf")) { where.push("COALESCE(c.leased_sf, c.available_sf, p.building_sf) <= ?"); binds.push(Number(q.get("max_sf"))); }
      if (q.get("since_date")) { where.push("c.txn_date_sort >= ?"); binds.push(q.get("since_date")); }
      if (q.get("ids")) { const ids = q.get("ids").split(",").map(Number).filter(Boolean).slice(0, 90); where.push(`c.id IN (${ids.map(() => "?").join(",") || "-1"})`); binds.push(...ids); }
      if (q.get("q")) {
        where.push("(p.address LIKE ? OR p.property_name LIKE ? OR c.tenant LIKE ? OR c.buyer LIKE ? OR c.seller LIKE ? OR p.city LIKE ? OR p.submarket LIKE ? OR c.source_label LIKE ?)");
        const like = `%${q.get("q")}%`;
        binds.push(like, like, like, like, like, like, like, like);
      }
      if (q.get("bbox")) {
        const [a, b, c2, d] = q.get("bbox").split(",").map(Number);
        where.push("p.latitude BETWEEN ? AND ? AND p.longitude BETWEEN ? AND ?");
        binds.push(a, c2, b, d);
      }
      const limit = Math.min(Number(q.get("limit") || 500), 5000);
      const offset = Number(q.get("offset") || 0);
      const order = { newest: "c.txn_date_sort DESC", oldest: "c.txn_date_sort ASC", added: "c.id DESC", price: "c.price_psf DESC", rent: "c.rent_psf_yr DESC", size: "COALESCE(c.leased_sf, p.building_sf) DESC" }[q.get("sort") || "newest"] || "c.txn_date_sort DESC";
      const sql = `SELECT c.id, c.record_kind, c.property_id, c.source_kind, c.source_label, c.txn_date, c.txn_year, c.txn_date_sort, c.date_precision, c.rent_basis, c.net_effective_rent_psf_yr, c.lease_start, c.lease_end, c.landlord, c.occupancy_pct, c.created_at, p.product_tags, p.cranes, p.power, p.office_sf, p.drive_in_doors AS dri, p.owner, c.sale_price, c.price_psf, c.price_per_acre, c.cap_rate_pct, c.buyer, c.seller, c.sale_type, c.tenant, c.leased_sf, c.rent_psf_yr, c.rent_per_acre_mo, c.lease_structure, c.term_months, c.escalation_pct, c.free_rent_months, c.ti_psf, c.lease_kind, c.asking_rent_psf_yr, c.available_sf, c.severity, c.flags_json, c.notes,
        p.address, p.property_name, p.city, p.zip, p.market, p.submarket, p.latitude, p.longitude, p.geocode_quality, p.building_sf, p.land_acres, p.year_built, p.clear_height_ft, p.dock_doors, p.drive_in_doors, p.loading, p.construction, p.building_class, p.property_subtype, p.yard_acres, p.rail_served
        FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE ${where.join(" AND ")} ORDER BY ${order} NULLS LAST LIMIT ? OFFSET ?`;
      const { results } = await db.prepare(sql).bind(...binds, limit, offset).all();
      const total = await db.prepare(`SELECT count(*) AS n FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE ${where.join(" AND ")}`).bind(...binds).first();
      return json({ total: total.n, rows: results });
    }

    if ((mm = m(/^\/api\/v2\/properties\/(\d+)$/)) && req.method === "GET") {
      const p = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(Number(mm[1])).first();
      if (!p) return bad("not found", 404);
      const { results: comps } = await db.prepare("SELECT * FROM v2_comps WHERE property_id = ? ORDER BY txn_year DESC").bind(p.id).all();
      const { results: obs } = await db.prepare("SELECT field, value, source, source_ref, confidence, observed_at FROM v2_feature_obs WHERE property_id = ? ORDER BY field, confidence DESC").bind(p.id).all();
      return json({ property: { ...p, features: p.features_json ? JSON.parse(p.features_json) : null }, comps: comps.map((c) => ({ ...c, flags: JSON.parse(c.flags_json || "[]"), sources: JSON.parse(c.sources_json || "[]") })), observations: obs.map((o) => ({ ...o, value: (() => { try { return JSON.parse(o.value); } catch { return o.value; } })() })) });
    }

    // Manual edits to a property feature (highest confidence source).
    if ((mm = m(/^\/api\/v2\/properties\/(\d+)\/features$/)) && req.method === "POST") {
      const body = await req.json();
      const obs = Object.entries(body.features || {}).map(([field, value]) => ({ field, value, source: "manual", source_ref: `edited by ${userOf(req)}` }));
      const prev = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(Number(mm[1])).first();
      if (prev && obs.length) await db.batch(obs.map((o) => db.prepare("INSERT INTO v2_edits (entity, entity_id, field, old_value, new_value, reason, by_user) VALUES ('property', ?, ?, ?, ?, ?, ?)").bind(prev.id, o.field, prev[o.field] == null ? null : String(prev[o.field]), o.value == null ? null : String(o.value), body.reason || null, userOf(req))));
      await insertObservations(db, Number(mm[1]), obs);
      await refreshProperty(db, Number(mm[1]));
      return json({ ok: true });
    }

    if ((mm = m(/^\/api\/v2\/comps\/(\d+)$/)) && req.method === "PATCH") {
      const body = await req.json();
      const c = await db.prepare("SELECT * FROM v2_comps WHERE id = ?").bind(Number(mm[1])).first();
      if (!c) return bad("not found", 404);
      const who = userOf(req);
      if (body.status) {
        await db.prepare("UPDATE v2_comps SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(body.status, c.id).run();
        await db.prepare("INSERT INTO v2_edits (entity, entity_id, field, old_value, new_value, reason, by_user) VALUES ('comp', ?, 'status', ?, ?, ?, ?)").bind(c.id, c.status, body.status, body.reason || null, who).run();
      }
      if (body.fields) {
        const ek = Object.keys(body.fields).filter((k) => COMP_FIELDS.includes(k) && String(c[k] ?? "") !== String(body.fields[k] ?? ""));
        if (ek.length) await db.batch(ek.map((k) => db.prepare("INSERT INTO v2_edits (entity, entity_id, field, old_value, new_value, reason, by_user) VALUES ('comp', ?, ?, ?, ?, ?, ?)").bind(c.id, k, c[k] == null ? null : String(c[k]), body.fields[k] == null ? null : String(body.fields[k]), body.reason || null, who)));
        if (body.fields.sale_date !== undefined || body.fields.lease_start !== undefined) {
          const vr = validateRecord({ ...c, ...body.fields, record_kind: c.record_kind });
          const nd = txnDateOf(vr);
          await db.prepare("UPDATE v2_comps SET txn_date = ?, txn_year = ? WHERE id = ?").bind(nd || null, dateToNumber(nd), c.id).run();
        }
        const rec = validateRecord({ ...c, ...body.fields, record_kind: c.record_kind });
        const keys = Object.keys(body.fields).filter((k) => COMP_FIELDS.includes(k));
        if (keys.length) await db.prepare(`UPDATE v2_comps SET ${keys.map((k) => `${k} = ?`).join(", ")}, flags_json = ?, severity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`).bind(...keys.map((k) => rec[k]), JSON.stringify(rec._flags), rec._severity, c.id).run();
      }
      return json({ ok: true });
    }

    // ---------------- enrichment ----------------
    if (path === "/api/v2/enrich" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      let ids = body.ids;
      if (!ids) {
        const { results } = await db.prepare("SELECT id FROM v2_properties WHERE enrich_status = 'pending' AND latitude IS NOT NULL LIMIT ?").bind(Math.min(body.limit || 8, 25)).all();
        ids = results.map((r) => r.id);
      }
      const llm = body.aerial ? llmFor(env, url) : null;
      const out = await pool(ids, 4, (id) => enrichProperty(env, db, id, { aerial: !!body.aerial, web: !!body.web, llm }));
      return json({ enriched: out });
    }

    if (path === "/api/v2/admin/aerial") {
      const p = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(Number(url.searchParams.get("id"))).first();
      const dbg = {};
      const obs = await aerialObservations(env, llmFor(env, url), p, dbg);
      return json({ obs, dbg });
    }

    // ---------------- finder ----------------
    if (path === "/api/v2/finder" && req.method === "POST") {
      const body = await req.json();
      const llm = (() => { try { return llmFor(env, url); } catch { return null; } })();
      const llms = ["deepseek", "openai"].map((p) => { try { return makeLLM(env, { provider: p, noFallback: true }); } catch { return null; } }).filter(Boolean);
      const res = await findComps(env, db, llm, { query: body.query || "", location: body.location || null, filters: body.filters || {}, limit: Math.min(body.limit || 30, 100), llms });
      return json(res);
    }
    if (path === "/api/v2/finder/explain" && req.method === "POST") {
      const body = await req.json();
      const llm = makeLLM(env, { provider: env.DEEPSEEK_API_KEY ? "deepseek" : "openai" });
      return json(await explainComps(llm, body.query || "", body.parsed || {}, body.rows || []));
    }

    // ---------------- data quality ----------------
    if (path === "/api/v2/quality") {
      const flagged = await db.prepare("SELECT c.id, c.record_kind, c.severity, c.flags_json, c.source_label, c.txn_date, p.address, p.city, c.property_id FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.status = 'active' AND c.severity = 'error' ORDER BY c.id DESC LIMIT 300").all();
      const conflicts = await db.prepare("SELECT id, address, city, features_json FROM v2_properties WHERE features_json LIKE '%\"conflicts\":{\"%' LIMIT 300").all();
      const lowgeo = await db.prepare("SELECT count(*) AS n FROM v2_properties WHERE geocode_quality IN ('city','postal','street','failed') OR latitude IS NULL").first();
      const bySource = await db.prepare("SELECT source_kind, record_kind, count(*) AS n FROM v2_comps WHERE status='active' GROUP BY 1,2").all();
      return json({ errors: flagged.results.map((r) => ({ ...r, flags: JSON.parse(r.flags_json || "[]") })), feature_conflicts: conflicts.results.map((r) => { const f = JSON.parse(r.features_json); return { id: r.id, address: r.address, city: r.city, conflicts: f.conflicts, best: Object.fromEntries(Object.keys(f.conflicts).map((k) => [k, f.best[k]])) }; }), low_precision_locations: lowgeo.n, by_source: bySource.results });
    }

    if (path === "/api/v2/stats") {
      const r = await db.prepare(`SELECT p.market, c.record_kind, count(*) AS n, avg(c.price_psf) AS avg_psf, avg(c.cap_rate_pct) AS avg_cap, avg(c.rent_psf_yr) AS avg_rent, max(c.txn_date) AS latest
        FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.status='active' GROUP BY 1,2 ORDER BY 1,2`).all();
      const f = await db.prepare(`SELECT count(*) AS n, sum(clear_height_ft IS NOT NULL) AS clear, sum(year_built IS NOT NULL) AS yb, sum(land_acres IS NOT NULL) AS land, sum(building_sf IS NOT NULL) AS sf, sum(dock_doors IS NOT NULL) AS docks, sum(loading IS NOT NULL) AS loading, sum(construction IS NOT NULL) AS construction, sum(property_subtype IS NOT NULL) AS subtype FROM v2_properties`).first();
      return json({ by_market: r.results, feature_coverage: f });
    }

    if (path === "/api/v2/files") {
      const { results } = await db.prepare("SELECT f.id, f.filename, f.kind, f.uploaded_by, f.uploaded_at, f.summary, f.source_kind, f.via, f.via_ref, (SELECT count(*) FROM v2_comps c WHERE c.file_id = f.id AND c.status='active') AS comps, (SELECT j.id FROM v2_jobs j WHERE j.file_id = f.id ORDER BY j.created_at DESC LIMIT 1) AS job_id, (SELECT j.status FROM v2_jobs j WHERE j.file_id = f.id ORDER BY j.created_at DESC LIMIT 1) AS job_status, COALESCE((SELECT count(*) FROM v2_staged s JOIN v2_jobs j ON j.id = s.job_id WHERE j.file_id = f.id AND s.decision = 'review'), 0) AS held, f.status AS file_status FROM v2_files f WHERE f.kind != 'bulk' ORDER BY f.id DESC LIMIT 300").all();
      for (const r of results) r.status = r.held ? "review" : ["queued", "q_extracting", "needs_vision", "failed"].includes(r.job_status) ? r.job_status : r.file_status;
      return json({ files: results });
    }

    if (path === "/api/v2/export" && req.method === "POST") {
      const body = await req.json();
      const ids = (body.ids || []).map(Number).slice(0, 2000);
      if (!ids.length) return bad("no ids");
      const results = [];
      for (let x = 0; x < ids.length; x += 90) {
        const part = ids.slice(x, x + 90);
        const { results: rr } = await db.prepare(`SELECT c.*, p.address, p.property_name, p.city, p.zip, p.submarket, p.market, p.latitude, p.longitude, p.building_sf, p.land_acres, p.year_built, p.clear_height_ft, p.dock_doors, p.drive_in_doors, p.loading, p.construction, p.building_class, p.property_subtype FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.id IN (${part.map(() => "?").join(",")})`).bind(...part).all();
        results.push(...rr);
      }
      const cols = ["record_kind", "address", "property_name", "city", "zip", "submarket", "txn_date", "building_sf", "leased_sf", "year_built", "clear_height_ft", "dock_doors", "drive_in_doors", "loading", "construction", "building_class", "property_subtype", "land_acres", "sale_price", "price_psf", "cap_rate_pct", "buyer", "seller", "sale_type", "tenant", "rent_psf_yr", "lease_structure", "term_months", "escalation_pct", "free_rent_months", "ti_psf", "lease_kind", "source_label", "notes", "latitude", "longitude"];
      const order = new Map(ids.map((id, i) => [id, i]));
      results.sort((a, b) => order.get(a.id) - order.get(b.id));
      const esc = (v) => (v == null ? "" : /[",\n]/.test(String(v)) ? `"${String(v).replace(/"/g, '""')}"` : String(v));
      if (body.format === "xlsx") {
        const H = { record_kind: "Type", address: "Address", property_name: "Property", city: "City", zip: "Zip", submarket: "Submarket", txn_date: "Date", building_sf: "Building SF", leased_sf: "Leased SF", year_built: "Year Built", clear_height_ft: "Clear (ft)", dock_doors: "Docks", drive_in_doors: "Grade-level", loading: "Loading", construction: "Construction", building_class: "Class", property_subtype: "Product", land_acres: "Acres", sale_price: "Sale Price", price_psf: "Price/SF", cap_rate_pct: "Cap %", buyer: "Buyer", seller: "Seller", sale_type: "Sale Type", tenant: "Tenant", rent_psf_yr: "Rent $/SF/yr", lease_structure: "Lease Type", term_months: "Term (mo)", escalation_pct: "Bumps %", free_rent_months: "Free (mo)", ti_psf: "TI $/SF", lease_kind: "New/Renewal", source_label: "Source", notes: "Notes", latitude: "Lat", longitude: "Lng" };
        const aoa = [["Harbor Capital · Comps export " + new Date().toISOString().slice(0, 10)], [], cols.map((c) => H[c] || c), ...results.map((r) => cols.map((c) => (r[c] == null ? "" : r[c])))];
        const ws = XLSX.utils.aoa_to_sheet(aoa);
        ws["!cols"] = cols.map((c) => ({ wch: ["address", "source_label", "notes", "buyer", "seller", "tenant"].includes(c) ? 28 : 12 }));
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Comps");
        return new Response(XLSX.write(wb, { type: "array", bookType: "xlsx" }), { headers: { "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "content-disposition": `attachment; filename="harbor-comps-${new Date().toISOString().slice(0, 10)}.xlsx"` } });
      }
      const csv = [cols.join(","), ...results.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
      return new Response(csv, { headers: { "content-type": "text/csv", "content-disposition": `attachment; filename="harbor-comps-${new Date().toISOString().slice(0, 10)}.csv"` } });
    }

    return bad("not found", 404);
  } catch (e) {
    if (e.quota || /AI_CREDITS_EXHAUSTED/.test(String(e.message))) return json({ error: String(e.message).replace("AI_CREDITS_EXHAUSTED: ", ""), code: "ai_credits" }, 402);
    return json({ error: String(e.message || e), stack: String(e.stack || "").split("\n").slice(0, 4) }, 500);
  }
}

// Address-less comps (common in broker "mid bulk" sheets): place them at the
// submarket (gazetteer) or city centroid so they still show up in area searches.
async function locateArea(db, r, ctxCity, ctxMarket) {
  const name = String(r.submarket || "").trim().toLowerCase();
  if (name) {
    const row = await db.prepare("SELECT * FROM v2_submarkets WHERE lower(name) = ? ORDER BY n DESC LIMIT 1").bind(name).first().catch(() => null);
    if (row) return { lat: row.latitude, lng: row.longitude };
    const key = Object.keys(ALIASES).find((a) => a === name || name.includes(a));
    if (key) return { lat: ALIASES[key][1], lng: ALIASES[key][2] };
  }
  const city = r.city || ctxCity;
  if (city) {
    const g = await geocode({}, { address: null, propertyName: `${city}`, state: "TX" }).catch(() => null);
    if (g && g.lat != null) return { lat: g.lat, lng: g.lng };
  }
  return null;
}

function pick(c) {
  const o = {};
  for (const k of ["id", "record_kind", "txn_date", "sale_price", "price_psf", "cap_rate_pct", "buyer", "tenant", "leased_sf", "rent_psf_yr", "term_months", "source_label"]) o[k] = c[k];
  return o;
}

export async function enrichProperty(env, db, id, { aerial = false, web = false, llm = null } = {}) {
  const p = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(id).first();
  if (!p) return { id, error: "not found" };
  const obs = [];
  const notes = [];
  // Re-runs replace earlier machine observations from the same sources.
  await db.prepare("DELETE FROM v2_feature_obs WHERE property_id = ? AND source IN ('costar','cad','parcel'" + (aerial ? ",'aerial'" : "") + (web ? ",'web'" : "") + ")").bind(id).run();
  try {
    const cs = await matchCostar(db, p);
    if (cs) {
      obs.push(...costarObservations(cs.row));
      await db.prepare("UPDATE v2_properties SET costar_prop_id = ?, submarket = COALESCE(submarket, ?) WHERE id = ?").bind(cs.row.prop_id, cs.row.submarket, id).run();
      notes.push(`CoStar match ${cs.row.prop_id} (${Math.round(cs.distanceMi * 5280)} ft)`);
    }
  } catch (e) {
    notes.push("costar: " + e.message);
  }
  try {
    const { obs: po, meta } = await parcelObservations(db, p);
    obs.push(...po.filter((o) => ["building_sf", "land_acres", "year_built", "construction", "property_subtype"].includes(o.field) || o.field.startsWith("cad_")));
    if (meta) {
      await db.prepare("UPDATE v2_properties SET parcel_id = COALESCE(?, parcel_id), owner = COALESCE(?, owner), county = COALESCE(county, ?) WHERE id = ?").bind(meta.parcel_id, meta.owner, meta.county, id).run();
      notes.push(`parcel ${meta.county || ""} ${meta.parcel_id || ""}`);
    }
  } catch (e) {
    notes.push("parcel: " + e.message);
  }
  if (aerial && llm) obs.push(...(await aerialObservations(env, llm, p).catch(() => [])));
  if (web) obs.push(...(await webObservations(env, p).catch(() => [])));
  // Only property columns are resolved; cad_* and notes stay as observations.
  await insertObservations(db, id, obs);
  await refreshProperty(db, id);
  await db.prepare("UPDATE v2_properties SET enrich_status = ?, enriched_at = CURRENT_TIMESTAMP WHERE id = ?").bind(obs.length ? "done" : "partial", id).run();
  return { id, observations: obs.length, notes };
}

export { UI_HTML };
