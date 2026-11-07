// Deals: "comp this deal". A subject (from a Slack write-up, an OM or an
// address) plus its comp set, with suggested comps per lane (sale / lease).
//
// Subject facts keep their source. The write-up always wins over database
// facts; disagreements are kept as conflicts and shown to the analyst.

import * as XLSX from "xlsx";
import { makeLLM } from "./llm.js";
import { geocode, haversineMi, marketFor } from "./geo.js";
import { findComps, describeSubject, heuristicParse, geoCache, summarize, ALIASES, saleSf } from "./finder.js";
import { subjectTags, tagList, TAGS } from "./tags.js";

const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
const bad = (msg, status = 400) => json({ error: msg }, status);

export const SUBJECT_FIELDS = ["building_sf", "land_acres", "clear_height_ft", "year_built", "year_renovated", "construction", "loading", "dock_doors", "drive_in_doors", "cranes", "power", "rail_served", "yard_acres", "building_class", "tenancy", "occupancy_pct", "office_pct", "sprinkler", "property_subtype", "tenant", "walt_years", "asking_price", "price_psf", "cap_rate_pct", "in_place_rent_psf_yr"];
const NUMERIC = new Set(["building_sf", "land_acres", "clear_height_ft", "year_built", "year_renovated", "dock_doors", "drive_in_doors", "yard_acres", "occupancy_pct", "office_pct", "walt_years", "asking_price", "price_psf", "cap_rate_pct", "in_place_rent_psf_yr"]);

const SUBJECT_SYSTEM = `You read a Harbor Capital deal write-up (Slack investment summary, broker email, OM excerpt or a comp request) about a Texas industrial property and pull out the SUBJECT property's facts and what comps are needed. Only facts stated in the text. Return JSON only.
Vocabulary: "45k SF" = 45000; "28' clear"; DH dock-high; GL/drive-in/ramps grade-level; RL rear-load, FL front-load, cross-dock; tilt-wall/metal/masonry; IOS outdoor storage; crane-served / crane-ready (keep tonnage text); heavy power (keep amps/KVA text); WALT years; "$X ($Y/SF)" price and price per SF; cap rate %; rents under ~$2 are $/SF/month (convert to yearly x12).`;

function subjectUser(text, today) {
  return `Today is ${today}. Text:
"""${text.slice(0, 12000)}"""
Return:
{"name": "short deal name: street address or property/portfolio name",
 "address": "street address of the subject or null", "city": null, "submarket": "submarket/corridor named or null", "market": "Houston|DFW|San Antonio|Austin|Laredo|El Paso|null",
 "lanes": ["sale","lease"]  (comps needed: a deal to buy/price usually needs both; "lease comps" only -> ["lease"]; "sale comps" only -> ["sale"]),
 "facts": {"<field>": {"value": <number|string|bool>, "quote": "exact words from the text"}} for fields among: ${SUBJECT_FIELDS.join(", ")}
   (construction one of tilt_wall|metal|masonry|concrete_block|precast|steel; loading one of rear|front|cross_dock|side; building_class A|B|C; tenancy single|multi; property_subtype one of warehouse|bulk_distribution|shallow_bay|multi_tenant|flex|manufacturing|ios|cold_storage|service; cranes/power as short text),
 "search": {"size_sf": {"min": n|null, "max": n|null}, "radius_mi": n|null, "since": "YYYY-MM-DD"|null},
 "portfolio": true|false}
For a portfolio use the total SF and the main location. Omit facts not stated.`;
}

function heuristicSubject(text) {
  const h = heuristicParse(text);
  const facts = {};
  const q = (re) => re.exec(text);
  const put = (f, v, m) => { if (v != null && !Number.isNaN(v)) facts[f] = { value: v, quote: m ? m[0] : null }; };
  let m;
  if (h.size_sf?.target) put("building_sf", h.size_sf.target, q(/[\d,.]+\s*k?\s*(?:sf|sq)/i));
  if (h.clear_height_ft?.target) put("clear_height_ft", h.clear_height_ft.target, q(/\d{2}\s*['’]?\s*clear/i));
  if ((m = q(/(\d+(?:\.\d+)?)\s*(?:\+\/-\s*)?(?:ac|acres)\b/i))) put("land_acres", +m[1], m);
  if ((m = q(/built\s*(?:in\s*)?((?:19|20)\d{2})/i))) put("year_built", +m[1], m);
  if ((m = q(/(\d+)\s*(?:dock[- ]high|docks?\b|DH\b)/i))) put("dock_doors", +m[1], m);
  if ((m = q(/(\d+)\s*(?:drive[- ]ins?|grade[- ]level|GL\b)/i))) put("drive_in_doors", +m[1], m);
  if ((m = q(/crane[^.\n]{0,60}/i))) put("cranes", m[0].slice(0, 60), m);
  if ((m = q(/(\d+(?:\.\d+)?)\s*%\s*cap/i))) put("cap_rate_pct", +m[1], m);
  if (h.construction?.[0]) facts.construction = { value: h.construction[0], quote: null };
  if (h.loading?.[0]) facts.loading = { value: h.loading[0], quote: null };
  return { name: h.subject?.address || text.slice(0, 40), address: h.subject?.address || null, submarket: h.subject?.location_text || null, market: h.subject?.market || null, lanes: h.comp_types?.filter((x) => x === "sale" || x === "lease").length ? h.comp_types.filter((x) => x === "sale" || x === "lease") : ["sale", "lease"], facts, search: { size_sf: h.size_sf?.min || h.size_sf?.max ? { min: h.size_sf.min ?? null, max: h.size_sf.max ?? null } : null, radius_mi: h.subject?.radius_mi || null, since: h.since || null }, _parser: "heuristic" };
}

export async function readSubject(env, text) {
  const today = new Date().toISOString().slice(0, 10);
  const llms = ["deepseek", "openai"].map((p) => { try { return makeLLM(env, { provider: p, noFallback: true }); } catch { return null; } }).filter(Boolean);
  if (llms.length) {
    const tries = llms.map((m) => m.chat({ system: SUBJECT_SYSTEM, user: subjectUser(text, today), tier: "fast", reasoning: "off", maxTokens: 2500, timeoutMs: 25000 }).then((r) => { if (r && (r.facts || r.address || r.name)) return { ...r, _parser: `llm:${m.provider}` }; throw new Error("bad"); }));
    try {
      return await Promise.race([Promise.any(tries), new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 26000))]);
    } catch {}
  }
  return heuristicSubject(text);
}

function differs(a, b) {
  if (a == null || b == null) return false;
  if (typeof a === "number" || typeof b === "number") {
    const x = Number(a), y = Number(b);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return String(a) !== String(b);
    return Math.abs(x - y) / Math.max(Math.abs(x), Math.abs(y), 1) > 0.1;
  }
  return String(a).toLowerCase() !== String(b).toLowerCase();
}

// Build subject {field: {value, source, ref}} + conflicts from text facts and DB facts.
function mergeSubject(textFacts, dbDetail) {
  const out = {};
  const conflicts = {};
  for (const [f, v] of Object.entries(textFacts || {})) {
    if (!SUBJECT_FIELDS.includes(f) || v == null || v.value == null || v.value === "") continue;
    let val = v.value;
    if (NUMERIC.has(f)) { val = Number(String(val).replace(/[^\d.-]/g, "")); if (!Number.isFinite(val)) continue; }
    out[f] = { value: val, source: "write-up", ref: v.quote || null };
  }
  for (const [f, d] of Object.entries(dbDetail || {})) {
    if (!SUBJECT_FIELDS.includes(f) || d.value == null) continue;
    if (!out[f]) out[f] = { value: d.value, source: d.source, ref: d.ref || null };
    else if (out[f].source === "write-up" && !["cranes", "power", "tenant", "property_subtype", "sprinkler"].includes(f) && differs(out[f].value, d.value)) conflicts[f] = { value: d.value, source: d.source, ref: d.ref || null };
  }
  return { facts: out, conflicts };
}

async function locate(env, db, sub) {
  if (sub.address) {
    const g = await geocode(env, { address: sub.address, city: sub.city || null, state: "TX" }, { cache: geoCache(db) });
    if (g.lat != null && !["city", "failed"].includes(g.quality)) return { lat: g.lat, lng: g.lng, quality: g.quality, label: g.formatted || sub.address, city: g.city || sub.city || null };
  }
  const name = String(sub.submarket || "").toLowerCase().trim();
  if (name) {
    const row = await db.prepare("SELECT * FROM v2_submarkets WHERE lower(name) = ? OR lower(aliases) LIKE ? LIMIT 1").bind(name, `%|${name}|%`).first().catch(() => null);
    if (row) return { lat: row.latitude, lng: row.longitude, quality: "submarket", label: `${row.name} (${row.market})` };
    const key = Object.keys(ALIASES).sort((a, b) => b.length - a.length).find((a) => name === a || name.includes(a));
    if (key) return { lat: ALIASES[key][1], lng: ALIASES[key][2], quality: "submarket", label: key };
  }
  return null;
}

export async function createDeal(env, db, user, body) {
  const text = String(body.text || "").trim();
  const address = String(body.address || "").trim();
  if (!text && !address) throw new Error("paste a write-up or type an address");
  const sub = text ? await readSubject(env, text) : { name: address, address, facts: {}, lanes: body.lanes || ["sale", "lease"], search: {} };
  if (address && !sub.address) sub.address = address;
  const loc = await locate(env, db, sub);
  let dbs = null;
  if (loc && !["submarket"].includes(loc.quality)) dbs = await describeSubject(db, { kind: "address", quality: "rooftop", lat: loc.lat, lng: loc.lng }, { subject: { address: sub.address } }).catch(() => null);
  const { facts, conflicts } = mergeSubject(sub.facts, dbs?.detail);
  const lanes = (body.lanes || sub.lanes || ["sale", "lease"]).filter((x) => x === "sale" || x === "lease");
  const search = { radius_mi: sub.search?.radius_mi || null, since: sub.search?.since || null, size_sf: sub.search?.size_sf && (sub.search.size_sf.min || sub.search.size_sf.max) ? sub.search.size_sf : null };
  const subject = { facts, conflicts, tags: subjectTags(Object.fromEntries(Object.entries(facts).map(([k, v]) => [k, v.value])), text), parser: sub._parser || null, portfolio: !!sub.portfolio };
  const r = await db
    .prepare(`INSERT INTO v2_deals (name, address, city, market, submarket, latitude, longitude, geocode_quality, property_id, lanes, source_text, subject_json, search_json, created_by, last_seen_comp_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT max(id) FROM v2_comps)) RETURNING id`)
    .bind(String(body.name || sub.name || sub.address || "Untitled deal").slice(0, 120), sub.address || null, loc?.city || sub.city || null, (loc ? marketFor(loc.lat, loc.lng) : null) || sub.market || null, sub.submarket || null, loc?.lat ?? null, loc?.lng ?? null, loc?.quality || null, dbs?.property_id || null, lanes.join(","), text || null, JSON.stringify(subject), JSON.stringify(search), user)
    .first();
  return r.id;
}

function subjectValues(subject) {
  return Object.fromEntries(Object.entries(subject.facts || {}).map(([k, v]) => [k, v.value]));
}

function threeYearsAgo() {
  const d = new Date();
  d.setFullYear(d.getFullYear() - 3);
  return d.toISOString().slice(0, 10);
}

export async function suggestions(env, db, deal, lane, { limit = 40 } = {}) {
  if (deal.latitude == null) return { rows: [], history: [], note: "Add an address or submarket so comps can be searched around it." };
  const subject = JSON.parse(deal.subject_json || "{}");
  const search = JSON.parse(deal.search_json || "{}");
  const s = subjectValues(subject);
  const tags = search.tags || subject.tags || [];
  const sf = s.building_sf || null;
  const parsed = {
    comp_types: [lane],
    subject: { market: deal.market || null },
    size_sf: search.size_sf ? { min: search.size_sf.min ?? null, max: search.size_sf.max ?? null } : sf ? { target: sf } : {},
    land_acres: tags.includes("ios") && s.land_acres ? { target: s.land_acres } : {},
    clear_height_ft: s.clear_height_ft ? { target: s.clear_height_ft } : {},
    year_built: s.year_built ? { target: s.year_renovated && s.year_renovated > s.year_built + 15 ? Math.round((s.year_built + s.year_renovated) / 2) : s.year_built } : {},
    loading: s.loading ? [s.loading] : [],
    construction: s.construction ? [s.construction] : [],
    building_class: [],
    tags_want: tags,
    product_gate: search.product_gate !== false,
    since: search.since || threeYearsAgo(),
    strict: [search.size_sf && search.size_sf.min && search.size_sf.max ? "size_sf" : null, "since"].filter(Boolean),
    needs: { cranes: tags.includes("crane"), heavy_power: tags.includes("heavy_power"), yard: tags.includes("yard") || tags.includes("ios"), rail: tags.includes("rail") },
    priorities: ["product"],
  };
  const location = { lat: deal.latitude, lng: deal.longitude, radius_mi: search.radius_mi || (deal.geocode_quality === "submarket" ? 6 : 5), label: deal.address || deal.submarket || deal.name };
  const res = await findComps(env, db, null, { parsed, location, limit: limit + 15, skipSubject: true, perProperty: 2 });
  const history = [];
  const rows = [];
  for (const r of res.results) {
    if ((deal.property_id && r.property_id === deal.property_id) || (r.distance_mi != null && r.distance_mi < 0.03)) history.push(r);
    else rows.push(r);
  }
  return { rows: rows.slice(0, limit), history, location: res.location, matched: res.matched, candidates: res.candidates };
}

const COMP_SELECT = `SELECT c.id, c.record_kind, c.property_id, c.source_kind, c.source_label, c.txn_date, c.txn_date_sort, c.date_precision, c.txn_year, c.sale_price, c.price_psf, c.price_per_acre, c.cap_rate_pct, c.buyer, c.seller, c.sale_type, c.occupancy_pct,
  c.tenant, c.leased_sf, c.rent_psf_yr, c.rent_psf_mo, c.rent_per_acre_mo, c.lease_structure, c.rent_basis, c.term_months, c.escalation_pct, c.free_rent_months, c.ti_psf, c.lease_kind, c.lease_start, c.net_effective_rent_psf_yr,
  c.asking_rent_psf_yr, c.available_sf, c.severity, c.flags_json, c.notes, c.created_at,
  p.address, p.property_name, p.city, p.zip, p.market, p.submarket, p.latitude, p.longitude, p.geocode_quality, p.building_sf, p.land_acres, p.year_built, p.clear_height_ft, p.dock_doors, p.drive_in_doors,
  p.loading, p.construction, p.building_class, p.property_subtype, p.office_sf, p.office_pct, p.yard_acres, p.rail_served, p.cranes, p.power, p.product_tags
  FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id`;

async function dealItems(db, deal) {
  const { results: items } = await db.prepare("SELECT * FROM v2_set_items WHERE deal_id = ? ORDER BY COALESCE(position, 1e9), at").bind(deal.id).all();
  if (!items.length) return [];
  const ids = items.map((i) => i.comp_id);
  const rows = [];
  for (let i = 0; i < ids.length; i += 90) {
    const chunk = ids.slice(i, i + 90);
    const { results } = await db.prepare(`${COMP_SELECT} WHERE c.id IN (${chunk.map(() => "?").join(",")})`).bind(...chunk).all();
    rows.push(...results);
  }
  const byId = new Map(rows.map((r) => [r.id, r]));
  return items
    .map((it) => {
      const c = byId.get(it.comp_id);
      if (!c) return null;
      const dist = deal.latitude != null && c.latitude != null ? +haversineMi(deal.latitude, deal.longitude, c.latitude, c.longitude).toFixed(2) : null;
      return { ...it, adjust: it.adjust_json ? JSON.parse(it.adjust_json) : null, comp: { ...c, distance_mi: dist, size_sf: c.record_kind === "lease" ? c.leased_sf ?? c.building_sf : c.record_kind === "listing" ? c.available_sf ?? c.building_sf : saleSf(c), flags: JSON.parse(c.flags_json || "[]").filter((f) => f.severity !== "info").map((f) => f.msg), flags_json: undefined } };
    })
    .filter(Boolean);
}

function dealOut(d) {
  return { ...d, subject: JSON.parse(d.subject_json || "{}"), search: JSON.parse(d.search_json || "{}"), lanes: String(d.lanes || "sale,lease").split(",").filter(Boolean), subject_json: undefined, search_json: undefined };
}

export async function handleDeals(req, env, ctx, path, user) {
  const db = env.COMPS_DB;
  const url = new URL(req.url);
  let mm;
  if (path === "/api/v2/deals" && req.method === "GET") {
    const status = url.searchParams.get("status") || "active";
    const { results } = await db
      .prepare(`SELECT d.id, d.name, d.address, d.city, d.market, d.submarket, d.lanes, d.status, d.watch, d.created_by, d.created_at, d.updated_at,
        (SELECT count(*) FROM v2_set_items i JOIN v2_comps c ON c.id = i.comp_id WHERE i.deal_id = d.id AND i.state = 'add' AND c.record_kind IN ('sale','land_sale')) AS n_sale,
        (SELECT count(*) FROM v2_set_items i JOIN v2_comps c ON c.id = i.comp_id WHERE i.deal_id = d.id AND i.state = 'add' AND c.record_kind IN ('lease','listing')) AS n_lease,
        (SELECT count(*) FROM v2_alerts a WHERE a.kind = 'deal' AND a.ref_id = d.id AND a.seen_at IS NULL) AS n_alerts
        FROM v2_deals d WHERE d.status = ? ORDER BY d.updated_at DESC LIMIT 300`)
      .bind(status)
      .all();
    return json({ deals: results.map((d) => ({ ...d, lanes: String(d.lanes || "").split(",").filter(Boolean) })) });
  }
  if (path === "/api/v2/deals" && req.method === "POST") {
    const body = await req.json();
    const id = await createDeal(env, db, user, body);
    return json({ id });
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)$/.exec(path))) {
    const id = Number(mm[1]);
    const deal = await db.prepare("SELECT * FROM v2_deals WHERE id = ?").bind(id).first();
    if (!deal) return bad("deal not found", 404);
    if (req.method === "GET") {
      const out = dealOut(deal);
      const items = await dealItems(db, deal);
      const inSet = new Map(items.map((i) => [i.comp_id, i.state]));
      const lanes = {};
      if (url.searchParams.get("suggest") !== "0") {
        for (const lane of out.lanes) {
          const s = await suggestions(env, db, deal, lane);
          lanes[lane] = { ...s, rows: s.rows.map((r) => ({ ...r, set_state: inSet.get(r.id) || null })) };
        }
      }
      const { results: alerts } = await db.prepare("SELECT * FROM v2_alerts WHERE kind = 'deal' AND ref_id = ? AND seen_at IS NULL ORDER BY id DESC LIMIT 5").bind(id).all();
      const { results: edits } = await db.prepare("SELECT * FROM v2_edits WHERE entity = 'deal' AND entity_id = ? ORDER BY id DESC LIMIT 30").bind(id).all();
      return json({ deal: out, items, lanes, alerts: alerts.map((a) => ({ ...a, comp_ids: JSON.parse(a.comp_ids_json || "[]") })), edits, tag_labels: TAGS });
    }
    if (req.method === "PATCH") {
      const body = await req.json();
      const sets = [];
      const binds = [];
      const edits = [];
      for (const f of ["name", "status", "watch", "slack_channel", "hubspot_deal_id", "address"]) if (body[f] !== undefined) { sets.push(`${f} = ?`); binds.push(body[f]); edits.push([f, deal[f], body[f]]); }
      if (body.lanes) { sets.push("lanes = ?"); binds.push(body.lanes.filter((x) => x === "sale" || x === "lease").join(",")); }
      if (body.search) { const s = { ...JSON.parse(deal.search_json || "{}"), ...body.search }; sets.push("search_json = ?"); binds.push(JSON.stringify(s)); edits.push(["search", deal.search_json, JSON.stringify(s)]); }
      if (body.subject) {
        const subj = JSON.parse(deal.subject_json || "{}");
        subj.facts = subj.facts || {};
        for (const [f, v] of Object.entries(body.subject)) {
          if (!SUBJECT_FIELDS.includes(f)) continue;
          const old = subj.facts[f]?.value ?? null;
          if (v === null || v === "") delete subj.facts[f];
          else subj.facts[f] = { value: NUMERIC.has(f) ? Number(v) : v, source: "manual", ref: `edited by ${user}` };
          if (subj.conflicts) delete subj.conflicts[f];
          edits.push([`subject.${f}`, old, v]);
        }
        if (body.tags) subj.tags = body.tags;
        else subj.tags = subjectTags(subjectValues(subj), deal.source_text || "");
        sets.push("subject_json = ?");
        binds.push(JSON.stringify(subj));
      } else if (body.tags) {
        const subj = JSON.parse(deal.subject_json || "{}");
        edits.push(["tags", JSON.stringify(subj.tags || []), JSON.stringify(body.tags)]);
        subj.tags = body.tags;
        sets.push("subject_json = ?");
        binds.push(JSON.stringify(subj));
      }
      if (body.address && body.address !== deal.address) {
        const loc = await locate(env, db, { address: body.address });
        if (loc) { sets.push("latitude = ?", "longitude = ?", "geocode_quality = ?", "market = ?"); binds.push(loc.lat, loc.lng, loc.quality, marketFor(loc.lat, loc.lng)); }
      }
      if (sets.length) {
        sets.push("updated_at = CURRENT_TIMESTAMP");
        await db.prepare(`UPDATE v2_deals SET ${sets.join(", ")} WHERE id = ?`).bind(...binds, id).run();
        if (edits.length) await db.batch(edits.map(([f, o, n]) => db.prepare("INSERT INTO v2_edits (entity, entity_id, field, old_value, new_value, by_user) VALUES ('deal', ?, ?, ?, ?, ?)").bind(id, f, o == null ? null : String(o), n == null ? null : String(n), user)));
      }
      return json({ ok: true });
    }
    if (req.method === "DELETE") {
      await db.prepare("UPDATE v2_deals SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(id).run();
      return json({ ok: true });
    }
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)\/items$/.exec(path)) && req.method === "POST") {
    const id = Number(mm[1]);
    const body = await req.json();
    const ids = (body.comp_ids || [body.comp_id]).map(Number).filter(Boolean);
    const state = body.state === "pass" ? "pass" : "add";
    const stmts = ids.map((cid) =>
      db.prepare(`INSERT INTO v2_set_items (deal_id, comp_id, state, reason, note, score, by_user, position) VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(max(position), 0) + 1 FROM v2_set_items WHERE deal_id = ?))
        ON CONFLICT(deal_id, comp_id) DO UPDATE SET state = excluded.state, reason = COALESCE(excluded.reason, reason), by_user = excluded.by_user, at = CURRENT_TIMESTAMP`).bind(id, cid, state, body.reason || null, body.note || null, body.score ?? null, user, id),
    );
    stmts.push(db.prepare("UPDATE v2_deals SET updated_at = CURRENT_TIMESTAMP WHERE id = ?").bind(id));
    await db.batch(stmts);
    return json({ ok: true, n: ids.length });
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)\/items\/(\d+)$/.exec(path))) {
    const [id, cid] = [Number(mm[1]), Number(mm[2])];
    if (req.method === "DELETE") {
      await db.prepare("DELETE FROM v2_set_items WHERE deal_id = ? AND comp_id = ?").bind(id, cid).run();
      return json({ ok: true });
    }
    if (req.method === "PATCH") {
      const body = await req.json();
      const sets = [];
      const binds = [];
      if (body.note !== undefined) { sets.push("note = ?"); binds.push(body.note || null); }
      if (body.adjust !== undefined) { sets.push("adjust_json = ?"); binds.push(body.adjust ? JSON.stringify(body.adjust) : null); }
      if (body.position !== undefined) { sets.push("position = ?"); binds.push(body.position); }
      if (body.state) { sets.push("state = ?"); binds.push(body.state === "pass" ? "pass" : "add"); }
      if (sets.length) await db.prepare(`UPDATE v2_set_items SET ${sets.join(", ")} WHERE deal_id = ? AND comp_id = ?`).bind(...binds, id, cid).run();
      return json({ ok: true });
    }
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)\/order$/.exec(path)) && req.method === "POST") {
    const body = await req.json();
    await db.batch((body.comp_ids || []).map((cid, i) => db.prepare("UPDATE v2_set_items SET position = ? WHERE deal_id = ? AND comp_id = ?").bind(i + 1, Number(mm[1]), Number(cid))));
    return json({ ok: true });
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)\/seen$/.exec(path)) && req.method === "POST") {
    await db.prepare("UPDATE v2_alerts SET seen_at = CURRENT_TIMESTAMP WHERE kind = 'deal' AND ref_id = ? AND seen_at IS NULL").bind(Number(mm[1])).run();
    return json({ ok: true });
  }
  if ((mm = /^\/api\/v2\/deals\/(\d+)\/export\.xlsx$/.exec(path)) && req.method === "GET") {
    const deal = await db.prepare("SELECT * FROM v2_deals WHERE id = ?").bind(Number(mm[1])).first();
    if (!deal) return bad("deal not found", 404);
    const items = (await dealItems(db, deal)).filter((i) => i.state === "add");
    const buf = dealWorkbook(dealOut(deal), items, url.searchParams.get("adjusted") === "1");
    const fname = `${String(deal.name).replace(/[^\w .-]+/g, "").trim() || "deal"} - Comp Set - ${new Date().toISOString().slice(0, 10)}.xlsx`;
    return new Response(buf, { headers: { "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "content-disposition": `attachment; filename="${fname}"` } });
  }
  return null;
}

// ---------------- Excel exhibit (Harbor's own comp-set layout) ----------------
const LBL = { tilt_wall: "Tilt Wall", metal: "Metal", masonry: "Masonry", concrete_block: "Block", precast: "Precast", steel: "Steel", rear: "Rear-load", front: "Front-load", cross_dock: "Cross-dock", side: "Side-load", owner_user: "User sale", sale_leaseback: "Sale-leaseback", investment: "Investment", portfolio: "Portfolio" };
function loadingText(c) {
  const p = [];
  if (c.dock_doors) p.push(`${c.dock_doors} Docks`);
  if (c.drive_in_doors) p.push(`${c.drive_in_doors} Grade Level`);
  if (!p.length && c.loading) p.push(LBL[c.loading] || c.loading);
  return p.join(", ");
}
function dateCell(s) {
  if (!s) return "";
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (m) return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  return s;
}
function median(a) {
  const v = a.filter((x) => x != null && Number.isFinite(x)).sort((x, y) => x - y);
  if (!v.length) return null;
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}
export function adjustedValue(c, adj) {
  const base = c.record_kind === "sale" || c.record_kind === "land_sale" ? c.price_psf : c.rent_psf_yr;
  if (base == null || !adj) return base;
  const tot = Object.values(adj).reduce((a, b) => a + (Number(b) || 0), 0);
  return +(base * (1 + tot / 100)).toFixed(2);
}

function dealWorkbook(deal, items, withAdj) {
  const wb = XLSX.utils.book_new();
  const s = subjectValues(deal.subject || {});
  const today = new Date().toISOString().slice(0, 10);
  const sales = items.filter((i) => ["sale", "land_sale"].includes(i.comp.record_kind));
  const leases = items.filter((i) => ["lease", "listing"].includes(i.comp.record_kind));
  const head = (title) => [["Harbor Capital"], [`${deal.name} - ${title}`], [`Prepared ${today} · comps.harborcap.dev`], []];
  const statRows = (vals, label, fmtCols, ncols) => {
    const v = vals.filter((x) => x != null);
    if (!v.length) return [];
    const avg = v.reduce((a, b) => a + b, 0) / v.length;
    return [["", `${label} (n=${v.length})`], ["", "Low", Math.min(...v)], ["", "Median", median(v)], ["", "Average", +avg.toFixed(2)], ["", "High", Math.max(...v)]];
  };
  if (sales.length) {
    const H = ["#", "Property Address", "City", "State", "Zip", "Construction", "Year Built", "SF", "Office SF", "Acreage", "Clear Height", "Loading", "Sale Date", "Sale Price", "Price per SF", ...(withAdj ? ["Adj. %", "Adj. Price per SF"] : []), "Cap Rate", "Buyer", "Seller", "Sale Type", "Distance (mi)", "Notes", "Source"];
    const rows = sales.map((i, n) => {
      const c = i.comp;
      const tot = i.adjust ? Object.values(i.adjust).reduce((a, b) => a + (Number(b) || 0), 0) : null;
      return [n + 1, c.address || c.property_name || "", c.city || "", "TX", c.zip || "", LBL[c.construction] || c.construction || "", c.year_built || "", c.size_sf || c.building_sf || "", c.office_sf || "", c.land_acres ? +(+c.land_acres).toFixed(2) : "", c.clear_height_ft ? `${c.clear_height_ft}'` : "", loadingText(c), dateCell(c.txn_date), c.sale_price || "", c.price_psf || "", ...(withAdj ? [tot != null ? tot / 100 : "", adjustedValue(c, i.adjust) || ""] : []), c.cap_rate_pct ? c.cap_rate_pct / 100 : "", c.buyer || "", c.seller || "", LBL[c.sale_type] || c.sale_type || "", c.distance_mi ?? "", i.note || "", c.source_label || ""];
    });
    const subj = ["Subject", deal.address || deal.name, deal.city || "", "TX", "", LBL[s.construction] || s.construction || "", s.year_built || "", s.building_sf || "", "", s.land_acres || "", s.clear_height_ft ? `${s.clear_height_ft}'` : "", [s.dock_doors && `${s.dock_doors} Docks`, s.drive_in_doors && `${s.drive_in_doors} Grade Level`].filter(Boolean).join(", "), "", s.asking_price || "", s.price_psf || "", ...(withAdj ? ["", ""] : []), s.cap_rate_pct ? s.cap_rate_pct / 100 : "", "", "", "", "", "", ""];
    const aoa = [...head("Sale Comps"), H, subj, ...rows, [], ...statRows(sales.map((i) => (withAdj ? adjustedValue(i.comp, i.adjust) : i.comp.price_psf)), withAdj ? "Adj. price per SF" : "Price per SF"), ...statRows(sales.map((i) => i.comp.cap_rate_pct), "Cap rate %")];
    const ws = XLSX.utils.aoa_to_sheet(aoa, { cellDates: true });
    fmtSheet(ws, aoa, H, { "Sale Date": "m/d/yyyy", "Sale Price": '"$"#,##0', "Price per SF": '"$"#,##0.00', "Adj. Price per SF": '"$"#,##0.00', "Adj. %": "0.0%", "Cap Rate": "0.00%", SF: "#,##0", "Office SF": "#,##0" });
    XLSX.utils.book_append_sheet(wb, ws, "Sale Comps");
  }
  if (leases.length) {
    const H = ["#", "Property Address", "City", "State", "Zip", "Construction", "Year Built", "SF", "Clear Height", "Loading", "Months", "Free Months", "Base Rate ($/SF/mo)", "Base Rate ($/SF/yr)", ...(withAdj ? ["Adj. %", "Adj. Rate ($/SF/yr)"] : []), "Lease Type", "Esc", "TI ($/SF)", "Commencement", "Tenant", "Distance (mi)", "Notes", "Source"];
    const rows = leases.map((i, n) => {
      const c = i.comp;
      const yr = c.rent_psf_yr ?? c.asking_rent_psf_yr ?? null;
      const tot = i.adjust ? Object.values(i.adjust).reduce((a, b) => a + (Number(b) || 0), 0) : null;
      return [n + 1, c.address || c.property_name || "", c.city || "", "TX", c.zip || "", LBL[c.construction] || c.construction || "", c.year_built || "", c.size_sf || "", c.clear_height_ft ? `${c.clear_height_ft}'` : "", loadingText(c), c.term_months || "", c.free_rent_months ?? "", yr ? +(yr / 12).toFixed(3) : "", yr || "", ...(withAdj ? [tot != null ? tot / 100 : "", adjustedValue(c, i.adjust) || ""] : []), c.rent_basis || c.lease_structure || "", c.escalation_pct ? c.escalation_pct / 100 : "", c.ti_psf ?? "", dateCell(c.lease_start || c.txn_date), c.tenant || (c.record_kind === "listing" ? "(availability)" : ""), c.distance_mi ?? "", i.note || "", c.source_label || ""];
    });
    const subj = ["Subject", deal.address || deal.name, deal.city || "", "TX", "", LBL[s.construction] || s.construction || "", s.year_built || "", s.building_sf || "", s.clear_height_ft ? `${s.clear_height_ft}'` : "", [s.dock_doors && `${s.dock_doors} Docks`, s.drive_in_doors && `${s.drive_in_doors} Grade Level`].filter(Boolean).join(", "), "", "", s.in_place_rent_psf_yr ? +(s.in_place_rent_psf_yr / 12).toFixed(3) : "", s.in_place_rent_psf_yr || "", ...(withAdj ? ["", ""] : []), "", "", "", "", s.tenant || "", "", "", ""];
    const aoa = [...head("Lease Comps"), H, subj, ...rows, [], ...statRows(leases.map((i) => (withAdj ? adjustedValue(i.comp, i.adjust) : i.comp.rent_psf_yr)), withAdj ? "Adj. rate $/SF/yr" : "Base rate $/SF/yr")];
    const ws = XLSX.utils.aoa_to_sheet(aoa, { cellDates: true });
    fmtSheet(ws, aoa, H, { Commencement: "m/d/yyyy", "Base Rate ($/SF/mo)": '"$"#,##0.00', "Base Rate ($/SF/yr)": '"$"#,##0.00', "Adj. Rate ($/SF/yr)": '"$"#,##0.00', "Adj. %": "0.0%", Esc: "0.0%", "TI ($/SF)": '"$"#,##0.00', SF: "#,##0" });
    XLSX.utils.book_append_sheet(wb, ws, "Lease Comps");
  }
  if (!sales.length && !leases.length) XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([["No comps in this set yet."]]), "Comps");
  return XLSX.write(wb, { type: "array", bookType: "xlsx" });
}

function fmtSheet(ws, aoa, H, formats) {
  const hr = aoa.findIndex((r) => r === H);
  ws["!cols"] = H.map((h) => ({ wch: Math.min(40, Math.max(8, h.length + 2, ...aoa.slice(hr + 1).map((r) => String(r[H.indexOf(h)] ?? "").length + 1).slice(0, 60))) }));
  for (const [col, z] of Object.entries(formats)) {
    const ci = H.indexOf(col);
    if (ci < 0) continue;
    for (let r = hr + 1; r < aoa.length; r++) {
      const ref = XLSX.utils.encode_cell({ r, c: ci });
      if (ws[ref] && (typeof ws[ref].v === "number" || ws[ref].t === "d")) ws[ref].z = z;
    }
  }
  // stats block number formats follow the column they summarise
  for (let r = hr + 1; r < aoa.length; r++) {
    const ref = XLSX.utils.encode_cell({ r, c: 2 });
    if (ws[ref] && typeof ws[ref].v === "number" && aoa[r][1] && /Low|Median|Average|High/.test(aoa[r][1])) {
      const label = aoa.slice(hr, r).reverse().find((x) => x[1] && /\(n=/.test(x[1]))?.[1] || "";
      ws[ref].z = /Cap/.test(label) ? '0.00"%"' : '"$"#,##0.00';
    }
  }
}

// ---------------- watch: new comps that fit a deal ----------------
export async function checkDeals(env, db, { max = 3 } = {}) {
  const { results: deals } = await db
    .prepare("SELECT * FROM v2_deals WHERE status = 'active' AND watch = 1 AND latitude IS NOT NULL AND (last_checked_at IS NULL OR last_checked_at < datetime('now', '-6 hours')) ORDER BY last_checked_at NULLS FIRST LIMIT ?")
    .bind(max)
    .all();
  const maxId = (await db.prepare("SELECT max(id) AS m FROM v2_comps").first())?.m || 0;
  const out = [];
  for (const d of deals) {
    const seen = d.last_seen_comp_id || 0;
    const fresh = [];
    if (maxId > seen) {
      for (const lane of String(d.lanes || "sale,lease").split(",").filter(Boolean)) {
        const s = await suggestions(env, db, d, lane, { limit: 25 });
        for (const r of s.rows) if (r.id > seen && r.score >= 55 && !r.relaxed) fresh.push(r.id);
      }
    }
    const { results: inSet } = fresh.length ? await db.prepare(`SELECT comp_id FROM v2_set_items WHERE deal_id = ? AND comp_id IN (${fresh.map(() => "?").join(",")})`).bind(d.id, ...fresh).all() : { results: [] };
    const skip = new Set(inSet.map((x) => x.comp_id));
    const ids = [...new Set(fresh.filter((x) => !skip.has(x)))];
    const stmts = [db.prepare("UPDATE v2_deals SET last_checked_at = CURRENT_TIMESTAMP, last_seen_comp_id = ? WHERE id = ?").bind(maxId, d.id)];
    if (ids.length) stmts.push(db.prepare("INSERT INTO v2_alerts (owner, kind, ref_id, title, comp_ids_json) VALUES (?, 'deal', ?, ?, ?)").bind(d.created_by, d.id, `${ids.length} new comp${ids.length > 1 ? "s" : ""} match ${d.name}`, JSON.stringify(ids)));
    await db.batch(stmts);
    out.push({ deal: d.id, new: ids.length });
  }
  return out;
}
