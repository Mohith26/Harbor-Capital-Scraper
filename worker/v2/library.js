// Library: players, saved searches / views, alerts, edit history and the
// data-health fix queues.

import { findComps } from "./finder.js";
import { makeLLM } from "./llm.js";
import { normName } from "./players.js";

const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
const bad = (msg, status = 400) => json({ error: msg }, status);

const ROW = `c.id, c.record_kind, c.property_id, c.source_kind, c.source_label, c.txn_date, c.txn_date_sort, c.date_precision, c.sale_price, c.price_psf, c.cap_rate_pct, c.buyer, c.seller, c.sale_type,
  c.tenant, c.leased_sf, c.rent_psf_yr, c.rent_basis, c.lease_structure, c.term_months, c.escalation_pct, c.asking_rent_psf_yr, c.available_sf, c.severity, c.flags_json,
  p.address, p.property_name, p.city, p.market, p.submarket, p.latitude, p.longitude, p.building_sf, p.year_built, p.clear_height_ft, p.property_subtype, p.product_tags, p.land_acres`;

// Fix queues shown on the Health page. Each is a WHERE clause over comps (c) / properties (p).
export const QUEUES = {
  errors: { label: "Numbers out of bounds", where: "c.status = 'active' AND c.severity = 'error'", hint: "A value fails a sanity check (e.g. price/SF disagrees with price ÷ size). Fix the number or mark the comp as not a comp." },
  duplicates: { label: "Possible duplicates", where: "c.status = 'active' AND c.flags_json LIKE '%possible_duplicate%'", hint: "Looks like another comp at the same building with different numbers." },
  no_date: { label: "No deal date", where: "c.status = 'active' AND c.record_kind IN ('sale','lease','land_sale') AND c.txn_date IS NULL", hint: "Can't be sorted by recency or used in date filters." },
  imprecise_date: { label: "Month / quarter dates", where: "c.status = 'active' AND c.date_precision IN ('month','quarter','year')", hint: "Sorted at the middle of the period. Fine for most uses; add the exact date if you have it." },
  future_date: { label: "Dated in the future", where: "c.status = 'active' AND c.txn_date_sort > date('now')", hint: "Usually a lease that commences later. Check the date is the signing or commencement date." },
  no_type: { label: "Building type unknown", where: "c.status = 'active' AND (p.product_tags IS NULL OR p.product_tags = '') AND p.property_subtype IS NULL", hint: "No product tag (crane, IOS, shallow bay...). These can't pass a product match." },
  no_clear: { label: "Clear height missing", where: "c.status = 'active' AND p.clear_height_ft IS NULL", hint: "Find it from the OM, CoStar or the satellite read on the building page." },
  area_only: { label: "Located to submarket only", where: "c.status = 'active' AND p.geocode_quality IN ('submarket','city','postal')", hint: "No street address, so the pin is the submarket centre." },
};

export async function handleLibrary(req, env, ctx, path, user) {
  const db = env.COMPS_DB;
  const url = new URL(req.url);
  let mm;

  // ---------- players ----------
  if (path === "/api/v2/players" && req.method === "GET") {
    const q = url.searchParams.get("q");
    const role = url.searchParams.get("role");
    const where = ["p.n_deals > 0"];
    const b = [];
    if (q) { where.push("(p.name LIKE ? OR p.norm LIKE ?)"); b.push(`%${q}%`, `%${normName(q) || q}%`); }
    if (role) { where.push("p.kinds LIKE ?"); b.push(`%|${role}|%`); }
    const { results } = await db.prepare(`SELECT p.* FROM v2_players p WHERE ${where.join(" AND ")} ORDER BY p.n_deals DESC, p.last_date DESC LIMIT ?`).bind(...b, Math.min(Number(url.searchParams.get("limit") || 60), 300)).all();
    return json({ players: results });
  }
  if ((mm = /^\/api\/v2\/players\/(\d+)$/.exec(path)) && req.method === "GET") {
    const p = await db.prepare("SELECT * FROM v2_players WHERE id = ?").bind(Number(mm[1])).first();
    if (!p) return bad("not found", 404);
    const { results } = await db.prepare(`SELECT cp.role, ${ROW} FROM v2_comp_players cp JOIN v2_comps c ON c.id = cp.comp_id JOIN v2_properties p ON p.id = c.property_id WHERE cp.player_id = ? AND c.status = 'active' ORDER BY c.txn_date_sort DESC NULLS LAST LIMIT 500`).bind(p.id).all();
    const by = (k) => { const m = {}; for (const r of results) { const v = typeof k === "function" ? k(r) : r[k]; if (v) m[v] = (m[v] || 0) + 1; } return Object.entries(m).sort((a, b) => b[1] - a[1]); };
    const tags = {};
    for (const r of results) for (const t of String(r.product_tags || "").split("|").filter(Boolean)) tags[t] = (tags[t] || 0) + 1;
    const { results: aliases } = await db.prepare("SELECT alias_norm FROM v2_player_aliases WHERE player_id = ?").bind(p.id).all();
    return json({ player: p, comps: results.map((r) => ({ ...r, flags_json: undefined })), roles: by("role"), markets: by("market"), submarkets: by("submarket").slice(0, 8), tags: Object.entries(tags).sort((a, b) => b[1] - a[1]), aliases: aliases.map((a) => a.alias_norm) });
  }
  // merge player A into B (aliases keep future imports pointing at B)
  if ((mm = /^\/api\/v2\/players\/(\d+)\/merge$/.exec(path)) && req.method === "POST") {
    const from = Number(mm[1]);
    const { into } = await req.json();
    const a = await db.prepare("SELECT * FROM v2_players WHERE id = ?").bind(from).first();
    const b = await db.prepare("SELECT * FROM v2_players WHERE id = ?").bind(Number(into)).first();
    if (!a || !b || a.id === b.id) return bad("bad merge");
    await db.batch([
      db.prepare("INSERT OR IGNORE INTO v2_comp_players (comp_id, player_id, role) SELECT comp_id, ?, role FROM v2_comp_players WHERE player_id = ?").bind(b.id, a.id),
      db.prepare("DELETE FROM v2_comp_players WHERE player_id = ?").bind(a.id),
      db.prepare("INSERT OR REPLACE INTO v2_player_aliases (alias_norm, player_id) VALUES (?, ?)").bind(a.norm, b.id),
      db.prepare("UPDATE v2_player_aliases SET player_id = ? WHERE player_id = ?").bind(b.id, a.id),
      db.prepare("UPDATE v2_players SET kinds = COALESCE(kinds,'') || COALESCE(?, ''), n_deals = (SELECT count(DISTINCT comp_id) FROM v2_comp_players WHERE player_id = ?) WHERE id = ?").bind(a.kinds, b.id, b.id),
      db.prepare("DELETE FROM v2_players WHERE id = ?").bind(a.id),
      db.prepare("INSERT INTO v2_edits (entity, entity_id, field, old_value, new_value, by_user) VALUES ('player', ?, 'merged_into', ?, ?, ?)").bind(b.id, a.name, b.name, user),
    ]);
    return json({ ok: true });
  }

  // ---------- saved searches / views ----------
  if (path === "/api/v2/saved" && req.method === "GET") {
    const kind = url.searchParams.get("kind");
    const { results } = await db.prepare(`SELECT s.*, (SELECT count(*) FROM v2_alerts a WHERE a.kind = 'search' AND a.ref_id = s.id AND a.seen_at IS NULL) AS n_alerts FROM v2_saved s WHERE (? IS NULL OR s.kind = ?) AND (s.shared = 1 OR s.owner = ?) ORDER BY s.created_at DESC`).bind(kind, kind, user).all();
    return json({ saved: results.map((s) => ({ ...s, filters: s.filters_json ? JSON.parse(s.filters_json) : null, location: s.location_json ? JSON.parse(s.location_json) : null, polygon: s.polygon_json ? JSON.parse(s.polygon_json) : null })) });
  }
  if (path === "/api/v2/saved" && req.method === "POST") {
    const b = await req.json();
    if (!b.name || !["search", "view"].includes(b.kind)) return bad("name and kind required");
    const max = await db.prepare("SELECT max(id) AS m FROM v2_comps").first();
    const r = await db.prepare("INSERT INTO v2_saved (kind, name, query, filters_json, location_json, polygon_json, alert, shared, owner, last_seen_comp_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id")
      .bind(b.kind, String(b.name).slice(0, 80), b.query || null, b.filters ? JSON.stringify(b.filters) : null, b.location ? JSON.stringify(b.location) : null, b.polygon ? JSON.stringify(b.polygon) : null, b.alert ? 1 : 0, b.shared === false ? 0 : 1, user, max?.m || 0).first();
    return json({ id: r.id });
  }
  if ((mm = /^\/api\/v2\/saved\/(\d+)$/.exec(path))) {
    if (req.method === "DELETE") { await db.prepare("DELETE FROM v2_saved WHERE id = ?").bind(Number(mm[1])).run(); return json({ ok: true }); }
    if (req.method === "PATCH") {
      const b = await req.json();
      if (b.alert !== undefined) await db.prepare("UPDATE v2_saved SET alert = ? WHERE id = ?").bind(b.alert ? 1 : 0, Number(mm[1])).run();
      if (b.name) await db.prepare("UPDATE v2_saved SET name = ? WHERE id = ?").bind(String(b.name).slice(0, 80), Number(mm[1])).run();
      return json({ ok: true });
    }
  }

  // ---------- alerts ----------
  if (path === "/api/v2/alerts" && req.method === "GET") {
    const { results } = await db.prepare(`SELECT a.*, CASE a.kind WHEN 'deal' THEN (SELECT name FROM v2_deals WHERE id = a.ref_id) ELSE (SELECT name FROM v2_saved WHERE id = a.ref_id) END AS ref_name FROM v2_alerts a WHERE a.seen_at IS NULL ORDER BY a.id DESC LIMIT 50`).all();
    return json({ alerts: results.map((a) => ({ ...a, comp_ids: JSON.parse(a.comp_ids_json || "[]") })) });
  }
  if (path === "/api/v2/alerts/seen" && req.method === "POST") {
    const b = await req.json();
    const ids = (b.ids || []).map(Number);
    if (ids.length) await db.prepare(`UPDATE v2_alerts SET seen_at = CURRENT_TIMESTAMP WHERE id IN (${ids.map(() => "?").join(",")})`).bind(...ids).run();
    return json({ ok: true });
  }

  // ---------- edit history ----------
  if (path === "/api/v2/edits" && req.method === "GET") {
    const e = url.searchParams.get("entity");
    const id = Number(url.searchParams.get("id"));
    const { results } = await db.prepare("SELECT * FROM v2_edits WHERE entity = ? AND entity_id = ? ORDER BY id DESC LIMIT 100").bind(e, id).all();
    return json({ edits: results });
  }

  // ---------- health queues ----------
  if (path === "/api/v2/health/queues" && req.method === "GET") {
    const counts = {};
    await Promise.all(Object.entries(QUEUES).map(async ([k, q]) => { counts[k] = (await db.prepare(`SELECT count(*) AS n FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE ${q.where}`).first()).n; }));
    const cov = await db.prepare(`SELECT count(*) AS n, sum(clear_height_ft IS NOT NULL) AS clear, sum(building_sf IS NOT NULL) AS sf, sum(year_built IS NOT NULL) AS yb, sum(land_acres IS NOT NULL) AS land,
      sum(product_tags IS NOT NULL AND product_tags != '') AS tags, sum(loading IS NOT NULL OR dock_doors IS NOT NULL) AS loading, sum(construction IS NOT NULL) AS construction, sum(geocode_quality NOT IN ('submarket','city','postal','failed')) AS located FROM v2_properties p WHERE EXISTS (SELECT 1 FROM v2_comps c WHERE c.property_id = p.id AND c.status = 'active')`).first();
    const dates = await db.prepare("SELECT date_precision AS k, count(*) AS n FROM v2_comps WHERE status = 'active' GROUP BY 1").all();
    const held = await db.prepare("SELECT count(*) AS n FROM v2_staged WHERE decision = 'review'").first();
    const src = await db.prepare("SELECT source_kind AS k, record_kind AS r, count(*) AS n FROM v2_comps WHERE status = 'active' GROUP BY 1, 2").all();
    return json({ queues: Object.fromEntries(Object.entries(QUEUES).map(([k, q]) => [k, { label: q.label, hint: q.hint, n: counts[k] }])), coverage: cov, dates: dates.results, held: held.n, sources: src.results });
  }
  if ((mm = /^\/api\/v2\/health\/queues\/(\w+)$/.exec(path)) && req.method === "GET") {
    const q = QUEUES[mm[1]];
    if (!q) return bad("no such queue", 404);
    const off = Number(url.searchParams.get("offset") || 0);
    const { results } = await db.prepare(`SELECT ${ROW} FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE ${q.where} ORDER BY c.txn_date_sort DESC NULLS LAST, c.id DESC LIMIT 100 OFFSET ?`).bind(off).all();
    return json({ rows: results.map((r) => ({ ...r, flags: JSON.parse(r.flags_json || "[]").filter((f) => f.severity !== "info"), flags_json: undefined })) });
  }
  return null;
}

// Daily check of saved searches with alerts on.
export async function checkSaved(env, db, { max = 3 } = {}) {
  const { results } = await db.prepare("SELECT * FROM v2_saved WHERE kind = 'search' AND alert = 1 AND (last_run_at IS NULL OR last_run_at < datetime('now', '-20 hours')) ORDER BY last_run_at NULLS FIRST LIMIT ?").bind(max).all();
  const maxId = (await db.prepare("SELECT max(id) AS m FROM v2_comps").first())?.m || 0;
  const out = [];
  for (const s of results) {
    let ids = [];
    if (maxId > (s.last_seen_comp_id || 0)) {
      const f = s.filters_json ? JSON.parse(s.filters_json) : {};
      const llm = (() => { try { return makeLLM(env, { provider: "deepseek", noFallback: true }); } catch { return null; } })();
      const res = await findComps(env, db, llm, { query: f.parsed ? "" : s.query || "", parsed: f.parsed || null, location: s.location_json ? JSON.parse(s.location_json) : null, filters: f.filters || {}, limit: 60, skipSubject: true, manual: f.manual || null, polygon: s.polygon_json ? JSON.parse(s.polygon_json) : null });
      let rows = res.results.filter((r) => !r.relaxed && r.id > (s.last_seen_comp_id || 0));
      const poly = s.polygon_json ? JSON.parse(s.polygon_json) : null;
      if (poly && poly.length > 2) rows = rows.filter((r) => r.latitude != null && inPoly([r.latitude, r.longitude], poly));
      ids = rows.map((r) => r.id);
    }
    const st = [db.prepare("UPDATE v2_saved SET last_run_at = CURRENT_TIMESTAMP, last_seen_comp_id = ? WHERE id = ?").bind(maxId, s.id)];
    if (ids.length) st.push(db.prepare("INSERT INTO v2_alerts (owner, kind, ref_id, title, comp_ids_json) VALUES (?, 'search', ?, ?, ?)").bind(s.owner, s.id, `${ids.length} new comp${ids.length > 1 ? "s" : ""} for "${s.name}"`, JSON.stringify(ids)));
    await db.batch(st);
    out.push({ saved: s.id, new: ids.length });
  }
  return out;
}

export function inPoly([y, x], poly) {
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [yi, xi] = poly[i], [yj, xj] = poly[j];
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}
