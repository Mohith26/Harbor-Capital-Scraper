import { productTags } from "./tags.js";
import { linkPlayers } from "./players.js";
// D1 persistence for properties, comps and feature observations.

import { parseAddress, marketFor } from "./geo.js";
import { samePropertyScore, sameDealScore, mergeMissing } from "./dedupe.js";
import { FEATURE_FIELDS } from "./schema.js";
import { dateToNumber } from "./validate.js";
import { bbox } from "./geo.js";
import { resolveFeatures, confFor } from "./enrich.js";

export const COMP_FIELDS = [
  "sale_price", "price_psf", "price_per_acre", "cap_rate_pct", "sale_date", "buyer", "seller", "sale_type", "occupancy_pct", "noi",
  "tenant", "leased_sf", "rent_psf_yr", "rent_psf_mo", "rent_per_acre_mo", "monthly_rent_total", "lease_structure", "term_months",
  "lease_start", "lease_end", "escalation_pct", "free_rent_months", "ti_psf", "lease_kind", "landlord", "asking_rent_psf_yr", "asking_price", "available_sf",
  "net_effective_rent_psf_yr", "tenant_industry", "rent_basis",
];

export function rentBasis(s) {
  if (!s) return null;
  const u = String(s).toUpperCase();
  if (["NNN", "NET", "TRIPLE NET"].includes(u)) return "NNN";
  if (["NN", "DOUBLE NET", "MODIFIED NET"].includes(u)) return "NN";
  if (["MG", "IG", "MODIFIED GROSS", "INDUSTRIAL GROSS"].includes(u)) return "MG";
  if (["GROSS", "FSG", "FULL SERVICE", "FULL SERVICE GROSS"].includes(u)) return "gross";
  return s;
}
export const PROP_FEATURES = FEATURE_FIELDS.filter((f) => f !== "office_sf" || true);
const PROP_COLS = ["building_sf", "land_acres", "year_built", "year_renovated", "clear_height_ft", "dock_doors", "drive_in_doors", "loading", "construction", "office_sf", "office_pct", "sprinkler", "rail_served", "yard_acres", "building_class", "property_subtype", "tenancy", "car_parking", "trailer_parking", "power", "cranes", "zoning"];

export function txnDate(r) {
  if (r.record_kind === "sale" || r.record_kind === "land_sale") return r.sale_date;
  return r.lease_start || r.sale_date || null;
}

// Find the best existing property for a record (already geocoded).
export async function findPropertyMatch(db, rec) {
  const cands = new Map();
  const pa = rec.address ? parseAddress(rec.address) : null;
  if (pa && pa.key && pa.numbers.length) {
    const { results } = await db.prepare("SELECT * FROM v2_properties WHERE addr_key = ? LIMIT 20").bind(pa.key).all();
    results.forEach((r) => cands.set(r.id, r));
  }
  if (rec.latitude != null) {
    const b = bbox(rec.latitude, rec.longitude, 0.35);
    const { results } = await db.prepare("SELECT * FROM v2_properties WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ? LIMIT 80").bind(b.minLat, b.maxLat, b.minLng, b.maxLng).all();
    results.forEach((r) => cands.set(r.id, r));
  }
  if (rec.property_name && !pa?.numbers.length) {
    const { results } = await db.prepare("SELECT * FROM v2_properties WHERE lower(property_name) = lower(?) LIMIT 10").bind(rec.property_name).all();
    results.forEach((r) => cands.set(r.id, r));
  }
  let best = null;
  for (const p of cands.values()) {
    const s = samePropertyScore(rec, p);
    if (!best || s.score > best.score) best = { ...s, property: p };
  }
  return best && best.score >= 0.75 ? best : best && best.score >= 0.55 ? { ...best, weak: true } : null;
}

export async function findDealMatch(db, propertyId, rec) {
  const { results } = await db.prepare("SELECT * FROM v2_comps WHERE property_id = ? AND status != 'rejected' LIMIT 200").bind(propertyId).all();
  let best = null;
  for (const c of results) {
    const d = sameDealScore(rec, c);
    if (d.verdict === "duplicate" || d.verdict === "conflict") {
      if (!best || d.score > best.score) best = { ...d, comp: c };
    }
  }
  return best;
}

function featureObsFromRecord(rec) {
  const obs = [];
  for (const f of PROP_COLS) {
    let v = rec[f];
    if (f === "cranes") continue;
    if (v == null || v === "") continue;
    // A lease row's "building_sf" is the building; leased SF is not a building feature.
    obs.push({ field: f, value: v, source: "broker_sheet" });
  }
  return obs;
}

export async function insertObservations(db, propertyId, obs, sourceRef) {
  if (!obs.length) return;
  const stmts = obs.map((o) =>
    db.prepare("INSERT INTO v2_feature_obs (property_id, field, value, source, source_ref, confidence) VALUES (?, ?, ?, ?, ?, ?)").bind(propertyId, o.field, JSON.stringify(o.value), o.source, o.source_ref || sourceRef || null, o.confidence ?? confFor(o.source, o.field)),
  );
  for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
}

// Recompute a property's resolved features from all observations.
export async function refreshProperty(db, propertyId) {
  const { results } = await db.prepare("SELECT field, value, source, source_ref, confidence, observed_at FROM v2_feature_obs WHERE property_id = ?").bind(propertyId).all();
  const obs = results.map((o) => ({ ...o, value: safeParse(o.value) }));
  const { best, conflicts } = resolveFeatures(obs);
  const sets = [];
  const binds = [];
  for (const f of PROP_COLS) {
    if (best[f] === undefined) continue;
    let v = best[f].value;
    if (f === "rail_served") v = v === true || v === "true" || v === 1 ? 1 : v === false || v === "false" ? 0 : null;
    sets.push(`${f} = ?`);
    binds.push(v);
  }
  sets.push("features_json = ?", "updated_at = CURRENT_TIMESTAMP");
  binds.push(JSON.stringify({ best, conflicts }));
  // product tags from resolved features + comp notes + broker file titles
  try {
    const cur = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(propertyId).first();
    const txt = await db.prepare("SELECT group_concat(COALESCE(notes,'') || ' ' || COALESCE(source_label,'') || ' ' || COALESCE(source_sheet,''), ' ') AS t FROM v2_comps WHERE property_id = ? AND status = 'active'").bind(propertyId).first();
    const merged = { ...(cur || {}) };
    for (const f of PROP_COLS) if (best[f] !== undefined) merged[f] = best[f].value;
    const noteObs = obs.filter((o) => /note|power|crane/.test(o.field)).map((o) => String(o.value)).join(" ");
    sets.push("product_tags = ?");
    binds.push(productTags(merged, `${txt?.t || ""} ${noteObs}`));
  } catch {}
  await db.prepare(`UPDATE v2_properties SET ${sets.join(", ")} WHERE id = ?`).bind(...binds, propertyId).run();
  // Downstream mirrors (Atlas) sync on v2_comps.updated_at, so building changes bump the comps too.
  await db.prepare("UPDATE v2_comps SET updated_at = CURRENT_TIMESTAMP WHERE property_id = ?").bind(propertyId).run();
}

function safeParse(v) {
  try {
    return JSON.parse(v);
  } catch {
    return v;
  }
}

export async function createProperty(db, rec) {
  const pa = rec.address ? parseAddress(rec.address) : null;
  const market = rec.latitude != null ? marketFor(rec.latitude, rec.longitude) : null;
  const r = await db
    .prepare(
      `INSERT INTO v2_properties (addr_key, address, property_name, city, state, zip, county, market, submarket, latitude, longitude, geocode_quality, geocode_source, formatted_address)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
    )
    .bind(pa?.key || null, rec.address || null, rec.property_name || null, rec.city || rec._geo?.city || null, rec.state || "TX", rec.zip || rec._geo?.zip || null, rec.county || rec._geo?.county || null, market, rec.submarket || null, rec.latitude ?? null, rec.longitude ?? null, rec.geocode_quality || null, rec._geo?.source || null, rec._geo?.formatted || null)
    .first();
  return r.id;
}

// Fill property identity gaps (name, zip, submarket) from a new record.
async function backfillProperty(db, prop, rec) {
  const upd = {};
  for (const f of ["property_name", "city", "zip", "submarket", "county"]) if (!prop[f] && rec[f]) upd[f] = rec[f];
  if (prop.latitude == null && rec.latitude != null) Object.assign(upd, { latitude: rec.latitude, longitude: rec.longitude, geocode_quality: rec.geocode_quality, market: marketFor(rec.latitude, rec.longitude) });
  const keys = Object.keys(upd);
  if (!keys.length) return;
  await db.prepare(`UPDATE v2_properties SET ${keys.map((k) => `${k} = ?`).join(", ")} WHERE id = ?`).bind(...keys.map((k) => upd[k]), prop.id).run();
}

export async function insertComp(db, propertyId, rec, { fileId = null, sourceKind = "broker", sourceLabel = null, user = null, status = "active", dupOf = null } = {}) {
  const td = txnDate(rec);
  if (rec.rent_basis == null && rec.lease_structure) rec.rent_basis = rentBasis(rec.lease_structure);
  const cols = ["property_id", "record_kind", "status", "dup_of", "file_id", "source_kind", "source_label", "source_sheet", "source_rows", "sources_json", ...COMP_FIELDS, "txn_date", "txn_year", "snapshot_json", "flags_json", "severity", "notes", "created_by"];
  const snap = {};
  for (const [k, v] of Object.entries(rec)) if (!k.startsWith("_") && v != null && k !== "evidence") snap[k] = v;
  const vals = [
    propertyId, rec.record_kind, status, dupOf, fileId, sourceKind, sourceLabel, rec.source_sheet || null, JSON.stringify(rec.source_rows || []),
    JSON.stringify([{ kind: sourceKind, label: sourceLabel, sheet: rec.source_sheet || null, rows: rec.source_rows || [], file_id: fileId }]),
    ...COMP_FIELDS.map((f) => (rec[f] === undefined ? null : rec[f])),
    td, dateToNumber(td), JSON.stringify(snap), JSON.stringify(rec._flags || []), rec._severity || "ok", rec.notes || null, user,
  ];
  const r = await db.prepare(`INSERT INTO v2_comps (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")}) RETURNING id`).bind(...vals).first();
  return r.id;
}

// Commit one resolved record. decision: add | merge | skip
export async function commitRecord(db, rec, match, decision, ctx) {
  if (decision === "skip" || rec.record_kind === "subject") return { action: "skipped" };
  let propertyId = match?.property?.id && !match.property.weak ? match.property.id : match?.property_id_override || null;
  // Re-match at commit time: earlier records of the same file may have just
  // created this building, and resolve-time matches can be stale.
  if (!propertyId) {
    const fresh = await findPropertyMatch(db, rec);
    if (fresh && !fresh.weak) {
      propertyId = fresh.property.id;
      if (decision === "add") {
        const deal = await findDealMatch(db, propertyId, rec);
        if (deal?.verdict === "duplicate") {
          match = { ...(match || {}), deal };
          decision = "merge";
        }
      }
    }
  }
  if (!propertyId) propertyId = await createProperty(db, rec);
  else {
    const prop = await db.prepare("SELECT * FROM v2_properties WHERE id = ?").bind(propertyId).first();
    if (prop) await backfillProperty(db, prop, rec);
  }
  await insertObservations(db, propertyId, featureObsFromRecord(rec), `${ctx.sourceLabel}${rec.source_sheet ? ` / ${rec.source_sheet}` : ""}${rec.source_rows?.length ? ` row ${rec.source_rows.join(",")}` : ""}`);
  await refreshProperty(db, propertyId);
  if (decision === "merge" && match?.deal?.comp) {
    const canon = match.deal.comp;
    const fill = mergeMissing(canon, rec, COMP_FIELDS);
    const sources = JSON.parse(canon.sources_json || "[]");
    sources.push({ kind: ctx.sourceKind, label: ctx.sourceLabel, sheet: rec.source_sheet || null, rows: rec.source_rows || [], file_id: ctx.fileId });
    const keys = Object.keys(fill);
    await db.prepare(`UPDATE v2_comps SET ${[...keys.map((k) => `${k} = ?`), "sources_json = ?", "updated_at = CURRENT_TIMESTAMP"].join(", ")} WHERE id = ?`).bind(...keys.map((k) => fill[k]), JSON.stringify(sources), canon.id).run();
    await linkPlayers(db, canon.id, rec, txnDate(rec)).catch(() => null);
    return { action: "merged", comp_id: canon.id, property_id: propertyId, filled: keys };
  }
  const id = await insertComp(db, propertyId, rec, ctx);
  await linkPlayers(db, id, rec, txnDate(rec)).catch(() => null);
  return { action: "added", comp_id: id, property_id: propertyId };
}
