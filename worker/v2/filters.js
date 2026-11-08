// Manual filter panel: one field list shared by the server (SQL for the Library,
// row matching for saved-search alerts) and the browser (served as /comp-filters.js).
// Value shape is the one produced by hcFilters (see _harbor-ui/hc-ui.js).
//   range: {min,max,unk}   multi: [..]   tri: 'yes'|'no'   text: '..'   daterange: {from,to}

const SIZE = "COALESCE(c.leased_sf, c.available_sf, p.building_sf)";
// id, label, type, sql expression, row key (finder/library rows), section, adv, extra
export const FIELD_DEFS = [
  // ---- Basic: deal ----
  ["kind", "Record type", "multi", "c.record_kind", "record_kind", "Deal", 0, { opts: [["sale", "Sale"], ["lease", "Lease"], ["listing", "Availability / listing"], ["land_sale", "Land / IOS sale"]] }],
  ["date", "Deal date", "daterange", "c.txn_date_sort", "txn_date_sort", "Deal", 0],
  ["sf", "Deal size", "range", SIZE, "_size", "Deal", 0, { unit: "SF", step: 1000, unknown: 1, hint: "Leased SF for leases, building SF for sales" }],
  ["price", "Sale price", "range", "c.sale_price", "sale_price", "Deal", 0, { unit: "$", step: 100000, unknown: 1 }],
  ["psf", "Price per SF", "range", "c.price_psf", "price_psf", "Deal", 0, { unit: "$/SF", unknown: 1 }],
  ["cap", "Cap rate", "range", "c.cap_rate_pct", "cap_rate_pct", "Deal", 0, { unit: "%", step: 0.05, unknown: 1 }],
  ["rent", "Rent", "range", "c.rent_psf_yr", "rent_psf_yr", "Deal", 0, { unit: "$/SF/yr", step: 0.05, unknown: 1 }],
  ["basis", "Rent basis", "multi", "c.rent_basis", "rent_basis", "Deal", 0],
  ["term", "Lease term", "range", "c.term_months", "term_months", "Deal", 0, { unit: "months", unknown: 1 }],
  // ---- Basic: location ----
  ["market", "Market", "multi", "p.market", "market", "Location", 0],
  ["submarket", "Submarket", "multi", "p.submarket", "submarket", "Location", 0],
  ["county", "County", "multi", "p.county", "county", "Location", 0],
  ["city", "City", "multi", "p.city", "city", "Location", 0],
  // ---- Basic: building ----
  ["subtype", "Product type", "multi", "p.property_subtype", "property_subtype", "Building", 0],
  ["tags", "Product tags", "multi", "p.product_tags", "product_tags", "Building", 0, { tags: 1 }],
  ["cls", "Building class", "multi", "p.building_class", "building_class", "Building", 0],
  ["bsf", "Building size", "range", "p.building_sf", "building_sf", "Building", 0, { unit: "SF", step: 1000, unknown: 1 }],
  ["acres", "Land", "range", "p.land_acres", "land_acres", "Building", 0, { unit: "acres", step: 0.5, unknown: 1 }],
  ["clear", "Clear height", "range", "p.clear_height_ft", "clear_height_ft", "Building", 0, { unit: "ft", unknown: 1 }],
  ["yb", "Year built", "range", "p.year_built", "year_built", "Building", 0, { unit: "", unknown: 1 }],
  // ---- Basic: parties ----
  ["tenant", "Tenant contains", "text", "c.tenant", "tenant", "Parties", 0],
  ["buyer", "Buyer contains", "text", "c.buyer", "buyer", "Parties", 0],
  ["seller", "Seller contains", "text", "c.seller", "seller", "Parties", 0],
  // ---- Advanced: lease economics ----
  ["structure", "Lease structure", "multi", "c.lease_structure", "lease_structure", "Lease economics", 1],
  ["lkind", "Lease kind", "multi", "c.lease_kind", "lease_kind", "Lease economics", 1],
  ["bumps", "Annual bumps", "range", "c.escalation_pct", "escalation_pct", "Lease economics", 1, { unit: "%", step: 0.25, unknown: 1, cov: "9%" }],
  ["free", "Free rent", "range", "c.free_rent_months", "free_rent_months", "Lease economics", 1, { unit: "months", unknown: 1, cov: "5%" }],
  ["ti", "TI allowance", "range", "c.ti_psf", "ti_psf", "Lease economics", 1, { unit: "$/SF", unknown: 1, cov: "6%" }],
  ["ner", "Net effective rent", "range", "c.net_effective_rent_psf_yr", "net_effective_rent_psf_yr", "Lease economics", 1, { unit: "$/SF/yr", unknown: 1, cov: "1%" }],
  ["leased", "Leased SF", "range", "c.leased_sf", "leased_sf", "Lease economics", 1, { unit: "SF", step: 1000, unknown: 1 }],
  ["industry", "Tenant industry", "multi", "c.tenant_industry", "tenant_industry", "Lease economics", 1],
  ["landlord", "Landlord contains", "text", "c.landlord", "landlord", "Lease economics", 1],
  // ---- Advanced: sale details ----
  ["stype", "Sale type", "multi", "c.sale_type", "sale_type", "Sale details", 1],
  ["occ", "Occupancy at sale", "range", "c.occupancy_pct", "occupancy_pct", "Sale details", 1, { unit: "%", unknown: 1 }],
  ["has_price", "Has a sale price", "tri", "c.sale_price", "sale_price", "Sale details", 1, { present: 1 }],
  ["has_cap", "Has a cap rate", "tri", "c.cap_rate_pct", "cap_rate_pct", "Sale details", 1, { present: 1 }],
  ["has_rent", "Has a rent", "tri", "c.rent_psf_yr", "rent_psf_yr", "Sale details", 1, { present: 1 }],
  // ---- Advanced: building features ----
  ["docks", "Dock-high doors", "range", "p.dock_doors", "dock_doors", "Building features", 1, { unknown: 1 }],
  ["dids", "Grade-level doors", "range", "p.drive_in_doors", "drive_in_doors", "Building features", 1, { unknown: 1 }],
  ["office", "Office share", "range", "p.office_pct", "office_pct", "Building features", 1, { unit: "%", unknown: 1 }],
  ["construction", "Construction", "multi", "p.construction", "construction", "Building features", 1],
  ["sprinkler", "Sprinkler", "multi", "p.sprinkler", "sprinkler", "Building features", 1],
  ["loading", "Loading", "multi", "p.loading", "loading", "Building features", 1, { cov: "6%" }],
  ["tenancy", "Tenancy", "multi", "p.tenancy", "tenancy", "Building features", 1],
  ["rail", "Rail served", "tri", "p.rail_served", "rail_served", "Building features", 1, { bool: 1, cov: "6%" }],
  ["crane", "Crane-served", "tri", "(p.cranes IS NOT NULL AND p.cranes <> '' OR p.product_tags LIKE '%|crane|%')", "_crane", "Building features", 1, { expr: 1 }],
  ["heavy", "Heavy power", "tri", "(p.power LIKE '%heavy%' OR p.product_tags LIKE '%|heavy_power|%')", "_heavy", "Building features", 1, { expr: 1 }],
  ["power", "Power contains", "text", "p.power", "power", "Building features", 1, { ph: "e.g. 480v, 2,000a" }],
  ["yard", "Yard", "range", "p.yard_acres", "yard_acres", "Building features", 1, { unit: "acres", unknown: 1, cov: "1%" }],
  ["zoning", "Zoning contains", "text", "p.zoning", "zoning", "Building features", 1],
  ["owner", "Owner contains", "text", "p.owner", "owner", "Building features", 1],
  ["zip", "ZIP code", "multi", "p.zip", "zip", "Building features", 1],
  // ---- Advanced: source & quality ----
  ["source", "Source", "multi", "c.source_kind", "source_kind", "Source & quality", 1, { opts: [["broker", "Broker file"], ["costar", "CoStar"], ["legacy", "Legacy"]] }],
  ["file", "Source file contains", "text", "c.source_label", "source_label", "Source & quality", 1],
  ["severity", "Review status", "multi", "c.severity", "severity", "Source & quality", 1, { opts: [["ok", "Clean"], ["warn", "Has notes"], ["error", "Needs review"]] }],
  ["precision", "Date precision", "multi", "c.date_precision", "date_precision", "Source & quality", 1],
  ["geo", "Geocoded on map", "tri", "p.latitude", "latitude", "Source & quality", 1, { present: 1 }],
];

export const FACET_IDS = FIELD_DEFS.filter((d) => d[2] === "multi" && !d[7]?.opts && !d[7]?.tags).map((d) => d[0]);
const byId = Object.fromEntries(FIELD_DEFS.map((d) => [d[0], d]));
const empty = (t, v) => v == null || v === "" || (t === "multi" && (!Array.isArray(v) || !v.length)) || (t === "range" && (v.min == null || v.min === "") && (v.max == null || v.max === "")) || (t === "daterange" && !v.from && !v.to);
const num = (x) => (x === "" || x == null || Number.isNaN(Number(x)) ? null : Number(x));

/** SQL WHERE fragments for the Library (/api/v2/comps?f=...). Unknown ids are ignored. */
export function filterSql(value) {
  const where = [], binds = [];
  for (const [id, v] of Object.entries(value || {})) {
    const d = byId[id]; if (!d) continue;
    const [, , t, col, , , , x = {}] = d;
    if (empty(t, v)) continue;
    if (t === "range") {
      const parts = [], b = [];
      if (num(v.min) != null) { parts.push(`${col} >= ?`); b.push(num(v.min)); }
      if (num(v.max) != null) { parts.push(`${col} <= ?`); b.push(num(v.max)); }
      if (!parts.length) continue;
      where.push(v.unk ? `(${col} IS NULL OR (${parts.join(" AND ")}))` : `(${parts.join(" AND ")})`); binds.push(...b);
    } else if (t === "multi") {
      const vals = v.slice(0, 200).map(String);
      if (x.tags) { where.push(`(${vals.map(() => `${col} LIKE ?`).join(" OR ")})`); binds.push(...vals.map((s) => `%|${s}|%`)); }
      else { where.push(`${col} IN (${vals.map(() => "?").join(",")})`); binds.push(...vals); }
    } else if (t === "tri") {
      const yes = v === "yes";
      if (x.expr) where.push(yes ? col : `NOT ${col}`);
      else if (x.bool) where.push(yes ? `${col} = 1` : `(${col} IS NULL OR ${col} = 0)`);
      else where.push(yes ? `(${col} IS NOT NULL AND ${col} <> '')` : `(${col} IS NULL OR ${col} = '')`);
    } else if (t === "text") {
      where.push(`${col} LIKE ?`); binds.push(`%${String(v).slice(0, 120)}%`);
    } else if (t === "daterange") {
      if (v.from) { where.push(`${col} >= ?`); binds.push(String(v.from).slice(0, 10)); }
      if (v.to) { where.push(`${col} <= ?`); binds.push(String(v.to).slice(0, 10)); }
    }
  }
  return { where, binds };
}

/** Same semantics against a result row (finder / saved-search alerts). */
export function rowValue(r, key) {
  if (key === "_size") return r.leased_sf ?? r.available_sf ?? r.building_sf ?? r.size_sf ?? null;
  if (key === "_crane") return !!((r.cranes && String(r.cranes).trim()) || String(r.product_tags || "").includes("|crane|"));
  if (key === "_heavy") return /heavy/i.test(String(r.power || "")) || String(r.product_tags || "").includes("|heavy_power|");
  return r[key];
}
export function matchRow(r, value) {
  for (const [id, v] of Object.entries(value || {})) {
    const d = byId[id]; if (!d) continue;
    const [, , t, , key, , , x = {}] = d;
    if (empty(t, v)) continue;
    const val = rowValue(r, key);
    if (t === "range") {
      const n = num(val);
      if (n == null) { if (!v.unk) return false; continue; }
      if (num(v.min) != null && n < num(v.min)) return false;
      if (num(v.max) != null && n > num(v.max)) return false;
    } else if (t === "multi") {
      if (x.tags) { const s = String(val || ""); if (!v.some((tg) => s.includes(`|${tg}|`))) return false; }
      else if (!v.map(String).includes(String(val ?? ""))) return false;
    } else if (t === "tri") {
      const has = x.expr ? !!val : x.bool ? val === 1 || val === true || val === "1" : val != null && val !== "";
      if ((v === "yes") !== has) return false;
    } else if (t === "text") {
      if (!String(val ?? "").toLowerCase().includes(String(v).toLowerCase())) return false;
    } else if (t === "daterange") {
      const s = String(val || "").slice(0, 10); if (!s) return false;
      if (v.from && s < v.from) return false; if (v.to && s > v.to) return false;
    }
  }
  return true;
}

/** Counts for every multi-select facet (cached by the caller). */
export async function facetCounts(db) {
  const out = {};
  // sequential, one small GROUP BY at a time (the db is shared with background loaders)
  for (const id of FACET_IDS) {
    const col = byId[id][3];
    const onComps = col.startsWith("c.");
    const sql = onComps
      ? `SELECT ${col.slice(2)} AS v, count(*) AS n FROM v2_comps c WHERE c.status = 'active' AND ${col} IS NOT NULL AND ${col} <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 400`
      : `SELECT ${col} AS v, count(*) AS n FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.status = 'active' AND ${col} IS NOT NULL AND ${col} <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 400`;
    out[id] = (await db.prepare(sql).all()).results.map((r) => ({ v: String(r.v), n: r.n }));
  }
  const kinds = await db.prepare("SELECT record_kind AS v, count(*) AS n FROM v2_comps WHERE status = 'active' GROUP BY 1").all();
  out.kind = kinds.results; 
  const src = await db.prepare("SELECT source_kind AS v, count(*) AS n FROM v2_comps WHERE status = 'active' GROUP BY 1").all();
  out.source = src.results;
  const sev = await db.prepare("SELECT severity AS v, count(*) AS n FROM v2_comps WHERE status = 'active' GROUP BY 1").all();
  out.severity = sev.results;
  const tags = await db.prepare("SELECT p.product_tags AS v, count(*) AS n FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.status = 'active' AND p.product_tags IS NOT NULL GROUP BY 1").all();
  const tc = {};
  for (const r of tags.results) for (const t of String(r.v).split("|").filter(Boolean)) tc[t] = (tc[t] || 0) + r.n;
  out.tags = Object.entries(tc).sort((a, b) => b[1] - a[1]).map(([v, n]) => ({ v, n }));
  return out;
}

/** Browser copy of the field list (served at /comp-filters.js). */
export const CLIENT_JS = `window.COMP_FIELD_DEFS=${JSON.stringify(FIELD_DEFS)};`;
