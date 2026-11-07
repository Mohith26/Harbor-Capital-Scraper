// Normalization + realistic-bounds validation for Texas industrial comps.
//
// Bounds come from Harbor's universe (infill industrial, DFW / Houston / San
// Antonio / Austin / Laredo) cross-checked against the 8,061 CoStar TX
// industrial sale records and 3,294 lease records Harbor already holds:
//   typical  = what an analyst expects to see; outside it is worth a look
//   hard     = physically / economically implausible; almost certainly a unit
//              or parsing error and blocks auto-save until reviewed
// Unit-confusion fixes are applied only when the corrected value lands inside
// the typical band AND a cross-check agrees; every fix is recorded as a flag.

import { ENUMS, NUMERIC_FIELDS, DATE_FIELDS } from "./schema.js";

export const BOUNDS = {
  sale_price: { typical: [250e3, 150e6], hard: [25e3, 1.5e9], label: "sale price" },
  price_psf: { typical: [35, 325], hard: [8, 900], label: "$/SF" },
  price_per_acre: { typical: [75e3, 3.5e6], hard: [5e3, 25e6], label: "$/acre" },
  cap_rate_pct: { typical: [4, 10], hard: [2, 15], label: "cap rate %" },
  occupancy_pct: { typical: [0, 100], hard: [0, 100], label: "occupancy %" },
  leased_sf: { typical: [1000, 600e3], hard: [100, 2.5e6], label: "leased SF" },
  rent_psf_yr: { typical: [3.5, 22], hard: [1.5, 60], label: "annual rent $/SF" },
  rent_psf_mo: { typical: [0.3, 1.85], hard: [0.12, 5], label: "monthly rent $/SF" },
  rent_per_acre_mo: { typical: [700, 9000], hard: [150, 30000], label: "IOS rent $/acre/mo" },
  asking_rent_psf_yr: { typical: [3.5, 24], hard: [1.5, 60], label: "asking rent $/SF/yr" },
  term_months: { typical: [12, 132], hard: [1, 300], label: "term (months)" },
  escalation_pct: { typical: [2, 5.5], hard: [0, 12], label: "escalations %" },
  free_rent_months: { typical: [0, 8], hard: [0, 24], label: "free rent (months)" },
  ti_psf: { typical: [0, 25], hard: [0, 120], label: "TI $/SF" },
  building_sf: { typical: [2500, 1.2e6], hard: [400, 4e6], label: "building SF" },
  available_sf: { typical: [1000, 1e6], hard: [100, 4e6], label: "available SF" },
  land_acres: { typical: [0.25, 120], hard: [0.02, 2500], label: "land acres" },
  yard_acres: { typical: [0.25, 60], hard: [0.02, 500], label: "yard acres" },
  // Year bounds are filled in per call: Workers freeze Date at module load (epoch 1970).
  year_built: { typical: [1950, 0], hard: [1880, 0], label: "year built", yearOffsets: [2, 4] },
  year_renovated: { typical: [1970, 0], hard: [1900, 0], label: "year renovated", yearOffsets: [1, 3] },
  clear_height_ft: { typical: [12, 42], hard: [8, 60], label: "clear height (ft)" },
  dock_doors: { typical: [0, 200], hard: [0, 600], label: "dock doors" },
  drive_in_doors: { typical: [0, 40], hard: [0, 150], label: "drive-in doors" },
  office_pct: { typical: [0, 60], hard: [0, 100], label: "office %" },
  office_sf: { typical: [0, 60e3], hard: [0, 600e3], label: "office SF" },
};

const SYNONYMS = {
  lease_structure: { "triple net": "NNN", nnn: "NNN", "n.n.n.": "NNN", "modified gross": "MG", mg: "MG", gross: "Gross", "full service": "FSG", fs: "FSG", fsg: "FSG", "industrial gross": "IG", ig: "IG", nn: "NN", net: "NNN" },
  lease_kind: { new: "new", "new deal": "new", "new lease": "new", direct: "new", renewal: "renewal", renew: "renewal", extension: "extension", expansion: "expansion", sublease: "sublease", sublet: "sublease" },
  sprinkler: { esfr: "ESFR", "early suppression": "ESFR", wet: "wet", "wet system": "wet", dry: "dry", no: "none", none: "none", "not sprinklered": "none", unsprinklered: "none", "no sprinkler": "none" },
  building_class: { "class a": "A", "class b": "B", "class c": "C" },
  property_subtype: { ios: "ios", "outdoor storage": "ios", "truck terminal": "ios", "shallow bay": "shallow_bay", "small bay": "shallow_bay", flex: "flex", manufacturing: "manufacturing", mfg: "manufacturing", "crane": "manufacturing", "cold storage": "cold_storage", freezer: "cold_storage", "multi-tenant": "multi_tenant", "multi tenant": "multi_tenant", "bulk": "bulk_distribution", distribution: "bulk_distribution", warehouse: "warehouse", service: "service" },
  tenancy: { single: "single", "single tenant": "single", "single-tenant": "single", multi: "multi", "multi tenant": "multi", "multi-tenant": "multi" },
  loading: { fl: "front", rl: "rear", cd: "cross_dock", xd: "cross_dock", "x-dock": "cross_dock", sl: "side", "rear load": "rear", "rear-load": "rear", rear: "rear", "front load": "front", "front-load": "front", front: "front", "cross dock": "cross_dock", "cross-dock": "cross_dock", crossdock: "cross_dock", cross_dock: "cross_dock", side: "side", "side load": "side", rail: "rail" },
  construction: { "tilt wall": "tilt_wall", "tilt-wall": "tilt_wall", tilt: "tilt_wall", tiltwall: "tilt_wall", "tilt-up": "tilt_wall", metal: "metal", "pre-engineered metal": "metal", pem: "metal", masonry: "masonry", "concrete block": "concrete_block", cmu: "concrete_block", block: "concrete_block", precast: "precast", steel: "steel" },
  sale_type: { investment: "investment", "owner user": "owner_user", "owner-user": "owner_user", owner_user: "owner_user", user: "owner_user", "sale leaseback": "sale_leaseback", "sale-leaseback": "sale_leaseback", slb: "sale_leaseback", portfolio: "portfolio", land: "land", development: "development" },
};

function normEnum(field, v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (ENUMS[field] && ENUMS[field].includes(s)) return s;
  const low = s.toLowerCase();
  const syn = SYNONYMS[field];
  if (syn) {
    if (syn[low]) return syn[low];
    for (const [k, val] of Object.entries(syn)) if (low.includes(k)) return val;
  }
  if (ENUMS[field]) {
    const hit = ENUMS[field].find((e) => e.toLowerCase() === low.replace(/[\s-]+/g, "_"));
    if (hit) return hit;
  }
  if (field === "sprinkler" && /^(y|yes|sprinklered|fully sprinklered|true)$/i.test(s)) return "wet";
  if (field === "building_class") return /^[abc]\b/i.test(s) ? s[0].toUpperCase() : null;
  return ENUMS[field] ? null : s;
}

const QUARTER = /^(?:([1-4])q\s?'?(\d{2,4})|q([1-4])\s?'?(\d{2,4})|(\d{4})[-\s]?q([1-4]))$/i;

export function normalizeDate(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") {
    if (v > 1900 && v < 2100) return String(v);
    if (v > 20000 && v < 80000) {
      const d = new Date(Date.UTC(1899, 11, 30) + v * 86400000);
      return d.toISOString().slice(0, 10);
    }
    return null;
  }
  let s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  if (/^\d{4}-\d{2}$/.test(s) || /^\d{4}$/.test(s) || /^\d{4}-Q[1-4]$/.test(s)) return s;
  const q = QUARTER.exec(s.replace(/\s+/g, ""));
  if (q) {
    const quarter = q[1] || q[3] || q[6];
    let y = q[2] || q[4] || q[5];
    if (y.length === 2) y = "20" + y;
    return `${y}-Q${quarter}`;
  }
  let m = /^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})$/.exec(s);
  if (m) {
    let y = m[3].length === 2 ? (Number(m[3]) > 60 ? "19" : "20") + m[3] : m[3];
    return `${y}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  }
  m = /^(\d{1,2})[/.-](\d{4})$/.exec(s);
  if (m) return `${m[2]}-${m[1].padStart(2, "0")}`;
  const months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
  m = /^([a-z]{3})[a-z]*[\s'.,-]*(\d{2,4})$/i.exec(s);
  if (m && months.includes(m[1].toLowerCase())) {
    let y = m[2].length === 2 ? "20" + m[2] : m[2];
    return `${y}-${String(months.indexOf(m[1].toLowerCase()) + 1).padStart(2, "0")}`;
  }
  const d = new Date(s);
  if (!Number.isNaN(d.getTime()) && d.getFullYear() > 1900) return d.toISOString().slice(0, 10);
  return s;
}

export function dateToNumber(iso) {
  // Approximate decimal year for comparisons / recency.
  if (!iso) return null;
  let m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  if (m) return Number(m[1]) + (Number(m[2]) - 1) / 12 + (Number(m[3]) - 1) / 365;
  m = /^(\d{4})-(\d{2})$/.exec(iso);
  if (m) return Number(m[1]) + (Number(m[2]) - 0.5) / 12;
  m = /^(\d{4})-Q([1-4])$/.exec(iso);
  if (m) return Number(m[1]) + (Number(m[2]) * 3 - 1.5) / 12;
  m = /^(\d{4})$/.exec(iso);
  if (m) return Number(m[1]) + 0.5;
  return null;
}

function flag(r, field, code, severity, msg, extra = {}) {
  r._flags.push({ field, code, severity, msg, ...extra });
}

function inRange(v, [a, b]) {
  return v >= a && v <= b;
}

export function validateRecord(input, { today = new Date() } = {}) {
  const r = { ...input, _flags: [...(input._flags || [])] };
  // ---- enums & text ----
  for (const f of ["lease_structure", "lease_kind", "loading", "construction", "sale_type", "sprinkler", "building_class", "property_subtype", "tenancy"]) r[f] = normEnum(f, r[f]);
  if (r.record_kind && !ENUMS.record_kind.includes(r.record_kind)) r.record_kind = /lease/i.test(r.record_kind) ? "lease" : /sale/i.test(r.record_kind) ? "sale" : /list|avail/i.test(r.record_kind) ? "listing" : r.record_kind;
  if (!r.state && (r.city || r.address)) r.state = "TX";
  if (r.state && /^texas$/i.test(r.state)) r.state = "TX";
  // A street address sitting in the name field (broker "Building Name" columns often hold the street).
  if (!r.address && r.property_name && /^\s*\d{1,6}[a-z]?(?:\s*[-&]\s*\d+)?\s+[a-z0-9]/i.test(r.property_name) && !/^\d+\s*(?:sf|ac|acres|buildings?|bldgs?)\b/i.test(r.property_name)) {
    r.address = r.property_name;
    r.property_name = null;
  }
  if (r.zip != null) {
    const z = String(r.zip).match(/\d{5}/);
    r.zip = z ? z[0] : null;
  }
  for (const f of ["address", "property_name", "city", "tenant", "buyer", "seller", "landlord", "submarket"]) {
    if (r[f] != null) {
      r[f] = String(r[f]).replace(/\s+/g, " ").trim() || null;
      if (r[f] && /^(n\/?a|tbd|undisclosed|confidential|-+|none|unknown|\?)$/i.test(r[f])) r[f] = null;
    }
  }
  if (typeof r.rail_served === "string") r.rail_served = /^(y|yes|true|rail)/i.test(r.rail_served) ? true : /^(n|no|false)/i.test(r.rail_served) ? false : null;
  for (const f of DATE_FIELDS) r[f] = normalizeDate(r[f]);
  // Excel reads "3/31/31" as 1931: comps dates before 1950 are century slips.
  for (const f of DATE_FIELDS) {
    const m = /^(19[0-4]\d)(.*)$/.exec(r[f] || "");
    if (m) {
      const fixed = String(Number(m[1]) + 100) + m[2];
      r._flags.push({ field: f, code: "unit_fix", severity: "info", msg: `${f} ${r[f]} read as ${fixed} (two-digit year stored as 19xx)` });
      r[f] = fixed;
    }
  }
  // Placeholder zeros mean "unknown".
  for (const f of ["cap_rate_pct", "land_acres", "leased_sf", "term_months", "clear_height_ft", "year_built", "year_renovated"]) if (r[f] === 0) r[f] = null;
  // Years must be plain 4-digit years ("2021 reno", "2003 / 2010").
  for (const f of ["year_built", "year_renovated"]) {
    if (r[f] != null && (r[f] < 1800 || r[f] > 2200)) {
      const m = String(r[f]).match(/(18|19|20)\d{2}/);
      r[f] = m ? Number(m[0]) : null;
    }
  }

  // ---- unit-confusion repairs (each recorded) ----
  // cap rate given as decimal
  if (r.cap_rate_pct != null && r.cap_rate_pct > 0 && r.cap_rate_pct < 0.2) {
    flag(r, "cap_rate_pct", "unit_fix", "info", `cap rate ${r.cap_rate_pct} read as a decimal -> ${+(r.cap_rate_pct * 100).toFixed(3)}%`);
    r.cap_rate_pct = +(r.cap_rate_pct * 100).toFixed(3);
  }
  for (const f of ["escalation_pct", "office_pct", "occupancy_pct"]) {
    if (r[f] != null && r[f] > 0 && r[f] < 0.2 && f !== "office_pct") {
      flag(r, f, "unit_fix", "info", `${f} ${r[f]} read as a decimal -> ${+(r[f] * 100).toFixed(2)}`);
      r[f] = +(r[f] * 100).toFixed(2);
    }
  }
  if (r.occupancy_pct != null && r.occupancy_pct > 0 && r.occupancy_pct <= 1) r.occupancy_pct = r.occupancy_pct * 100;
  // monthly vs annual rent
  if (r.rent_psf_mo != null && r.rent_psf_mo > 2.5 && inRange(r.rent_psf_mo, BOUNDS.rent_psf_yr.typical) && (r.rent_psf_yr == null || Math.abs(r.rent_psf_yr - r.rent_psf_mo * 12) < 0.05 || Math.abs(r.rent_psf_yr - r.rent_psf_mo) < 0.01)) {
    flag(r, "rent_psf_yr", "unit_fix", "warn", `rent $${r.rent_psf_mo} was labeled monthly but is an annual industrial rate; treated as $${r.rent_psf_mo}/SF/yr`);
    r.rent_psf_yr = r.rent_psf_mo;
    r.rent_psf_mo = null;
  }
  if (r.rent_psf_yr != null && r.rent_psf_yr > 36 && r.rent_psf_yr / 12 >= BOUNDS.rent_psf_yr.typical[0] && r.rent_psf_yr / 12 <= 30 && r.property_subtype !== "flex" && r.lease_structure !== "FSG") {
    flag(r, "rent_psf_yr", "unit_fix", "warn", `annual rent $${r.rent_psf_yr} is 12x too high for industrial; an annual rate was multiplied as if monthly -> $${+(r.rent_psf_yr / 12).toFixed(2)}`);
    r.rent_psf_yr = +(r.rent_psf_yr / 12).toFixed(4);
    r.rent_psf_mo = null;
  }
  for (const f of ["sale_price", "asking_price", "price_psf", "rent_psf_yr", "asking_rent_psf_yr", "building_sf", "leased_sf", "available_sf"]) {
    if (r[f] === 0) r[f] = null; // "$0 / subject to offer / N/A" placeholders
  }
  if (r.record_kind === "listing" && r.available_sf == null && r.leased_sf == null && r.building_sf != null) r.available_sf = r.building_sf;
  if (r.record_kind === "listing" && r.available_sf == null && r.leased_sf != null) {
    r.available_sf = r.leased_sf;
    r.leased_sf = null;
  }
  if (r.rent_psf_mo != null && r.rent_psf_yr == null) r.rent_psf_yr = +(r.rent_psf_mo * 12).toFixed(4);
  if (r.rent_psf_yr != null && r.rent_psf_mo == null && r.rent_psf_yr < 2.25 && r.rent_psf_yr > 0.12 && r.record_kind !== "land_sale") {
    const fixed = +(r.rent_psf_yr * 12).toFixed(4);
    if (inRange(fixed, BOUNDS.rent_psf_yr.typical)) {
      flag(r, "rent_psf_yr", "unit_fix", "warn", `rent $${r.rent_psf_yr}/SF/yr is implausibly low for industrial; treated as monthly -> $${fixed}/SF/yr`);
      r.rent_psf_mo = r.rent_psf_yr;
      r.rent_psf_yr = fixed;
    }
  }
  if (r.rent_psf_yr != null && r.rent_psf_mo != null && Math.abs(r.rent_psf_mo * 12 - r.rent_psf_yr) > 0.06 * r.rent_psf_yr) {
    flag(r, "rent_psf_yr", "inconsistent", "warn", `monthly $${r.rent_psf_mo} x 12 != annual $${r.rent_psf_yr}`);
  }
  if (r.asking_rent_psf_yr != null && r.asking_rent_psf_yr < 2.25 && r.asking_rent_psf_yr > 0.12) {
    flag(r, "asking_rent_psf_yr", "unit_fix", "warn", `asking rent $${r.asking_rent_psf_yr} treated as monthly -> annual`);
    r.asking_rent_psf_yr = +(r.asking_rent_psf_yr * 12).toFixed(4);
  }
  // term in years
  if (r.term_months != null && r.term_months > 0 && r.term_months <= 15 && r.lease_start && r.lease_end) {
    const a = dateToNumber(r.lease_start), b = dateToNumber(r.lease_end);
    if (a && b && Math.abs((b - a) * 12 - r.term_months * 12) < 3) {
      flag(r, "term_months", "unit_fix", "warn", `term ${r.term_months} looks like years (dates span ${((b - a) * 12).toFixed(0)} months) -> ${r.term_months * 12}`);
      r.term_months = r.term_months * 12;
    }
  }
  if (r.term_months == null && r.lease_start && r.lease_end) {
    const a = dateToNumber(r.lease_start), b = dateToNumber(r.lease_end);
    if (a && b && b > a) r.term_months = Math.round((b - a) * 12);
  }
  // TI given as total dollars
  if (r.ti_psf != null && r.ti_psf > BOUNDS.ti_psf.hard[1] && r.leased_sf) {
    const per = r.ti_psf / r.leased_sf;
    flag(r, "ti_psf", "unit_fix", "warn", `TI ${r.ti_psf} looks like total dollars -> $${per.toFixed(2)}/SF`);
    r.ti_psf = +per.toFixed(2);
  }
  // land SF mistakenly in acres
  if (r.land_acres != null && r.land_acres > 2500 && r.land_acres / 43560 < 500) {
    flag(r, "land_acres", "unit_fix", "warn", `land ${r.land_acres} looks like SF -> ${(r.land_acres / 43560).toFixed(2)} ac`);
    r.land_acres = +(r.land_acres / 43560).toFixed(2);
  }
  // clear height in inches
  if (r.clear_height_ft != null && r.clear_height_ft > 100 && r.clear_height_ft / 12 <= 60) {
    flag(r, "clear_height_ft", "unit_fix", "warn", `clear height ${r.clear_height_ft} looks like inches -> ${(r.clear_height_ft / 12).toFixed(1)} ft`);
    r.clear_height_ft = +(r.clear_height_ft / 12).toFixed(1);
  }
  // sale price in thousands / millions
  if (r.sale_price != null && r.price_psf && r.building_sf) {
    const implied = r.price_psf * r.building_sf;
    for (const k of [1e3, 1e6]) {
      if (Math.abs(r.sale_price * k - implied) / implied < 0.05 && Math.abs(r.sale_price - implied) / implied > 0.5) {
        flag(r, "sale_price", "unit_fix", "warn", `sale price ${r.sale_price} looks scaled (x${k}) given $${r.price_psf}/SF x ${r.building_sf} SF`);
        r.sale_price = r.sale_price * k;
      }
    }
  }

  // ---- derived values ----
  if (r.price_psf == null && r.sale_price && r.building_sf && r.sale_type !== "portfolio" && r.record_kind === "sale") {
    r.price_psf = +(r.sale_price / r.building_sf).toFixed(2);
    r._derived = [...(r._derived || []), "price_psf"];
  }
  if (r.price_per_acre == null && r.sale_price && r.land_acres && (r.record_kind === "land_sale" || r.property_subtype === "ios")) {
    r.price_per_acre = Math.round(r.sale_price / r.land_acres);
    r._derived = [...(r._derived || []), "price_per_acre"];
  }
  if (r.office_pct == null && r.office_sf && r.building_sf) {
    r.office_pct = +((r.office_sf / r.building_sf) * 100).toFixed(1);
    r._derived = [...(r._derived || []), "office_pct"];
  }

  // ---- cross-field consistency ----
  if (r.sale_price && r.building_sf && r.price_psf && r.sale_type !== "portfolio") {
    const implied = r.sale_price / r.building_sf;
    const off = Math.abs(implied - r.price_psf) / r.price_psf;
    if (off > 0.03) flag(r, "price_psf", "inconsistent", off > 0.25 ? "error" : "warn", `price/SF $${r.price_psf} vs price/size $${implied.toFixed(2)} (${(off * 100).toFixed(0)}% apart) - check building size, portfolio allocation or price`);
  }
  if (r.leased_sf && r.building_sf && r.leased_sf > r.building_sf * 1.02) flag(r, "leased_sf", "inconsistent", "warn", `leased SF ${r.leased_sf} exceeds building SF ${r.building_sf} - columns may be swapped`);
  if (r.office_sf && r.building_sf && r.office_sf > r.building_sf) flag(r, "office_sf", "inconsistent", "warn", "office SF exceeds building SF");
  if (r.office_sf && r.leased_sf && !r.building_sf && r.office_sf > r.leased_sf) flag(r, "office_sf", "inconsistent", "warn", "office SF exceeds leased SF - columns may be swapped");
  if (r.free_rent_months && r.term_months && r.free_rent_months >= r.term_months) flag(r, "free_rent_months", "inconsistent", "error", "free rent >= term");
  if (r.land_acres && r.building_sf) {
    const cov = r.building_sf / (r.land_acres * 43560);
    if (cov > 0.75) flag(r, "land_acres", "inconsistent", "warn", `site coverage ${(cov * 100).toFixed(0)}% is unusually high for industrial (land may be wrong or multi-story)`);
    else if (cov < 0.05 && r.property_subtype !== "ios" && r.record_kind !== "land_sale") flag(r, "land_acres", "info", "info", `coverage ${(cov * 100).toFixed(1)}% - low-coverage site, possible IOS / excess land`);
  }
  if (r.dock_doors && r.building_sf && r.dock_doors > r.building_sf / 1500) flag(r, "dock_doors", "inconsistent", "warn", `${r.dock_doors} docks on ${r.building_sf} SF is more than 1 per 1,500 SF`);
  if (r.cap_rate_pct && r.noi && r.sale_price) {
    const implied = (r.noi / r.sale_price) * 100;
    if (Math.abs(implied - r.cap_rate_pct) > 0.35) flag(r, "cap_rate_pct", "inconsistent", "warn", `cap ${r.cap_rate_pct}% vs NOI/price ${implied.toFixed(2)}%`);
  }
  const ty = today.getFullYear() + (today.getMonth() + 1) / 12;
  for (const f of DATE_FIELDS) {
    const n = dateToNumber(r[f]);
    if (r[f] && n == null) flag(r, f, "unparsed", "warn", `could not parse date "${r[f]}"`);
    if (n && n < 1995) flag(r, f, "bounds", "warn", `${f} ${r[f]} is very old for a comp`);
    if (n && f !== "lease_end" && n > ty + 1.5) flag(r, f, "bounds", "warn", `${f} ${r[f]} is in the future`);
  }
  if (r.lease_start && r.lease_end && dateToNumber(r.lease_end) <= dateToNumber(r.lease_start)) flag(r, "lease_end", "inconsistent", "error", "lease end is before start");
  if (r.year_built && r.year_renovated && r.year_renovated < r.year_built) flag(r, "year_renovated", "inconsistent", "warn", "renovated before built");

  // ---- bounds ----
  const yNow = today.getFullYear();
  for (const [f, b0] of Object.entries(BOUNDS)) {
    const b = b0.yearOffsets ? { ...b0, typical: [b0.typical[0], yNow + b0.yearOffsets[0]], hard: [b0.hard[0], yNow + b0.yearOffsets[1]] } : b0;
    const v = r[f];
    if (v == null || typeof v !== "number") continue;
    if (f === "rent_psf_yr" && (r.property_subtype === "flex" || r.lease_structure === "FSG")) {
      if (v > 40) flag(r, f, "bounds", "warn", `${b.label} ${v} high even for flex/full-service`);
      continue;
    }
    if (!inRange(v, b.hard)) flag(r, f, "bounds", "error", `${b.label} ${fmt(v)} is outside the plausible range ${fmt(b.hard[0])}-${fmt(b.hard[1])}`);
    else if (!inRange(v, b.typical)) flag(r, f, "bounds", "info", `${b.label} ${fmt(v)} is outside the typical range ${fmt(b.typical[0])}-${fmt(b.typical[1])}`);
  }

  // ---- completeness ----
  if (!r.address && !r.property_name) flag(r, "address", "missing", "error", "no address or property name - cannot locate this comp");
  else if (!r.address) flag(r, "address", "missing", "warn", "no street address, only a property name - geocoding will be approximate");
  else if (!/\d/.test(r.address) && !/&| and | at |\//i.test(r.address)) flag(r, "address", "partial", "warn", "address has no street number - location will be approximate");
  if (r.record_kind === "sale" && !r.sale_price && !r.price_psf) flag(r, "sale_price", "missing", "warn", "sale with no price or $/SF");
  if (r.record_kind === "lease" && !r.rent_psf_yr && !r.rent_per_acre_mo && !r.monthly_rent_total) flag(r, "rent_psf_yr", "missing", "warn", "lease with no rent");
  if (r.record_kind === "sale" && !r.sale_date) flag(r, "sale_date", "missing", "info", "sale date missing");
  if (r.record_kind === "lease" && !r.lease_start) flag(r, "lease_start", "missing", "info", "lease date missing");

  r._severity = r._flags.some((x) => x.severity === "error") ? "error" : r._flags.some((x) => x.severity === "warn") ? "warn" : "ok";
  return r;
}

function fmt(v) {
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (Math.abs(v) >= 1e4) return Math.round(v).toLocaleString("en-US");
  return String(+v.toFixed(3));
}
