// Two-level dedupe.
//
//  Property level - is this the same physical building?
//    address similarity (number ranges, suffix/direction normalization,
//    building designators) + geocode distance + name similarity.
//  Transaction level - is this the same deal we already hold?
//    sale:  same property, dates within 120 days (or one undated), price
//           within 2% / $psf within 3%, or same buyer+seller pair
//    lease: same property, tenant match (or both blank), leased SF within
//           5%, start within 6 months
//
// Outcomes: "new" | "duplicate" (same deal; merge missing fields, keep the
// extra source) | "conflict" (same deal key but material numbers disagree:
// needs a human) | "related" (same building, different deal -> attaches to
// the existing property).

import { addrSimilarity, haversineMi, parseAddress } from "./geo.js";
import { dateToNumber } from "./validate.js";

const norm = (s) => String(s || "").toLowerCase().replace(/\b(llc|inc|ltd|lp|corp|corporation|company|co|the|group|holdings|properties|trust|partners|realty)\b/g, "").replace(/[^a-z0-9]/g, "");

export function nameSimilarity(a, b) {
  const x = norm(a), y = norm(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.length > 3 && y.length > 3 && (x.includes(y) || y.includes(x))) return 0.9;
  // bigram dice
  const bg = (s) => {
    const m = new Map();
    for (let i = 0; i < s.length - 1; i++) m.set(s.slice(i, i + 2), (m.get(s.slice(i, i + 2)) || 0) + 1);
    return m;
  };
  const A = bg(x), B = bg(y);
  let inter = 0;
  for (const [k, v] of A) inter += Math.min(v, B.get(k) || 0);
  return (2 * inter) / Math.max(1, x.length - 1 + y.length - 1);
}

// Score 0..1 that two records/properties are the same building.
export function samePropertyScore(a, b) {
  let s = 0;
  const reasons = [];
  const A = a.address ? parseAddress(a.address) : null;
  const B = b.address ? parseAddress(b.address) : null;
  let addr = 0;
  if (A && B && A.core && B.core) {
    addr = addrSimilarity(A, B);
    if (A.building && B.building && A.building !== B.building) {
      return { score: 0.1, reasons: [`different buildings (${A.building} vs ${B.building})`] };
    }
  }
  const haveGeo = a.latitude != null && b.latitude != null;
  const d = haveGeo ? haversineMi(a.latitude, a.longitude, b.latitude, b.longitude) : null;
  const preciseA = ["source", "rooftop", "parcel", "interpolated"].includes(a.geocode_quality || "rooftop");
  const preciseB = ["source", "rooftop", "parcel", "interpolated"].includes(b.geocode_quality || "rooftop");
  const name = a.property_name && b.property_name ? nameSimilarity(a.property_name, b.property_name) : 0;
  const sameCity = a.city && b.city ? norm(a.city) === norm(b.city) : null;
  const sameZip = a.zip && b.zip ? a.zip === b.zip : null;

  if (addr >= 0.95) {
    s = 0.85;
    reasons.push("address match");
  } else if (addr >= 0.75) {
    s = 0.6;
    reasons.push("address close");
  }
  if (d != null && preciseA && preciseB) {
    if (d < 0.03) {
      s += 0.25;
      reasons.push(`${Math.round(d * 5280)} ft apart`);
    } else if (d < 0.12) {
      s += 0.1;
      reasons.push(`${Math.round(d * 5280)} ft apart`);
    } else if (d > 1.0) {
      s -= 0.5;
      reasons.push(`${d.toFixed(1)} mi apart`);
    }
  } else if (d != null && d > 15) {
    s -= 0.5;
    reasons.push(`${d.toFixed(0)} mi apart`);
  }
  if (name >= 0.85) {
    s += addr ? 0.1 : 0.55;
    reasons.push("same name");
  }
  if (sameZip === false && addr < 0.95) s -= 0.2;
  if (sameCity === false && sameZip !== true && addr < 0.95) s -= 0.15;
  // Same street number but one is a range ("5851-5861") containing the other.
  return { score: Math.max(0, Math.min(1, s)), reasons, distanceMi: d };
}

function within(a, b, pct) {
  if (a == null || b == null) return null;
  if (a === 0 && b === 0) return true;
  return Math.abs(a - b) / Math.max(Math.abs(a), Math.abs(b)) <= pct;
}

// Compare two transactions on the SAME property.
export function sameDealScore(x, y) {
  if (x.record_kind !== y.record_kind) {
    // listing vs lease of the same space is not a duplicate
    return { score: 0, verdict: "different_kind" };
  }
  const reasons = [];
  const conflicts = [];
  if (x.record_kind === "sale" || x.record_kind === "land_sale") {
    const dx = dateToNumber(x.sale_date), dy = dateToNumber(y.sale_date);
    const dateClose = dx != null && dy != null ? Math.abs(dx - dy) <= 120 / 365 : null;
    const dateFar = dx != null && dy != null ? Math.abs(dx - dy) > 1.0 : false;
    const price = within(x.sale_price, y.sale_price, 0.02);
    const psf = within(x.price_psf, y.price_psf, 0.03);
    const parties = x.buyer && y.buyer ? nameSimilarity(x.buyer, y.buyer) >= 0.8 : null;
    if (dateFar) return { score: 0.05, verdict: "different_deal", reasons: ["sale dates more than a year apart (re-sale)"] };
    let s = 0;
    if (dateClose) { s += 0.4; reasons.push("same sale date"); }
    else if (dx == null || dy == null) s += 0.15;
    if (price) { s += 0.45; reasons.push("same price"); }
    else if (psf) { s += 0.35; reasons.push("same $/SF"); }
    if (parties) { s += 0.25; reasons.push("same buyer"); }
    if (price === false && psf === false) conflicts.push(`price ${x.sale_price ?? "?"} vs ${y.sale_price ?? "?"}`);
    if (x.cap_rate_pct && y.cap_rate_pct && Math.abs(x.cap_rate_pct - y.cap_rate_pct) > 0.3) conflicts.push(`cap ${x.cap_rate_pct}% vs ${y.cap_rate_pct}%`);
    if (x.building_sf && y.building_sf && !within(x.building_sf, y.building_sf, 0.05)) conflicts.push(`SF ${x.building_sf} vs ${y.building_sf}`);
    const score = Math.min(1, s);
    if (score >= 0.6 && conflicts.length) return { score, verdict: "conflict", reasons, conflicts };
    if (score >= 0.6) return { score, verdict: "duplicate", reasons };
    if (dateClose && conflicts.length) return { score: 0.5, verdict: "conflict", reasons, conflicts };
    return { score, verdict: "different_deal", reasons };
  }
  // leases / listings
  const tx = x.tenant ? norm(x.tenant) : "", ty = y.tenant ? norm(y.tenant) : "";
  const tenantSim = tx && ty ? nameSimilarity(x.tenant, y.tenant) : null;
  const sfX = x.leased_sf ?? x.available_sf, sfY = y.leased_sf ?? y.available_sf;
  const sf = within(sfX, sfY, 0.05);
  const dx = dateToNumber(x.lease_start), dy = dateToNumber(y.lease_start);
  const dateClose = dx != null && dy != null ? Math.abs(dx - dy) <= 0.5 : null;
  const dateFar = dx != null && dy != null ? Math.abs(dx - dy) > 1.5 : false;
  const rent = within(x.rent_psf_yr, y.rent_psf_yr, 0.03);
  if (tenantSim != null && tenantSim < 0.5) return { score: 0.05, verdict: "different_deal", reasons: ["different tenants"] };
  if (dateFar) return { score: 0.1, verdict: "different_deal", reasons: ["lease dates far apart (renewal / new deal)"] };
  let s = 0;
  if (tenantSim != null && tenantSim >= 0.8) { s += 0.45; reasons.push("same tenant"); }
  if (sf) { s += 0.3; reasons.push("same SF"); }
  if (dateClose) { s += 0.25; reasons.push("same start"); }
  else if (dx == null || dy == null) s += 0.1;
  if (rent) { s += 0.15; reasons.push("same rent"); }
  if (rent === false && x.rent_psf_yr && y.rent_psf_yr) conflicts.push(`rent $${x.rent_psf_yr} vs $${y.rent_psf_yr}`);
  if (sf === false) conflicts.push(`SF ${sfX} vs ${sfY}`);
  if (x.term_months && y.term_months && Math.abs(x.term_months - y.term_months) > 3) conflicts.push(`term ${x.term_months} vs ${y.term_months} mo`);
  if (x.ti_psf != null && y.ti_psf != null && Math.abs(x.ti_psf - y.ti_psf) > 0.5) conflicts.push(`TI $${x.ti_psf} vs $${y.ti_psf}`);
  // No tenant on either side: require SF + rent + date.
  if (tenantSim == null && !(sf && (rent || dateClose))) return { score: Math.min(s, 0.4), verdict: "different_deal", reasons };
  const score = Math.min(1, s);
  if (score >= 0.6 && conflicts.length) return { score, verdict: "conflict", reasons, conflicts };
  if (score >= 0.6) return { score, verdict: "duplicate", reasons };
  return { score, verdict: "different_deal", reasons };
}

// Merge a duplicate into the canonical record: fill only missing fields.
export function mergeMissing(canonical, incoming, fields) {
  const filled = {};
  for (const f of fields) {
    if ((canonical[f] == null || canonical[f] === "") && incoming[f] != null && incoming[f] !== "") filled[f] = incoming[f];
  }
  return filled;
}

// Within one upload: collapse exact repeats (hidden duplicate tabs, comps that
// appear on two sheets of the same workbook).
export function dedupeBatch(records) {
  const groups = [];
  records.forEach((r, i) => {
    if (r.record_kind === "subject") return;
    for (const g of groups) {
      const head = records[g[0]];
      const p = samePropertyScore(head, r);
      if (p.score < 0.8) continue;
      const d = sameDealScore(head, r);
      if (d.verdict === "duplicate" || d.verdict === "conflict") {
        g.push(i);
        r._batchDup = { of: g[0], verdict: d.verdict, reasons: [...p.reasons, ...(d.reasons || [])], conflicts: d.conflicts || [] };
        return;
      }
    }
    groups.push([i]);
  });
  return records;
}

// Portfolio detection: identical sale price + date on several buildings.
export function tagPortfolios(records) {
  const byKey = new Map();
  records.forEach((r, i) => {
    if (r.record_kind !== "sale" || !r.sale_price) return;
    const k = `${r.sale_price}|${r.sale_date || ""}|${norm(r.buyer)}`;
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(i);
  });
  for (const idx of byKey.values()) {
    if (idx.length < 2) continue;
    const addrs = new Set(idx.map((i) => parseAddress(records[i].address || records[i].property_name || String(i)).key));
    if (addrs.size < 2) continue;
    for (const i of idx) {
      records[i].sale_type = "portfolio";
      records[i]._portfolio = { size: idx.length };
      records[i]._flags = records[i]._flags || [];
      records[i]._flags.push({ field: "sale_price", code: "portfolio", severity: "info", msg: `same price/date as ${idx.length - 1} other building(s): portfolio sale, price is the total - $/SF excluded from stats` });
    }
  }
  return records;
}
