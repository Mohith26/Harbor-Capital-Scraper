// Match OMKB deals (OM / flyer extracted facts) to comp-DB properties and
// emit "om" feature observations. Strict matching: same address key within
// 0.5 mi, or <150 ft with similar address and SF within 25%.
// Usage: node omkb-features.mjs <omkb_deals.json> <props.json> <out.json>
import fs from "node:fs";
import { parseAddress, addrSimilarity, haversineMi } from "../geo.js";
const [dealsF, propsF, outF] = process.argv.slice(2);
const deals = JSON.parse(fs.readFileSync(dealsF, "utf8"))[0].results;
const props = JSON.parse(fs.readFileSync(propsF, "utf8"))[0].results;
const cell = (lat, lng) => `${Math.round(lat * 100)}|${Math.round(lng * 100)}`;
const grid = new Map();
for (const p of props) { const k = cell(p.latitude, p.longitude); if (!grid.has(k)) grid.set(k, []); grid.get(k).push(p); }
const near = (lat, lng) => { const out = []; const a = Math.round(lat * 100), b = Math.round(lng * 100); for (let i = -1; i <= 1; i++) for (let j = -1; j <= 1; j++) out.push(...(grid.get(`${a + i}|${b + j}`) || [])); return out; };
const sfOk = (a, b) => !a || !b || Math.abs(a - b) / Math.max(a, b) <= 0.25;
const CONS = [[/tilt/i, "tilt_wall"], [/precast/i, "precast"], [/metal|steel/i, "metal"], [/masonry|brick|block|cmu/i, "masonry"], [/concrete/i, "concrete_block"]];
const SUB = { bulk_distribution: "bulk_distribution", flex: "flex", ios: "ios", manufacturing: "manufacturing", heavy_industrial: "manufacturing", cold_storage: "cold_storage", showroom: "flex", shallow_bay: "shallow_bay", multi_tenant: "multi_tenant", service: "service" };
const matches = new Map(); // property_id -> {deal, how, d}
let ambiguous = 0;
for (const d of deals) {
  const dk = parseAddress(d.address || "").key;
  const cands = [];
  for (const p of near(d.lat, d.lng)) {
    const dist = haversineMi(d.lat, d.lng, p.latitude, p.longitude);
    if (dist > 0.5) continue;
    const sameKey = dk && p.addr_key && dk === p.addr_key;
    const sim = d.address && p.address ? addrSimilarity(d.address, p.address) : 0;
    if (sameKey && sfOk(d.building_sf, p.building_sf)) cands.push({ p, how: "address", dist, s: 2 - dist });
    else if (dist < 0.0285 && sim >= 0.6 && d.building_sf && p.building_sf && sfOk(d.building_sf, p.building_sf)) cands.push({ p, how: "geo+sf", dist, s: 1 - dist });
  }
  if (!cands.length) continue;
  cands.sort((a, b) => b.s - a.s);
  if (cands.length > 1 && cands[1].how === cands[0].how && cands[0].how === "geo+sf") { ambiguous++; continue; }
  for (const c of cands.filter((c) => c.how === cands[0].how).slice(0, 1)) {
    const prev = matches.get(c.p.id);
    if (!prev || c.s > prev.s || (c.s === prev.s && (d.doc_count || 0) > (prev.deal.doc_count || 0))) matches.set(c.p.id, { deal: d, how: c.how, s: c.s });
  }
}
const items = [];
const fieldN = {};
for (const [pid, { deal: d, how }] of matches) {
  const obs = [];
  const ref = `OMKB ${d.source === "upload" ? "upload" : "listing"} ${d.id}`;
  const add = (field, value) => { if (value == null || value === "" || (typeof value === "number" && !Number.isFinite(value))) return; obs.push({ field, value, source: "om", source_ref: ref }); fieldN[field] = (fieldN[field] || 0) + 1; };
  if (d.building_sf > 500) add("building_sf", d.building_sf);
  if (d.land_acres > 0) add("land_acres", d.land_acres);
  if (d.year_built >= 1880 && d.year_built <= 2030) add("year_built", d.year_built);
  if (d.clear_height_ft >= 8 && d.clear_height_ft <= 60) add("clear_height_ft", d.clear_height_ft);
  if (d.dock_doors != null && d.dock_doors >= 0 && d.dock_doors < 500) add("dock_doors", d.dock_doors);
  if (d.grade_doors != null && d.grade_doors >= 0 && d.grade_doors < 300) add("drive_in_doors", d.grade_doors);
  const cfg = String(d.config || "").toLowerCase();
  add("loading", /cross/.test(cfg) ? "cross_dock" : /rear/.test(cfg) ? "rear" : /front/.test(cfg) ? "front" : /side/.test(cfg) ? "side" : null);
  if (d.construction) add("construction", CONS.find(([re]) => re.test(d.construction))?.[1] || null);
  if (d.office_sf > 0 && (!d.building_sf || d.office_sf < d.building_sf)) add("office_sf", d.office_sf);
  if (d.sprinkler && !/^(no|none)$/i.test(d.sprinkler)) add("sprinkler", /esfr/i.test(d.sprinkler) ? "ESFR" : /dry/i.test(d.sprinkler) ? "dry" : "wet");
  if (d.rail === 1) add("rail_served", true);
  if (d.yard_acres > 0 && d.yard_acres < 500) add("yard_acres", d.yard_acres);
  if (d.asset_type && SUB[d.asset_type]) add("property_subtype", SUB[d.asset_type]);
  if (d.tenancy && /^(single|multi)/i.test(d.tenancy)) add("tenancy", /^multi/i.test(d.tenancy) ? "multi" : "single");
  const pw = d.power_detail || (d.power_amps ? `${d.power_amps}A${d.power_volts ? "/" + d.power_volts + "V" : ""}${d.power_phase ? " " + d.power_phase + "-phase" : ""}` : null);
  if (pw) add("power", String(pw).slice(0, 120));
  if (d.crane === 1) add("cranes", String(d.crane_detail || [d.crane_count && `${d.crane_count} cranes`, d.crane_max_tons && `up to ${d.crane_max_tons} ton`].filter(Boolean).join(", ") || "yes").slice(0, 120));
  if (obs.length) items.push({ property_id: pid, how, obs });
}
fs.writeFileSync(outF, JSON.stringify(items));
console.log("deals", deals.length, "matched properties", items.length, "by", JSON.stringify(items.reduce((m, i) => ((m[i.how] = (m[i.how] || 0) + 1), m), {})), "ambiguous skipped", ambiguous, "fields", JSON.stringify(fieldN));
