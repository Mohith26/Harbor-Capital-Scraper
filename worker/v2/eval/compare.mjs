// Compare pipeline output against hand-extracted gold records.
// usage: node compare.mjs <golddir> <outdir> [--diffs]
import fs from "node:fs";
import path from "node:path";

const [goldDir, outDir, ...flags] = process.argv.slice(2);
const SHOW = flags.includes("--diffs");

const FIELDS = [
  "record_kind", "property_name", "address", "city", "zip", "submarket",
  "sale_price", "price_psf", "price_per_acre", "cap_rate_pct", "sale_date", "buyer", "seller", "sale_type", "occupancy_pct",
  "tenant", "leased_sf", "rent_psf_yr", "rent_psf_mo", "rent_per_acre_mo", "lease_structure", "term_months", "lease_start", "lease_end",
  "escalation_pct", "free_rent_months", "ti_psf", "lease_kind", "landlord", "asking_rent_psf_yr", "asking_price", "available_sf",
  "building_sf", "land_acres", "year_built", "clear_height_ft", "dock_doors", "drive_in_doors", "loading", "construction",
  "office_sf", "office_pct", "sprinkler", "rail_served", "yard_acres", "building_class", "property_subtype",
];
const NUM = new Set(["sale_price", "price_psf", "price_per_acre", "cap_rate_pct", "occupancy_pct", "leased_sf", "rent_psf_yr", "rent_psf_mo", "rent_per_acre_mo", "term_months", "escalation_pct", "free_rent_months", "ti_psf", "asking_rent_psf_yr", "asking_price", "available_sf", "building_sf", "land_acres", "year_built", "clear_height_ft", "dock_doors", "drive_in_doors", "office_sf", "office_pct", "yard_acres"]);
const DATES = new Set(["sale_date", "lease_start", "lease_end"]);
// Fields where loose text matching is fair (names written many ways).
const FUZZY = new Set(["property_name", "address", "buyer", "seller", "tenant", "landlord", "submarket", "city"]);
// Fields that matter most for comps; reported separately.
const CORE = ["record_kind", "address", "sale_price", "price_psf", "cap_rate_pct", "sale_date", "tenant", "leased_sf", "rent_psf_yr", "term_months", "lease_start", "building_sf", "land_acres", "year_built", "clear_height_ft"];

const norm = (s) => String(s ?? "").toLowerCase().replace(/\b(street|st|road|rd|drive|dr|avenue|ave|boulevard|blvd|parkway|pkwy|lane|ln|highway|hwy|freeway|fwy|suite|ste|building|bldg)\b\.?/g, (m) => m.slice(0, 2)).replace(/[^a-z0-9]/g, "");
function tokens(s) {
  return new Set(String(s ?? "").toLowerCase().replace(/[^a-z0-9 ]/g, " ").split(/\s+/).filter((w) => w.length > 1));
}
function textSim(a, b) {
  const na = norm(a), nb = norm(b);
  if (!na && !nb) return 1;
  if (!na || !nb) return 0;
  if (na === nb || na.includes(nb) || nb.includes(na)) return 1;
  const ta = tokens(a), tb = tokens(b);
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  return inter / Math.max(1, Math.min(ta.size, tb.size));
}
function numEq(a, b) {
  if (a == null || b == null) return false;
  const x = Number(a), y = Number(b);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
  return Math.abs(x - y) <= Math.max(0.011, Math.abs(y) * 0.01);
}
function dateEq(a, b) {
  if (!a || !b) return false;
  a = String(a); b = String(b);
  if (a === b) return true;
  // Accept extra precision on the extracted side ("2024-10-15" vs gold "2024-10").
  if (a.startsWith(b) || b.startsWith(a)) return true;
  // Quarter vs month in that quarter
  const q = (s) => { const m = /^(\d{4})-Q([1-4])$/.exec(s); return m ? [m[1], Number(m[2])] : null; };
  const mo = (s) => { const m = /^(\d{4})-(\d{2})/.exec(s); return m ? [m[1], Math.ceil(Number(m[2]) / 3)] : null; };
  const qa = q(a) || mo(a), qb = q(b) || mo(b);
  return !!(qa && qb && (q(a) || q(b)) && qa[0] === qb[0] && qa[1] === qb[1]);
}
function fieldEq(f, a, b) {
  if (NUM.has(f)) return numEq(a, b);
  if (DATES.has(f)) return dateEq(a, b);
  if (FUZZY.has(f)) return textSim(a, b) >= 0.6;
  if (typeof b === "boolean") return a === b;
  return String(a ?? "").toLowerCase() === String(b ?? "").toLowerCase();
}

function matchScore(g, e) {
  let s = 0;
  if (g.source_sheet && e.source_sheet && norm(g.source_sheet) === norm(e.source_sheet)) s += 1;
  const gr = new Set(g.source_rows || []);
  const overlap = (e.source_rows || []).filter((x) => gr.has(x)).length;
  if (overlap) s += 3 + overlap * 0.1;
  const addr = textSim(g.address || g.property_name, e.address || e.property_name);
  s += addr * 2;
  if (g.tenant && e.tenant) s += textSim(g.tenant, e.tenant);
  for (const f of ["sale_price", "leased_sf", "building_sf", "rent_psf_yr"]) if (g[f] != null && numEq(e[f], g[f])) s += 0.7;
  if (g.record_kind === e.record_kind) s += 0.3;
  return s;
}

const totals = { gold: 0, ext: 0, matched: 0, kind: 0, f: {}, extra: {} };
const perFile = [];
for (const gf of fs.readdirSync(goldDir).filter((f) => f.endsWith(".json"))) {
  const gold = JSON.parse(fs.readFileSync(path.join(goldDir, gf), "utf8")).records;
  const outPath = path.join(outDir, gf);
  if (!fs.existsSync(outPath)) continue;
  const ext = JSON.parse(fs.readFileSync(outPath, "utf8")).records;
  // greedy best-pair matching
  const pairs = [];
  gold.forEach((g, gi) => ext.forEach((e, ei) => pairs.push([matchScore(g, e), gi, ei])));
  pairs.sort((a, b) => b[0] - a[0]);
  const gUsed = new Map(), eUsed = new Set();
  for (const [s, gi, ei] of pairs) {
    if (s < 2.2 || gUsed.has(gi) || eUsed.has(ei)) continue;
    gUsed.set(gi, ei);
    eUsed.add(ei);
  }
  const fileStat = { file: gf.replace(/\.json$/, ""), gold: gold.length, ext: ext.length, matched: gUsed.size, fieldsOk: 0, fieldsTotal: 0, diffs: [] };
  gold.forEach((g, gi) => {
    if (!gUsed.has(gi)) {
      fileStat.diffs.push(`MISSING gold row ${g.source_sheet}:${(g.source_rows || []).join(",")} ${g.record_kind} ${g.address || g.property_name || ""} ${g.tenant || ""}`);
      return;
    }
    const e = ext[gUsed.get(gi)];
    for (const f of FIELDS) {
      const gv = g[f], ev = e[f];
      if (gv == null || gv === "") {
        if (ev != null && ev !== "" && f !== "record_kind") {
          totals.extra[f] = (totals.extra[f] || 0) + 1;
        }
        continue;
      }
      const ok = fieldEq(f, ev, gv);
      totals.f[f] = totals.f[f] || [0, 0];
      totals.f[f][1]++;
      fileStat.fieldsTotal++;
      if (ok) {
        totals.f[f][0]++;
        fileStat.fieldsOk++;
      } else fileStat.diffs.push(`  ${g.source_sheet}:${(g.source_rows || [])[0]} ${String(g.address || g.tenant || "").slice(0, 28)} | ${f}: gold=${JSON.stringify(gv)} got=${JSON.stringify(ev)}`);
    }
  });
  ext.forEach((e, ei) => {
    if (!eUsed.has(ei)) fileStat.diffs.push(`EXTRA extracted ${e.source_sheet}:${(e.source_rows || []).join(",")} ${e.record_kind} ${e.address || e.property_name || ""} ${e.tenant || ""}`);
  });
  totals.gold += gold.length;
  totals.ext += ext.length;
  totals.matched += gUsed.size;
  perFile.push(fileStat);
}

console.log("FILE".padEnd(58), "gold  ext  match  field-acc");
for (const p of perFile) console.log(p.file.slice(0, 57).padEnd(58), String(p.gold).padStart(4), String(p.ext).padStart(4), String(p.matched).padStart(6), (p.fieldsTotal ? ((100 * p.fieldsOk) / p.fieldsTotal).toFixed(1) + "%" : "-").padStart(9));
const allOk = Object.values(totals.f).reduce((a, [o]) => a + o, 0), allT = Object.values(totals.f).reduce((a, [, t]) => a + t, 0);
const coreOk = CORE.reduce((a, f) => a + (totals.f[f]?.[0] || 0), 0), coreT = CORE.reduce((a, f) => a + (totals.f[f]?.[1] || 0), 0);
console.log(`\nrecords: gold ${totals.gold}, extracted ${totals.ext}, matched ${totals.matched} -> recall ${((100 * totals.matched) / totals.gold).toFixed(1)}%, precision ${((100 * totals.matched) / Math.max(1, totals.ext)).toFixed(1)}%`);
console.log(`field accuracy (gold non-null fields on matched records): ${((100 * allOk) / allT).toFixed(1)}% (${allOk}/${allT}); core fields ${((100 * coreOk) / coreT).toFixed(1)}%`);
console.log("\nper field:", Object.entries(totals.f).sort((a, b) => a[1][0] / a[1][1] - b[1][0] / b[1][1]).map(([f, [o, t]]) => `${f} ${((100 * o) / t).toFixed(0)}% (${t})`).join(" | "));
console.log("\nfilled where gold is null:", JSON.stringify(totals.extra));
if (SHOW) for (const p of perFile) if (p.diffs.length) console.log(`\n== ${p.file}\n${p.diffs.join("\n")}`);
