// Convert legacy sale_comps / lease_comps rows into v2 records for files we could NOT re-extract.
// usage: node legacy-records.mjs <d1dir> <seededNamesFile> <out.json>
import fs from "node:fs";
const [dir, seededFile, out] = process.argv.slice(2);
const S = JSON.parse(fs.readFileSync(`${dir}/sale_comps.json`, "utf8"))[0].results;
const L = JSON.parse(fs.readFileSync(`${dir}/lease_comps.json`, "utf8"))[0].results;
const canon = (n) => String(n || "").toLowerCase().replace(/^[0-9a-f]{8}_/, "").replace(/\s*\(\d+\)\s*/g, "").replace(/[_\s]+/g, " ").replace(/\.(xlsx|xls|csv|pdf)$/, "").trim();
const seeded = new Set(fs.readFileSync(seededFile, "utf8").split("\n").filter(Boolean).map((x) => canon(x.split("/").pop())));
const hasNum = (a) => /^\s*\d/.test(a || "");
function addr(r) {
  // legacy overwrote the broker address with the geocoder's formatted address; prefer the raw text
  let raw = null;
  try { const j = JSON.parse(r.raw_address_data || "null"); raw = j && (j.address || j.raw || j.original || null); } catch { raw = r.raw_address_data || null; }
  const a = raw || r.address;
  return String(a).replace(/,\s*(Texas|TX)\s*\d{0,5},?\s*USA$/i, "").replace(/,\s*USA$/i, "").trim();
}
const recs = [];
const skipped = { seeded: 0, no_street: 0 };
for (const r of S) {
  if (seeded.has(canon(r.source_file))) { skipped.seeded++; continue; }
  const a = addr(r);
  const good = hasNum(a);
  if (!good && !/\d/.test(r.address)) { skipped.no_street++; continue; }
  recs.push({ record_kind: "sale", _source_label: `${r.source_file} (legacy import #${r.id})`, source_sheet: "legacy", source_rows: [], address: a, city: r.city, zip: r.zip_code, state: "TX",
    latitude: good ? r.latitude : null, longitude: good ? r.longitude : null,
    sale_price: r.sale_price, building_sf: r.building_size, price_psf: r.price_per_sf || null, sale_date: r.closing_date ? String(r.closing_date).slice(0, 10) : null,
    year_built: r.year_built ? Math.round(r.year_built) : null, cap_rate_pct: r.cap_rate, buyer: r.buyer, seller: r.seller, notes: r.notes });
}
for (const r of L) {
  if (seeded.has(canon(r.source_file))) { skipped.seeded++; continue; }
  const a = addr(r);
  const good = hasNum(a);
  if (!good && !/\d/.test(r.address)) { skipped.no_street++; continue; }
  recs.push({ record_kind: "lease", _source_label: `${r.source_file} (legacy import #${r.id})`, source_sheet: "legacy", source_rows: [], address: a, city: r.city, zip: r.zip_code, state: "TX",
    latitude: good ? r.latitude : null, longitude: good ? r.longitude : null,
    tenant: r.tenant_name, leased_sf: r.leased_sf || null, rent_psf_yr: r.rate_annually || null, lease_structure: r.lease_type, term_months: r.term_months,
    lease_start: r.commencement_date ? String(r.commencement_date).slice(0, 10) : null, ti_psf: r.ti_allowance, escalation_pct: parseFloat(String(r.escalations || "").replace(/[^\d.]/g, "")) || null,
    free_rent_months: parseFloat(String(r.free_rent || "").replace(/[^\d.]/g, "")) || null, clear_height_ft: r.clear_height, notes: [r.building_type, r.notes].filter(Boolean).join("; ") || null });
}
fs.writeFileSync(out, JSON.stringify(recs));
const by = {}; recs.forEach((r) => (by[r._source_label.split(" (legacy")[0]] = (by[r._source_label.split(" (legacy")[0]] || 0) + 1));
console.log("records", recs.length, skipped, by);
