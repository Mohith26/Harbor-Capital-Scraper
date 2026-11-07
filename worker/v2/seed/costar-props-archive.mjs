// Upsert the CoStar TX Industrial Archive properties (26.9k) into v2_costar_props.
// Existing values are kept where the archive has none (COALESCE upsert).
import fs from "node:fs";
import { parseAddress, marketFor } from "../geo.js";
const [src, outDir] = process.argv.slice(2);
const q = (v) => (v == null || v === "" ? "NULL" : typeof v === "number" ? (Number.isFinite(v) ? String(v) : "NULL") : `'${String(v).replace(/'/g, "''")}'`);
const cols = ["prop_id", "address", "city", "zip", "county", "market", "submarket", "latitude", "longitude", "addr_key", "building_sf", "land_acres", "year_built", "year_renovated", "clear_height_ft", "dock_doors", "drive_in_doors", "building_class", "sec_type", "stories", "power", "crane", "rail", "zoning", "name"];
const rows = [];
for (const l of fs.readFileSync(src, "utf8").split("\n")) {
  if (!l.trim()) continue;
  const r = JSON.parse(l);
  if (r.lat == null || !r.costar_id) continue;
  const yr = Number(String(r.year_built_renovated || "").match(/(?:19|20)\d{2}$/)?.[0]) || null;
  const pw = r.power == null || /^\s*none\s*$/i.test(r.power) ? null : String(r.power).replace(/\s+/g, " ").trim();
  const rail = r.rail == null || /^(none|unknown)$/i.test(r.rail) ? null : "Yes";
  const crane = r.cranes === "Yes" ? "Yes" : null;
  const ch = r.clear_height_ft != null && r.clear_height_ft >= 8 && r.clear_height_ft <= 60 ? r.clear_height_ft : null;
  rows.push([String(r.costar_id), r.address, r.city, r.zip, r.county, marketFor(r.lat, r.lng), r.submarket, r.lat, r.lng, parseAddress(r.address || "").key || null, r.building_sf || null, r.land_acres || null, r.year_built || null, yr && yr !== r.year_built ? yr : null, ch, r.dock_doors ?? null, r.drive_in_doors ?? null, /^[ABC]$/.test(r.building_class || "") ? r.building_class : null, r.property_subtype && r.property_subtype !== "None" ? r.property_subtype : null, r.stories ?? null, pw, crane, rail, r.zoning || null, r.name || null]);
}
const upd = cols.slice(1).map((c) => `${c} = COALESCE(excluded.${c}, ${c})`).join(", ");
fs.mkdirSync(outDir, { recursive: true });
let file = 0, buf = [], size = 0;
const flush = () => { if (!buf.length) return; fs.writeFileSync(`${outDir}/costar_archive_${String(file++).padStart(2, "0")}.sql`, buf.join("\n")); buf = []; size = 0; };
for (let i = 0; i < rows.length; i += 40) {
  const s = `INSERT INTO v2_costar_props (${cols.join(",")}) VALUES ${rows.slice(i, i + 40).map((v) => `(${v.map(q).join(",")})`).join(",")} ON CONFLICT(prop_id) DO UPDATE SET ${upd};`;
  if (size + s.length > 900000) flush();
  buf.push(s); size += s.length;
}
flush();
console.log("rows", rows.length, "files", file);
