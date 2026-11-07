import fs from "node:fs";
import { parseAddress, marketFor, haversineMi } from "../geo.js";
const S = process.argv[2];
const out = process.argv[3];
const rows = JSON.parse(fs.readFileSync(`${S}/costar_salecomps.json`, "utf8"));
const q = (v) => (v == null || v === "" ? "NULL" : typeof v === "number" ? (Number.isFinite(v) ? String(v) : "NULL") : `'${String(v).replace(/'/g, "''")}'`);
const ch = (s) => { const m = /(\d+)'(?:\s*(\d+)")?/.exec(s || ""); return m ? +(Number(m[1]) + (Number(m[2] || 0) / 12)).toFixed(1) : null; };
const cnt = (s) => { if (!s || s === "None") return s === "None" ? 0 : null; const m = /(\d+)/.exec(s); return m ? Number(m[1]) : null; };
const byProp = new Map();
for (const r of rows) {
  if (!r.propId) continue;
  const prev = byProp.get(r.propId);
  if (!prev || (r.saleDate || "") > (prev.saleDate || "")) byProp.set(r.propId, r);
}
const stmts = [];
let batch = [];
const cols = "prop_id,address,city,zip,county,market,submarket,latitude,longitude,addr_key,building_sf,land_acres,year_built,year_renovated,clear_height_ft,dock_doors,drive_in_doors,building_class,sec_type,stories,power,crane,rail,zoning,parcel,name";
for (const r of byProp.values()) {
  const lat = r.lat ?? null, lng = r.lon ?? null;
  const mk = lat != null ? marketFor(lat, lng) : null;
  const vals = [String(r.propId), r.address, r.city, r.zip, r.county, mk, r.submarket, lat, lng, parseAddress(r.address || "").key, r.sfNum, r.landAC, r.yearBuilt || null, r.yearReno || null, ch(r.ceilingHeight), cnt(r.docks), cnt(r.driveIns), /^[ABC]$/.test(r.class || "") ? r.class : null, r.secType || null, r.stories || null, r.power || null, r.crane || null, r.rail || null, r.zoning || null, r.parcel || null, r.name || null];
  batch.push(`(${vals.map(q).join(",")})`);
  if (batch.length === 40) { stmts.push(`INSERT OR REPLACE INTO v2_costar_props (${cols}) VALUES ${batch.join(",")};`); batch = []; }
}
if (batch.length) stmts.push(`INSERT OR REPLACE INTO v2_costar_props (${cols}) VALUES ${batch.join(",")};`);
// submarket gazetteer from all CoStar sale + lease points
const leases = JSON.parse(fs.readFileSync(`${S}/costar_leases.json`, "utf8"));
const pts = new Map();
for (const r of [...rows.map((x) => ({ s: x.submarket, lat: x.lat, lng: x.lon })), ...leases.map((x) => ({ s: x.submarket, lat: x.lat, lng: x.lon }))]) {
  if (!r.s || r.lat == null) continue;
  const mk = marketFor(r.lat, r.lng);
  const k = `${mk}|${r.s}`;
  if (!pts.has(k)) pts.set(k, []);
  pts.get(k).push([r.lat, r.lng]);
}
const med = (a) => { const v = [...a].sort((x, y) => x - y); return v[Math.floor(v.length / 2)]; };
const sub = [];
for (const [k, a] of pts) {
  if (a.length < 4) continue;
  const [mk, name] = k.split("|");
  const lat = med(a.map((p) => p[0])), lng = med(a.map((p) => p[1]));
  const d = a.map((p) => haversineMi(lat, lng, p[0], p[1])).sort((x, y) => x - y);
  const r = Math.max(2, Math.min(25, d[Math.floor(d.length * 0.8)]));
  sub.push(`(${[name, mk, +lat.toFixed(5), +lng.toFixed(5), +r.toFixed(2), a.length, "|" + name.toLowerCase() + "|"].map(q).join(",")})`);
}
for (let i = 0; i < sub.length; i += 40) stmts.push(`INSERT OR REPLACE INTO v2_submarkets (name, market, latitude, longitude, radius_mi, n, aliases) VALUES ${sub.slice(i, i + 40).join(",")};`);
fs.writeFileSync(out, stmts.join("\n"));
console.log("props", byProp.size, "submarkets", sub.length, "stmts", stmts.length, "bytes", fs.statSync(out).size);
