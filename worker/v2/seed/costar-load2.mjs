// Load priced CoStar sale comps and leases with a disclosed rent into harbor-comps
// through the live ingest endpoint (validation, building match, dedupe/merge).
// Unpriced sales, rent-less leases and listings are left out.
//
//   node worker/v2/seed/costar-load.mjs <dir with comps_v2.jsonl + properties.jsonl> [--test] [--kinds sale,lease] [--conc 4]
//
// Resumable: progress (done batch indices per kind, file_id, tallies) is kept in
// <dir>/costar-load-progress.json.
import fs from "node:fs";

const DIR = process.argv[2];
const TEST = process.argv.includes("--test");
const KINDS = (process.argv.find((a) => a.startsWith("--kinds=")) || "--kinds=sale,lease").slice(8).split(",");
const CONC = Number((process.argv.find((a) => a.startsWith("--conc=")) || "--conc=4").slice(7));
const BATCH = 40;
const URL_ = "https://comps.harborcap.dev/api/v2/admin/ingest";
const PROG = process.argv.find((a) => a.startsWith("--have=")) ? `${DIR}/costar-reconcile-progress.json` : `${DIR}/costar-load-progress.json`;
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json", "x-harbor-user": "costar-load-2" };

const num = (v) => {
  if (v == null || v === "" || v === "None") return null;
  const n = Number(String(v).replace(/[$,]/g, ""));
  return Number.isFinite(n) ? n : null;
};
const str = (v) => (v == null || v === "" || v === "None" || v === "[]" ? null : String(v).trim());
const SUB = { Warehouse: "warehouse", Distribution: "bulk_distribution", Manufacturing: "manufacturing", Service: "service", Showroom: "flex", "R&D": "flex", "Truck Terminal": "ios", "Refrigeration/Cold Storage": "cold_storage", "Food Processing": "manufacturing" };
const CONSTR = { Metal: "metal", Masonry: "masonry", Steel: "steel", "Reinforced Concrete": null, Wood: null };
const LS = { NNN: "NNN", "Triple Net": "NNN", MG: "MG", "Modified Gross": "MG", IG: "IG", "Industrial Gross": "IG", FSG: "FSG", "Full Service Gross": "FSG", "Double Net": "NN", NN: "NN", "Plus Electric": "MG", "Plus Utilities": "MG", "Plus Electric and Cleaning": "MG", "Plus All Utilities": "MG" };

function feats(c, p) {
  const pick = (k, pk = k) => num(c[k]) ?? num(p?.[pk]);
  const power = str(c.power) ?? str(p?.power);
  const rail = c.rail_served ?? p?.rail;
  const sub = str(c.property_subtype) ?? str(p?.property_subtype);
  const constr = str(c.construction) ?? str(p?.construction);
  return {
    building_sf: pick("building_sf"), land_acres: pick("land_acres"), year_built: pick("year_built"), clear_height_ft: pick("clear_height_ft"),
    dock_doors: pick("dock_doors"), drive_in_doors: pick("drive_in_doors"), office_sf: pick("office_sf"),
    construction: constr && constr in CONSTR ? CONSTR[constr] : constr ? constr.toLowerCase() : null,
    building_class: /^[ABC]$/.test(str(c.building_class) ?? str(p?.building_class) ?? "") ? str(c.building_class) ?? str(p?.building_class) : null,
    sprinkler: str(c.sprinkler) ?? str(p?.sprinkler),
    power: power && !/^no$/i.test(power) ? power : null,
    cranes: str(p?.cranes),
    rail_served: rail === true || /^(y|yes|true)$/i.test(String(rail ?? "")) ? true : null,
    property_subtype: sub ? SUB[sub] ?? null : null,
    tenancy: str(c.tenancy) ?? str(p?.tenancy),
  };
}

function loc(c, p) {
  const lat = num(c.lat) ?? num(p?.lat);
  const lng = num(c.lng) ?? num(p?.lng);
  return { latitude: lat, longitude: lng };
}

const PORT = /\s*\(Part of an? (\d+) Property (?:& Condo )?(?:Portfolio|Sale)\)\s*/i;
const cleanAddr = (a) => (a ? a.replace(PORT, " ").replace(/\s*\(Lot \d+\)\s*/i, " ").replace(/\s+/g, " ").trim() || null : null);

function base(c, p) {
  return {
    property_name: str(c.property_name) ?? str(p?.name), address: cleanAddr(str(c.address) ?? str(p?.address)), city: str(c.city) ?? str(p?.city), state: "TX",
    zip: str(c.zip) ?? str(p?.zip), submarket: str(c.submarket) ?? str(p?.submarket), county: str(c.county) ?? str(p?.county),
    ...loc(c, p), ...feats(c, p), source_rows: [],
  };
}

const BOILER = /(^|;\s*)(price (not )?disclosed|research status [^;]*|source [^;]*)(?=;|$)/gi;
function shortNotes(parts) {
  const s = parts.filter(Boolean).map((x) => x.replace(BOILER, "").replace(/^[;\s]+|[;\s]+$/g, "")).filter(Boolean).join("; ");
  return s ? s.slice(0, 280) : null;
}

export function mapSale(c, p) {
  const price = num(c.sale_price);
  const r = base(c, p);
  const psf = num(c.price_psf) ?? (price && r.building_sf ? +(price / r.building_sf).toFixed(2) : null);
  let cap = num(c.cap_rate_pct);
  if (cap != null && cap < 0.3) cap = +(cap * 100).toFixed(3);
  const st = str(c.sale_type);
  const pm = PORT.exec(str(c.address) || "");
  const nProps = pm ? Number(pm[1]) : 1;
  const baseType = st === "Investment" ? "investment" : st === "Owner User" ? "owner_user" : null;
  return {
    ...r,
    record_kind: (r.building_sf || 0) < 1000 && r.land_acres ? "land_sale" : "sale",
    source_sheet: "CoStar sale comps", _source_label: `CoStar sale comp ${c.costar_sale_comp_id}`,
    sale_price: price, price_psf: psf, cap_rate_pct: cap, sale_date: str(c.sale_date)?.slice(0, 10) ?? null,
    buyer: str(c.buyer), seller: str(c.seller), sale_type: nProps > 1 ? "portfolio" : baseType,
    occupancy_pct: num(c.occupancy_pct),
    notes: shortNotes([nProps > 1 ? `part of a ${nProps}-property sale, price is CoStar's allocation${st ? ` (${st.toLowerCase()})` : ""}` : null, str(c.notes)?.replace(/^CoStar sale comp;?\s*/i, ""), `CoStar property ${c.costar_property_id}`]),
  };
}

export function mapLease(c, p) {
  const r = base(c, p);
  const fr = /([\d.]+)\s*Month/i.exec(c.free_rent_raw || "");
  const esc = /([\d.]+)\s*%/.exec(c.escalations_raw || "");
  const signed = str(c.signed_date)?.slice(0, 10) ?? null;
  const start = str(c.lease_start)?.slice(0, 10) ?? null;
  const lsRaw = str(c.lease_structure);
  return {
    ...r,
    record_kind: "lease",
    source_sheet: "CoStar lease comps", _source_label: `CoStar lease comp ${c.costar_lease_comp_key}`,
    tenant: str(c.tenant), tenant_industry: str(c.tenant_industry), leased_sf: num(c.leased_sf),
    rent_psf_yr: num(c.rent_psf_yr), net_effective_rent_psf_yr: num(c.effective_rent_psf_yr),
    lease_structure: lsRaw ? LS[lsRaw] ?? lsRaw : null,
    term_months: num(c.term_months), lease_kind: str(c.lease_kind), ti_psf: num(c.ti_psf),
    free_rent_months: fr ? Number(fr[1]) : null, escalation_pct: esc ? Number(esc[1]) : null,
    lease_start: signed || start, lease_end: str(c.lease_end)?.slice(0, 10) ?? null,
    notes: shortNotes([
      signed && start && start !== signed ? `commences ${start}` : null,
      c.escalations_raw && !esc ? `escalations ${c.escalations_raw}` : null,
      lsRaw && LS[lsRaw] && LS[lsRaw] !== lsRaw ? `CoStar service type: ${lsRaw}` : null,
      str(c.notes)?.replace(/^CoStar lease comp;?\s*/i, ""),
    ]),
  };
}

function loadRecords() {
  const props = new Map();
  for (const l of fs.readFileSync(`${DIR}/properties.jsonl`, "utf8").split("\n")) {
    if (!l) continue;
    const r = JSON.parse(l);
    props.set(String(r.costar_id), r);
  }
  const out = { sale: [], lease: [] };
  for (const l of fs.readFileSync(`${DIR}/comps_v2.jsonl`, "utf8").split("\n")) {
    if (!l) continue;
    const c = JSON.parse(l);
    const p = props.get(String(c.costar_property_id));
    if (c.record_kind === "sale" && num(c.sale_price) > 0) out.sale.push({ pid: String(c.costar_property_id), rec: mapSale(c, p) });
    else if (c.record_kind === "lease" && num(c.rent_psf_yr) > 0) out.lease.push({ pid: String(c.costar_property_id), rec: mapLease(c, p) });
  }
  return out;
}

// Batches of ~BATCH that never split a building across batches (concurrent
// batches must not race to create the same property).
function batches(list) {
  list.sort((a, b) => a.pid.localeCompare(b.pid));
  const out = [];
  let cur = [];
  for (let i = 0; i < list.length; i++) {
    cur.push(list[i].rec);
    const next = list[i + 1];
    if (cur.length >= BATCH && (!next || next.pid !== list[i].pid)) { out.push(cur); cur = []; }
  }
  if (cur.length) out.push(cur);
  return out;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function readProg() {
  try { return JSON.parse(fs.readFileSync(PROG, "utf8")); } catch { return { file_id: null, done: { sale: [], lease: [] }, tally: {}, errors: [] }; }
}
let prog = readProg();
const save = () => fs.writeFileSync(PROG, JSON.stringify(prog));

async function post(records) {
  for (let attempt = 0; attempt < 7; attempt++) {
    try {
      const r = await fetch(URL_, { method: "POST", headers: H, body: JSON.stringify({ source_kind: "costar", source_label: "CoStar TX Industrial Archive (Nov 2026)", file_id: prog.file_id, records }) });
      const t = await r.text();
      if (r.status >= 500 || r.status === 429) throw new Error(`HTTP ${r.status} ${t.slice(0, 160)}`);
      const j = JSON.parse(t);
      if (j.error) throw new Error(j.error);
      return j;
    } catch (e) {
      const wait = Math.min(60000, 2000 * 2 ** attempt);
      console.log(`  retry ${attempt + 1} in ${wait / 1000}s: ${String(e.message).slice(0, 160)}`);
      await sleep(wait);
    }
  }
  throw new Error("gave up after retries");
}

function addTally(kind, j) {
  const t = (prog.tally[kind] = prog.tally[kind] || { added: 0, merged: 0, conflicts: 0, skipped: 0, failed: 0 });
  for (const k of Object.keys(t)) t[k] += j[k] || 0;
  if (j.errors?.length) prog.errors.push(...j.errors.slice(0, 3).map((e) => `${kind}: ${e}`));
  prog.errors = prog.errors.slice(-40);
}

const data = loadRecords();
// Reconcile mode: only records whose label is not in the database yet (re-runs rows lost to D1 overload).
const HAVE = process.argv.find((a) => a.startsWith("--have="));
if (HAVE) {
  const have = new Set(JSON.parse(fs.readFileSync(HAVE.slice(7), "utf8")));
  for (const k of Object.keys(data)) data[k] = data[k].filter((r) => !have.has(r._source_label));
}
console.log(`records: sale ${data.sale.length}, lease ${data.lease.length}`);
for (const kind of KINDS) {
  const bs = batches(data[kind]);
  const done = new Set(prog.done[kind] || []);
  const todo = bs.map((b, i) => i).filter((i) => !done.has(i));
  console.log(`${kind}: ${bs.length} batches, ${todo.length} to do`);
  if (TEST) {
    const i = todo[0];
    const j = await post(bs[i].slice(0, BATCH));
    if (!prog.file_id) prog.file_id = j.file_id;
    addTally(kind, j);
    prog.done[kind] = [...done, i];
    save();
    console.log("test batch", i, JSON.stringify(j));
    continue;
  }
  // first call alone if no file id yet (to create the file row once)
  if (!prog.file_id && todo.length) {
    const i = todo.shift();
    const j = await post(bs[i]);
    prog.file_id = j.file_id;
    addTally(kind, j);
    done.add(i);
    prog.done[kind] = [...done];
    save();
  }
  let next = 0;
  const t0 = Date.now();
  let n = 0;
  async function worker() {
    while (next < todo.length) {
      const i = todo[next++];
      try {
        const j = await post(bs[i]);
        addTally(kind, j);
        done.add(i);
        prog.done[kind] = [...done];
        save();
        n++;
        if (n % 10 === 0) console.log(`${kind} ${done.size}/${bs.length} batches · ${((Date.now() - t0) / 60000).toFixed(1)} min · ${JSON.stringify(prog.tally[kind])}`);
      } catch (e) {
        console.log(`batch ${kind} ${i} failed: ${e.message}`);
        prog.errors.push(`${kind} batch ${i}: ${e.message}`);
        save();
      }
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  console.log(`${kind} finished: ${done.size}/${bs.length} batches · ${JSON.stringify(prog.tally[kind])}`);
}
console.log("DONE", JSON.stringify(prog.tally));
