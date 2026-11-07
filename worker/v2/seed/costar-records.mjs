import fs from "node:fs";
const S = process.argv[2];
const sales = JSON.parse(fs.readFileSync(`${S}/costar_salecomps.json`, "utf8"));
const leases = JSON.parse(fs.readFileSync(`${S}/costar_leases.json`, "utf8"));
const ch = (s) => { const m = /(\d+)'(?:\s*(\d+)")?/.exec(s || ""); return m ? +(Number(m[1]) + Number(m[2] || 0) / 12).toFixed(1) : null; };
const cnt = (s) => { if (!s) return null; if (s === "None") return 0; const m = /(\d+)/.exec(s); return m ? Number(m[1]) : null; };
const SUB = { Warehouse: "warehouse", Distribution: "bulk_distribution", Manufacturing: "manufacturing", Service: "service", Showroom: "flex", "R&D": "flex", "Truck Terminal": "ios", "Refrigeration/Cold Storage": "cold_storage", "Food Processing": "manufacturing" };
const out = { sales: [], leases: [] };
for (const r of sales) {
  if (r.saleStatus !== "Sold" || !(r.salePrice > 0)) continue;
  out.sales.push({
    record_kind: (r.sfNum || 0) < 1000 && r.landAC ? "land_sale" : "sale",
    source_sheet: "CoStar sale comps", source_rows: [], _source_label: `CoStar sale comp ${r.compId || r.id}`,
    property_name: r.name || null, address: r.address, city: r.city, state: "TX", zip: r.zip, submarket: r.submarket, county: r.county,
    latitude: r.lat ?? null, longitude: r.lon ?? null,
    sale_price: r.salePrice, price_psf: r.salePrice && r.sfNum ? +(r.salePrice / r.sfNum).toFixed(2) : null, cap_rate_pct: r.capRate ? (r.capRate < 0.3 ? r.capRate * 100 : r.capRate) : null,
    sale_date: r.saleDate ? String(r.saleDate).slice(0, 10) : null, buyer: r.buyer || null, seller: r.seller || null,
    sale_type: /owner/i.test(r.saleType || "") && !/invest/i.test(r.saleType) ? "owner_user" : /invest/i.test(r.saleType || "") && !/owner/i.test(r.saleType) ? "investment" : null,
    occupancy_pct: r.pctLeased ? Number(r.pctLeased) : null,
    building_sf: r.sfNum || null, land_acres: r.landAC || null, year_built: r.yearBuilt || null, year_renovated: r.yearReno || null,
    clear_height_ft: ch(r.ceilingHeight), dock_doors: cnt(r.docks), drive_in_doors: cnt(r.driveIns), building_class: /^[ABC]$/.test(r.class || "") ? r.class : null,
    property_subtype: SUB[r.secType] || null, notes: r.priceComment || null,
  });
}
for (const r of leases) {
  const m = /\$?([\d.]+)\s*(?:\/(mo|yr))?\s*([A-Z]+)?/i.exec(r.rent || "");
  let rent = m ? Number(m[1]) : null;
  let structure = m && m[3] ? ({ NNN: "NNN", FS: "FSG", MG: "MG", IG: "IG", TBD: null, NN: "NN", GROSS: "Gross" }[m[3].toUpperCase()] ?? null) : null;
  const monthly = m && /mo/i.test(m[2] || "");
  const dt = r.commencement || r.signed || null;
  const mon = { Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06", Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12" };
  const dm = /^([A-Z][a-z]{2}) (\d{4})$/.exec(dt || "");
  out.leases.push({
    record_kind: "lease", source_sheet: "CoStar lease comps", source_rows: [], _source_label: `CoStar lease comp ${r.id}`,
    property_name: r.building || null, address: r.address, city: r.city, state: "TX", zip: r.zip, submarket: r.submarket,
    latitude: r.lat ?? null, longitude: r.lon ?? null,
    tenant: r.tenant || null, leased_sf: r.sf || null, rent_psf_yr: rent && !monthly ? rent : null, rent_psf_mo: rent && monthly ? rent : null,
    lease_structure: structure, lease_start: dm ? `${dm[2]}-${mon[dm[1]]}` : null, lease_end: r.expISO || null, landlord: r.landlord || null,
    property_subtype: r.propType === "Flex" ? "flex" : null, notes: [r.rentType, r.industry && `industry: ${r.industry}`, r.tenantRep && `tenant rep: ${r.tenantRep}`, r.landlordRep && `LL rep: ${r.landlordRep}`].filter(Boolean).join("; ") || null,
  });
}
fs.writeFileSync(`${S}/costar_records.json`, JSON.stringify(out));
console.log("sales", out.sales.length, "leases", out.leases.length, "lease rent parsed", out.leases.filter((x) => x.rent_psf_yr || x.rent_psf_mo).length);
console.log(JSON.stringify(out.leases[0]), JSON.stringify(out.sales[0]).slice(0, 600));
