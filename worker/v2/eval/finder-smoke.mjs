import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
const Q = [
  "Lease comps 450,000 - 1,000,000 SF around 1085 Heritage West Dr, Katy",
  "45k SF rear-load tilt-wall, 28' clear, NW Houston off 290 - sale comps last 24 months",
  "crane-served manufacturing sale comps in Houston with cap rates",
  "IOS lease comps in DFW, 3-10 acres",
  "shallow-bay multi-tenant lease comps in Upper GSW since 2023",
  "Laredo warehouse sale comps 50-120k SF since 2024",
  "Tri County San Antonio 20-60k SF front-load lease comps",
  "6508 Tri County Pkwy (Building 5): 36,800 SF on 2.76 acres, 24' clear, ESFR, heavy power, dock-high + grade-level loading, vacant. Pricing guidance $140 PSF",
  "517,500 SF, 30' clear, Class B cross-dock, built 2001 - 8901 Forney Rd Dallas, need sale comps",
  "comps for 13026 Rosecrest Drive Houston - 52,000 SF metal warehouse on 7.77 acres with laydown yard, crane-ready",
  "Brookhollow lease comps",
  "user sale comps 70-90k SF DFW",
];
for (const q of Q) {
  const t0 = Date.now();
  const r = await (await fetch("https://comps.harborcap.dev/api/v2/finder", { method: "POST", headers: H, body: JSON.stringify({ query: q, limit: 40 }) })).json();
  if (r.error) { console.log("ERR", q, r.error); continue; }
  const p = r.parsed;
  console.log(`\n# ${q}\n  parsed(${p._parser}): types=${p.comp_types} loc=${r.location ? r.location.label + " r" + r.location.radius : "none"} size=${JSON.stringify(p.size_sf)} sub=${p.property_subtype} since=${p.since || ""} | subject=${r.subject ? JSON.stringify({ sf: r.subject.building_sf, yb: r.subject.year_built, ch: r.subject.clear_height_ft, auto: r.subject.auto_filled }) : "-"}`);
  console.log(`  ${r.matched} matches (+${r.near_misses} near) of ${r.candidates}; ${((Date.now() - t0) / 1000).toFixed(1)}s; median psf ${r.stats.median_price_psf?.toFixed?.(0)} cap ${r.stats.median_cap_rate} rent ${r.stats.median_rent_psf_yr?.toFixed?.(2)}`);
  for (const x of r.results.slice(0, 4)) console.log(`   ${x.score} ${x.record_kind} ${x.txn_date} ${x.address || x.property_name} (${x.city || ""}) ${x.distance_mi ?? ""}mi sf=${x.size_sf} ${x.price_psf ? "$" + x.price_psf + "/sf" : ""}${x.rent_psf_yr ? "$" + x.rent_psf_yr + "/yr" : ""} ${x.property_subtype || ""} ${x.relaxed ? "NEAR" : ""}`);
}
