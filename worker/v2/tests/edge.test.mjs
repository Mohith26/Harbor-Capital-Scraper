// Edge-case tests for address normalization, dedupe and bounds/unit repair.
// run: node worker/v2/tests/edge.test.mjs
import { parseAddress, addrSimilarity } from "../geo.js";
import { samePropertyScore, sameDealScore, dedupeBatch, tagPortfolios } from "../dedupe.js";
import { validateRecord, normalizeDate } from "../validate.js";
import { heuristicParse } from "../finder.js";

let pass = 0, fail = 0;
function t(name, cond, info = "") {
  if (cond) pass++;
  else { fail++; console.log("FAIL", name, info); }
}

// ---------- address normalization ----------
const same = [
  ["1400 Schlipf Rd", "1400 Schlipf Road, Katy, TX 77493"],
  ["13719 FM 529", "13719 Farm to Market 529"],
  ["13719 FM 529", "13719 F.M. Road 529"],
  ["16310 East Fwy", "16310 East Freeway"],
  ["950 N IH-45", "950 North Interstate 45"],
  ["950 N IH-45", "950 N I-45"],
  ["3000 N Sam Houston Pky. W.", "3000 North Sam Houston Parkway West"],
  ["3000 N Sam Houston Pkwy W", "3000 N Beltway 8 W"],
  ["4910 Wright Rd., Suite 160", "4910 Wright Road"],
  ["5851-5861 E Rosedale St", "5855 East Rosedale Street"],
  ["625 & 633 Westport Parkway", "633 Westport Pkwy"],
  ["2400 FM 156 and Mobility Way", "2400 FM-156"],
];
for (const [a, b] of same) t(`same addr: ${a} ~ ${b}`, addrSimilarity(a, b) >= 0.9, addrSimilarity(a, b).toFixed(2));
const diff = [
  ["1208 Viceroy Drive", "1262 Viceroy Drive"],
  ["9910 Cypress Creek Pky Bldg A", "9910 Cypress Creek Pky Bldg B"],
  ["100 N FM 3083 E - Bldg E", "100 N FM 3083 E - Bldg A"],
  ["12236 Cutten Rd", "12321 Cutten Rd"],
  ["5350 Bay Oaks", "5391 Bay Oaks"],
];
for (const [a, b] of diff) t(`different addr: ${a} vs ${b}`, addrSimilarity(a, b) < 0.75, addrSimilarity(a, b).toFixed(2));

t("building after comma kept", samePropertyScore({ address: "2831 South Houston Avenue, Building 4", latitude: 29.9, longitude: -95.3 }, { address: "2831 South Houston Avenue, Building 2", latitude: 29.9005, longitude: -95.3 }).score < 0.5);
t("suites are the same building", samePropertyScore({ address: "5829 W Sam Houston Pkwy N, Suite 910", latitude: 29.88, longitude: -95.55, geocode_quality: "rooftop" }, { address: "5829 W Sam Houston Pkwy N, Suite 1206", latitude: 29.8801, longitude: -95.55, geocode_quality: "rooftop" }).score >= 0.85);
// ---------- property matching with geo ----------
const p1 = { address: "8901 Forney Rd", city: "Dallas", latitude: 32.7746, longitude: -96.6672, geocode_quality: "rooftop" };
t("same bldg w/ geo", samePropertyScore(p1, { address: "8901 Forney Road", city: "Dallas", latitude: 32.7748, longitude: -96.667, geocode_quality: "rooftop" }).score >= 0.8);
t("same street number, other city", samePropertyScore(p1, { address: "8901 Forney Rd", city: "Mesquite", zip: "75149", latitude: 32.77, longitude: -96.55, geocode_quality: "rooftop" }).score < 0.75);
t("name-only match", samePropertyScore({ property_name: "Milo Distribution Center Bldg 3", latitude: 27.6, longitude: -99.5 }, { property_name: "Milo Distribution Center Bldg 3", latitude: 27.6001, longitude: -99.5001 }).score >= 0.75);
t("different building letters never merge", samePropertyScore({ address: "9910 Cypress Creek Pkwy Bldg A", latitude: 29.97, longitude: -95.53 }, { address: "9910 Cypress Creek Pkwy Bldg B", latitude: 29.97, longitude: -95.53 }).score < 0.5);

// ---------- deal matching ----------
const s1 = { record_kind: "sale", sale_price: 11340000, price_psf: 162, sale_date: "2024-Q4", buyer: "MDH Partners", building_sf: 70000 };
t("sale dup (quarter vs exact date)", sameDealScore(s1, { ...s1, sale_date: "2024-11-15" }).verdict === "duplicate");
t("sale dup different price -> conflict", sameDealScore(s1, { ...s1, sale_price: 12500000, price_psf: 178.6, sale_date: "2024-11-15" }).verdict === "conflict");
t("re-sale 3 yrs later is a new deal", sameDealScore(s1, { ...s1, sale_date: "2021-06-01", sale_price: 8000000, price_psf: 114 }).verdict === "different_deal");
const l1 = { record_kind: "lease", tenant: "Clair Global Corp.", leased_sf: 67575, lease_start: "2025-04-09", rent_psf_yr: 8.99, term_months: 86 };
t("lease dup (tenant suffix differs)", sameDealScore(l1, { ...l1, tenant: "Clair Global Corp", lease_start: "2025-04" }).verdict === "duplicate");
t("renewal 5 yrs later is a new deal", sameDealScore(l1, { ...l1, lease_start: "2030-06-01", rent_psf_yr: 10.5 }).verdict === "different_deal");
t("different tenant same SF is new deal", sameDealScore(l1, { ...l1, tenant: "Real Floors Inc" }).verdict === "different_deal");
t("lease dup with TI conflict", sameDealScore(l1, { ...l1, ti_psf: 19.22 }, {}).verdict !== "different_deal");
t("listing vs lease never duplicates", sameDealScore(l1, { ...l1, record_kind: "listing" }).verdict === "different_kind");
t("blank tenants need SF+rent", sameDealScore({ record_kind: "lease", leased_sf: 10000, lease_start: "2024-01" }, { record_kind: "lease", leased_sf: 10000, lease_start: "2024-02" }).verdict === "different_deal");

// ---------- batch dedupe & portfolios ----------
const batch = dedupeBatch([
  { record_kind: "lease", address: "755 Ridgecrest Rd", tenant: "Acme", leased_sf: 817538, lease_start: "2026-Q3", rent_psf_yr: 6.55 },
  { record_kind: "lease", address: "755 Ridgecrest Road", tenant: "Acme", leased_sf: 817538, lease_start: "2026-08", rent_psf_yr: 6.55 },
  { record_kind: "lease", address: "755 Ridgecrest Road", tenant: "Beta", leased_sf: 200000, lease_start: "2026-08", rent_psf_yr: 6.9 },
]);
t("batch dup flagged", batch[1]._batchDup && batch[1]._batchDup.of === 0 && !batch[2]._batchDup);
const port = tagPortfolios([
  { record_kind: "sale", address: "1905 Alpha Dr", sale_price: 30000000, sale_date: "2024-05", buyer: "X", _flags: [] },
  { record_kind: "sale", address: "1910 Alpha Dr", sale_price: 30000000, sale_date: "2024-05", buyer: "X", _flags: [] },
  { record_kind: "sale", address: "77 Other St", sale_price: 5000000, sale_date: "2024-05", buyer: "Y", _flags: [] },
]);
t("portfolio tagged", port[0].sale_type === "portfolio" && port[1].sale_type === "portfolio" && port[2].sale_type !== "portfolio");

// ---------- validation / unit repair ----------
let r = validateRecord({ record_kind: "lease", address: "1 Main St", rent_psf_yr: 0.65, leased_sf: 20000 });
t("monthly mislabeled as annual -> x12", Math.abs(r.rent_psf_yr - 7.8) < 0.01 && r.rent_psf_mo === 0.65, JSON.stringify(r.rent_psf_yr));
r = validateRecord({ record_kind: "lease", address: "1 Main St", rent_psf_mo: 8.75, rent_psf_yr: 105 });
t("annual mislabeled as monthly -> fixed", r.rent_psf_yr === 8.75 && r.rent_psf_mo == null, JSON.stringify([r.rent_psf_yr, r.rent_psf_mo]));
r = validateRecord({ record_kind: "lease", address: "1 Main St", rent_psf_yr: 13, lease_structure: "FS", property_subtype: "flex" });
t("flex FS rent kept", r.rent_psf_yr === 13 && r.lease_structure === "FSG");
r = validateRecord({ record_kind: "sale", address: "1 Main St", cap_rate_pct: 0.0625, sale_price: 1e7, building_sf: 50000, price_psf: 200 });
t("decimal cap -> pct", r.cap_rate_pct === 6.25);
r = validateRecord({ record_kind: "sale", address: "1 Main St", sale_price: 11340, building_sf: 70000, price_psf: 162 });
t("price in $K scaled", r.sale_price === 11340000, String(r.sale_price));
r = validateRecord({ record_kind: "sale", address: "1 Main St", sale_price: 683890, building_sf: 62099, price_psf: 110 });
t("price/SF inconsistency flagged as error", r._flags.some((f) => f.field === "price_psf" && f.severity === "error"));
r = validateRecord({ record_kind: "lease", address: "1 Main St", ti_psf: 186930, leased_sf: 33000, rent_psf_yr: 9 });
t("TI total -> psf", Math.abs(r.ti_psf - 5.66) < 0.01, String(r.ti_psf));
r = validateRecord({ record_kind: "lease", address: "1 Main St", term_months: 5, lease_start: "2024-01-01", lease_end: "2028-12-31", rent_psf_yr: 9 });
t("term years -> months", r.term_months === 60);
r = validateRecord({ record_kind: "lease", address: "1 Main St", clear_height_ft: 336, rent_psf_yr: 9 });
t("clear height inches -> ft", r.clear_height_ft === 28);
r = validateRecord({ record_kind: "sale", address: "1 Main St", land_acres: 174240, sale_price: 5e6, price_psf: 100, building_sf: 50000 });
t("land SF in acres col -> acres", r.land_acres === 4);
r = validateRecord({ record_kind: "lease", address: "1 Main St", leased_sf: 100000, building_sf: 66000, rent_psf_yr: 9 });
t("leased > building flagged", r._flags.some((f) => f.field === "leased_sf"));
r = validateRecord({ record_kind: "lease", address: "1 Main St", rent_psf_yr: 9, free_rent_months: 12, term_months: 12 });
t("free rent >= term error", r._flags.some((f) => f.field === "free_rent_months" && f.severity === "error"));
r = validateRecord({ record_kind: "sale", address: "Texas, USA", sale_price: 1e6, price_psf: 100, building_sf: 10000 });
t("no street number flagged", r._flags.some((f) => f.code === "partial"));
r = validateRecord({ record_kind: "sale", address: "1 Main St", sale_price: 1e6, building_sf: 10000, year_built: 1820 });
t("year built implausible", r._flags.some((f) => f.field === "year_built" && f.severity === "error"));
r = validateRecord({ record_kind: "listing", building_sf: 8000, asking_rent_psf_yr: 1.04, address: "21215 Blair Rd", asking_price: 0 });
t("listing: monthly asking -> annual, $0 price -> null, avail SF", Math.abs(r.asking_rent_psf_yr - 12.48) < 0.01 && r.asking_price == null && r.available_sf === 8000);
r = validateRecord({ record_kind: "lease", property_name: "10600 Corporate Dr", loading: "RL", sprinkler: "No", rent_psf_yr: 7 });
t("name->address, RL->rear, No->none", r.address === "10600 Corporate Dr" && r.loading === "rear" && r.sprinkler === "none");

// ---------- dates ----------
t("4Q24", normalizeDate("4Q24") === "2024-Q4");
t("Q2 2024", normalizeDate("Q2 2024") === "2024-Q2");
t("Dec-23", normalizeDate("Dec-23") === "2023-12");
t("1/1/25", normalizeDate("1/1/25") === "2025-01-01");
t("excel serial", normalizeDate(45658) === "2025-01-01", normalizeDate(45658));

// ---------- finder heuristic parse ----------
let q = heuristicParse("Trying to find 450,000 - 1,000,000 leases around 1085 Heritage West Dr");
t("range parse", q.size_sf.min === 450000 && q.size_sf.max === 1000000 && q.comp_types.includes("lease"), JSON.stringify(q.size_sf));
q = heuristicParse("45k SF rear-load tilt-wall, 28' clear, NW Houston - sale comps last 24 months");
t("shorthand parse", q.size_sf.target === 45000 && q.clear_height_ft.target === 28 && q.loading.includes("rear") && q.construction.includes("tilt_wall") && q.subject.location_text === "nw houston" && q.since, JSON.stringify(q));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
