# Harbor comp record schema (v2)

One JSON object per comp (one transaction or one listing, for one building).
Units are fixed. Use `null` when the document does not state a value. Never
guess or compute a value that is not in the document, except the explicitly
"derived" cases noted below.

```jsonc
{
  "record_kind": "sale" | "lease" | "listing" | "land_sale" | "subject",
  //  sale       = closed (or under-contract) building sale
  //  lease      = signed lease (new, renewal, expansion, sublease)
  //  listing    = availability / for-lease or for-sale listing / competitive-set space (no signed deal)
  //  land_sale  = sale of land / IOS yard with no meaningful building
  //  subject    = the subject property of the comp sheet (the deal being underwritten), not a comp
  "source_sheet": "Sheet name or 'page N' for PDFs",
  "source_rows": [12],                 // spreadsheet row number(s) (1-based, as shown) or PDF line numbers the record came from

  // ---- location ----
  "property_name": null,               // building / park / project name if given ("Northport Bldg 2", "Milo Distribution Center")
  "address": "14002 Mercury Dr",       // street address only, as written (multiple numbers ok: "5851-5861 E Rosedale St"); null if none
  "city": "Laredo",
  "state": "TX",
  "zip": "78045",
  "submarket": "Laredo",               // as written in the doc (e.g. "Lower GSW", "North Fwy/Tomball Pky")
  "county": null,

  // ---- sale fields ----
  "sale_price": 11340000,              // total $ (convert $11.3M / $11,340K to dollars)
  "price_psf": 162,                    // $/SF of building as stated
  "price_per_acre": null,              // $/acre (land / IOS)
  "cap_rate_pct": 6.25,                // percent number, 6.25 means 6.25%
  "sale_date": "2024-10-15",           // ISO; "2024-10" if only month; "2024-Q4" if only quarter; "2024" if only year
  "buyer": "MDH Partners",
  "seller": "XDOCK Enterprises, LLC",
  "sale_type": null,                   // "investment" | "owner_user" | "sale_leaseback" | "portfolio" | "land" | "development" | null
  "occupancy_pct": null,               // % leased at sale, 100 = fully leased
  "noi": null,

  // ---- lease fields ----
  "tenant": null,
  "leased_sf": null,
  "rent_psf_yr": null,                 // ANNUAL starting rent $/SF/yr. If doc gives monthly ($0.65/SF/mo) put 7.80 here AND 0.65 in rent_psf_mo
  "rent_psf_mo": null,                 // monthly $/SF/mo only if the doc states monthly
  "rent_per_acre_mo": null,            // IOS / land rent $/acre/month if stated that way
  "monthly_rent_total": null,          // total $ per month if stated
  "lease_structure": null,             // "NNN" | "MG" | "Gross" | "IG" | "FSG" | "NN" | null
  "term_months": null,
  "lease_start": null,                 // commencement / start / transaction date, ISO as above
  "lease_end": null,
  "escalation_pct": null,              // annual bump in percent (4.00% -> 4)
  "free_rent_months": null,
  "ti_psf": null,                      // TI allowance $/SF
  "lease_kind": null,                  // "new" | "renewal" | "expansion" | "extension" | "sublease" | null
  "landlord": null,

  // ---- listing fields ----
  "asking_rent_psf_yr": null,
  "asking_price": null,
  "available_sf": null,

  // ---- building features (only what the doc states) ----
  "building_sf": 70000,                // total building RSF (for leases this is the BUILDING size, not the leased space; null if not given)
  "land_acres": 6.5,                   // convert land SF to acres (/43,560) only if the doc gives land SF
  "year_built": 2022,
  "year_renovated": null,
  "clear_height_ft": null,             // 28' -> 28 ; "24'-28'" -> 28 (max) and note the range in notes
  "dock_doors": null,                  // dock-high doors / docks count
  "drive_in_doors": null,              // grade-level / drive-in / ramps count
  "loading": null,                     // "rear" | "front" | "cross_dock" | "side" | "rail" | null
  "construction": null,                // "tilt_wall" | "metal" | "masonry" | "concrete_block" | "precast" | "steel" | null
  "office_sf": null,
  "office_pct": null,                  // percent number
  "sprinkler": null,                   // "ESFR" | "wet" | "dry" | "none" | null
  "rail_served": null,                 // true/false/null
  "yard_acres": null,                  // outdoor storage / IOS yard acres
  "building_class": null,              // "A" | "B" | "C"
  "property_subtype": null,            // "bulk_distribution" | "shallow_bay" | "flex" | "manufacturing" | "ios" | "cold_storage" | "multi_tenant" | "service" | "warehouse" | "data_center" | null
  "tenancy": null,                     // "single" | "multi"
  "car_parking": null,
  "trailer_parking": null,
  "power": null,                       // free text, e.g. "2,000A 480V 3-phase"
  "notes": null                        // anything else useful (comments column, special terms)
}
```

Rules
- One record per building per transaction. A portfolio sale listing 3 buildings
  with one total price -> 3 records, each `sale_type: "portfolio"`, the total
  price on each, and a note "portfolio of 3; price is total".
- Totals / averages / median rows are not records.
- Section-header rows ("Metal Buildings", "Class B") are context, not records.
- A sheet titled "Lease Comps" can still contain sales (and vice versa): decide
  from the columns and values.
- Percent: always the percent number (6.25), never 0.0625.
- Dates: ISO. Excel serials and "4Q24" -> "2024-Q4"; "Jan-24" -> "2024-01".
- Do not invent a city from a submarket unless the doc makes the city clear
  (e.g. the sheet title says "Laredo").
