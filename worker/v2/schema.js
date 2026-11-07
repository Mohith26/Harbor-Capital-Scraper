// Canonical comp record schema shared by the extractor, validator, DB layer
// and the gold-set evaluator. See SCHEMA.md for the human-readable version.

export const RECORD_KEYS = [
  "record_kind", "source_sheet", "source_rows",
  "property_name", "address", "city", "state", "zip", "submarket", "county", "latitude", "longitude",
  "sale_price", "price_psf", "price_per_acre", "cap_rate_pct", "sale_date", "buyer", "seller", "sale_type", "occupancy_pct", "noi",
  "tenant", "leased_sf", "rent_psf_yr", "rent_psf_mo", "rent_per_acre_mo", "monthly_rent_total", "lease_structure", "term_months",
  "lease_start", "lease_end", "escalation_pct", "free_rent_months", "ti_psf", "lease_kind", "landlord",
  "asking_rent_psf_yr", "asking_price", "available_sf",
  "building_sf", "land_acres", "year_built", "year_renovated", "clear_height_ft", "dock_doors", "drive_in_doors", "loading",
  "construction", "office_sf", "office_pct", "sprinkler", "rail_served", "yard_acres", "building_class", "property_subtype",
  "tenancy", "car_parking", "trailer_parking", "power", "notes",
];

export const NUMERIC_FIELDS = [
  "latitude", "longitude", "sale_price", "price_psf", "price_per_acre", "cap_rate_pct", "occupancy_pct", "noi",
  "leased_sf", "rent_psf_yr", "rent_psf_mo", "rent_per_acre_mo", "monthly_rent_total", "term_months", "escalation_pct",
  "free_rent_months", "ti_psf", "asking_rent_psf_yr", "asking_price", "available_sf",
  "building_sf", "land_acres", "year_built", "year_renovated", "clear_height_ft", "dock_doors", "drive_in_doors",
  "office_sf", "office_pct", "yard_acres", "car_parking", "trailer_parking",
];

export const DATE_FIELDS = ["sale_date", "lease_start", "lease_end"];

// Building features that describe the physical property (not a transaction).
export const FEATURE_FIELDS = [
  "building_sf", "land_acres", "year_built", "year_renovated", "clear_height_ft", "dock_doors", "drive_in_doors", "loading",
  "construction", "office_sf", "office_pct", "sprinkler", "rail_served", "yard_acres", "building_class", "property_subtype",
  "tenancy", "car_parking", "trailer_parking", "power",
];

export const ENUMS = {
  record_kind: ["sale", "lease", "listing", "land_sale", "subject"],
  sale_type: ["investment", "owner_user", "sale_leaseback", "portfolio", "land", "development"],
  lease_structure: ["NNN", "MG", "Gross", "IG", "FSG", "NN"],
  lease_kind: ["new", "renewal", "expansion", "extension", "sublease"],
  loading: ["rear", "front", "cross_dock", "side", "rail"],
  construction: ["tilt_wall", "metal", "masonry", "concrete_block", "precast", "steel"],
  sprinkler: ["ESFR", "wet", "dry", "none"],
  building_class: ["A", "B", "C"],
  property_subtype: ["bulk_distribution", "shallow_bay", "flex", "manufacturing", "ios", "cold_storage", "multi_tenant", "service", "warehouse", "data_center"],
  tenancy: ["single", "multi"],
};

export const SCHEMA_PROMPT = `RECORD SCHEMA (units are fixed):
record_kind: "sale" (closed / under-contract building sale) | "lease" (signed lease: new, renewal, expansion, sublease) | "listing" (availability, for-lease/for-sale, competitive-set space, no signed deal) | "land_sale" (land or IOS yard with no meaningful building) | "subject" (the subject property being underwritten, not a comp)
source_sheet, source_rows: sheet name and the row numbers (PDF: line numbers) the record came from
LOCATION: property_name (building/park/project name), address (street address ONLY, as written, e.g. "5851-5861 E Rosedale St"; null if the doc has none - never put a building name here), city, state ("TX"), zip, submarket (as written), county, latitude/longitude (only if the doc gives coordinates, e.g. lat/lon columns or map-link coords)
SALE: sale_price (total dollars; "$11.3M" -> 11300000), price_psf ($/SF of building), price_per_acre, cap_rate_pct (percent number: 6.25 means 6.25%; a raw 0.0625 -> 6.25), sale_date, buyer, seller, sale_type (investment|owner_user|sale_leaseback|portfolio|land|development), occupancy_pct (100 = fully leased), noi
LEASE: tenant, leased_sf (the leased space), rent_psf_yr (ANNUAL starting rent $/SF/yr), rent_psf_mo (only if the doc states a monthly $/SF rate; then ALSO set rent_psf_yr = 12 x monthly), rent_per_acre_mo (IOS $/acre/month), monthly_rent_total, lease_structure (NNN|MG|Gross|IG|FSG|NN), term_months (years x 12), lease_start (commencement / transaction date), lease_end, escalation_pct (annual bump percent: 4.00% -> 4; $0.15/SF fixed bumps -> put in notes), free_rent_months, ti_psf ($/SF), lease_kind (new|renewal|expansion|extension|sublease), landlord
LISTING: asking_rent_psf_yr (annual; convert monthly x12), asking_price, available_sf; an IOS / yard asking rate per acre per month goes in rent_per_acre_mo
BUILDING FEATURES (only if stated): building_sf (TOTAL building size; for leases this is the building, not the leased space), land_acres (land SF / 43,560, 2 decimals), year_built, year_renovated, clear_height_ft ("28'" -> 28; ranges -> max, note the range), dock_doors (dock-high doors/docks/DH count), drive_in_doors (grade-level / drive-in / GL / ramps count), loading (rear|front|cross_dock|side|rail), construction (tilt_wall|metal|masonry|concrete_block|precast|steel), office_sf, office_pct, sprinkler (ESFR|wet|dry|none), rail_served (true/false), yard_acres (outdoor storage acres), building_class (A|B|C), property_subtype (bulk_distribution|shallow_bay|flex|manufacturing|ios|cold_storage|multi_tenant|service|warehouse|data_center), tenancy (single|multi), car_parking, trailer_parking, power (text)
notes: other useful facts from the row (comments, concessions, fixed $ bumps, portfolio details, ranges)
DATES: ISO "YYYY-MM-DD"; month only "YYYY-MM"; quarter "4Q24" -> "2024-Q4"; year only "YYYY". Spreadsheet dates appear as "[date YYYY-MM-DD]".
DECISION RULES:
- Decide sale vs lease vs listing from the columns and values, not the sheet name.
- One record per building per transaction. Portfolio sale with one total price across buildings -> one record per building, sale_type "portfolio", note "portfolio of N; price is total".
- Totals / averages / median / section-title / repeated-header rows are NOT records.
- Rent magnitude check: industrial NNN rent is roughly $3-$20/SF/yr ($0.25-$1.70/SF/mo). A header "Rate" with values like 0.65 is monthly; 7.80 is annual. Flex/office FS can reach $30/yr.
- Prefer the explicit column over parsing notes; if a value only appears inside a comment cell, you may extract it and cite that cell.
- Never fabricate city or zip from a submarket; only use a city when the doc states it or the title makes it unambiguous (then cite the title cell). A zip/city inside a map hyperlink ("link: ...maps...Houston, TX 77044") counts as stated.
- SIZE COLUMNS: in a LEASE table a single size column ("Size", "SF", "Sq Ft", "RSF", "Leased SF", "Area") is leased_sf. Use building_sf only for an explicitly separate building-size column ("Building SF", "RBA", "Bldg Size", "Total Bldg"). In a LISTING table the space size is available_sf. In a SALE table the size is building_sf.
- RENT UNITS: decide from the header text AND magnitudes, not from the survey's guess. "$8.75 NNN" under "Base Rate" is ANNUAL. Only values below about $2.25 are monthly. Never output rent_psf_mo above 2.5.
- HEADER QUALIFIERS apply to every row: a header like "Rate (NNN)", "Rent $/SF/Mo", "Price ($MM)", "Land (AC)" defines the unit/structure for the whole column; a table title like "Cross-Dock Comps", "Rear Load", "Metal Buildings", "Manufacturing", "IOS", "Class B" defines loading / construction / property_subtype / building_class for every row in that table unless a row says otherwise.
- MINE COMMENT CELLS: comments / notes / description cells often hold building features. Extract them into the feature fields (land acres, yard acres, dock doors, drive-ins/grade-level doors, rail, construction tilt-wall/metal, rear/front/cross-dock loading, year built, office SF, clear height, cranes/power, occupancy at sale, sale type like "sale-leaseback" / "owner-user" / "forward sale", class, sprinklers) and cite the comment cell. Keep the full comment text in notes too.
- NAME vs ADDRESS: if one cell mixes a building name and a street ("Northport Bldg 2 / 1234 Main St / Laredo, TX 78045"), split it: the part that starts with a street number goes to address, the name to property_name, city/zip to their fields. A cell that is only a name ("Reynolds Company", "Corporate Drive / Building 1") -> property_name, and address only if a street is present (a street without a number like "Corporate Drive" may go in address).
- DATES in leases: lease_start is the COMMENCEMENT / start / move-in date. If the table has both a signed/execution/transaction date and a commencement date, use commencement for lease_start and put "signed <date>" in notes. Use the signed/transaction date only when no commencement date exists.
- LEASE STRUCTURE: if the rent header or a title says NNN / Net / MG / Gross (e.g. "Rate (NNN)", "Base Rent NNN", "Starting Rent $/SF/Mo NNN"), set lease_structure on every row of that table.
- ADDRESS hidden in text: if no address column exists but the name cell or a comment says "located at 6401 N. Eldridge Road" or the name itself is a street ("10600 Corporate Dr"), put that street in address.
- TENANT columns: in a lease table, a column labelled "Tenant", "User", "Occupant", "Lessee" or a combined "Property/Tenant" holds the tenant (put it in tenant; use property_name only for an obvious building/park name).
- STATUS text: "Leased April 2022" / "Signed 3Q24" gives lease_start; "For Lease", "Available", "On market", "Vacant - asking" means record_kind "listing" (use asking_* / available_sf fields); "Sold", "Closed" -> sale; "Under contract", "Awarded" -> sale with sale_date null and a note.
- PRODUCT TYPE from titles: a file/sheet/table title such as "IOS Lease Comps", "Cold Storage Comps", "Manufacturing Sale Comps", "Flex", "Shallow Bay", "Multi-Tenant", "Bulk" sets property_subtype for every row (ios, cold_storage, manufacturing, flex, shallow_bay, multi_tenant, bulk_distribution). For IOS rows a land/yard acreage column is land_acres (and yard_acres when it says yard/storage acres).
- OCCUPANCY at sale: "vacant" / "0% leased" -> occupancy_pct 0; "100% leased", "fully leased", "lease in place" -> 100; "owner-occupied/user" -> sale_type owner_user.
- sale_type: "portfolio" ONLY when one price covers several buildings; "sale_leaseback", "owner_user", "development" (forward / build-to-suit / land for development) when stated; "investment" when the buyer bought a leased/income property (cap rate given, tenant in place, or the table is an investment-sale table).`;
