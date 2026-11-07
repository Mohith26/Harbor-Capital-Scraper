-- Comp database v2: properties (buildings + features) and comps (transactions).
-- Legacy tables (sale_comps, lease_comps, ...) are left untouched.

CREATE TABLE IF NOT EXISTS v2_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT,
  size INTEGER,
  kind TEXT,
  source_kind TEXT DEFAULT 'broker',      -- broker | costar | legacy | manual
  broker TEXT,
  uploaded_by TEXT,
  uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
  status TEXT DEFAULT 'new',              -- new | surveyed | extracting | review | committed | failed
  summary TEXT,
  survey_json TEXT,
  stats_json TEXT,
  r2_key TEXT
);
CREATE INDEX IF NOT EXISTS ix_v2_files_sha ON v2_files(sha256);

CREATE TABLE IF NOT EXISTS v2_properties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  addr_key TEXT,
  address TEXT,
  property_name TEXT,
  city TEXT,
  state TEXT DEFAULT 'TX',
  zip TEXT,
  county TEXT,
  market TEXT,
  submarket TEXT,
  latitude REAL,
  longitude REAL,
  geocode_quality TEXT,
  geocode_source TEXT,
  formatted_address TEXT,
  parcel_id TEXT,
  costar_prop_id TEXT,
  building_sf REAL,
  land_acres REAL,
  year_built INTEGER,
  year_renovated INTEGER,
  clear_height_ft REAL,
  dock_doors INTEGER,
  drive_in_doors INTEGER,
  loading TEXT,
  construction TEXT,
  office_sf REAL,
  office_pct REAL,
  sprinkler TEXT,
  rail_served INTEGER,
  yard_acres REAL,
  building_class TEXT,
  property_subtype TEXT,
  tenancy TEXT,
  car_parking INTEGER,
  trailer_parking INTEGER,
  power TEXT,
  cranes TEXT,
  zoning TEXT,
  owner TEXT,
  features_json TEXT,                     -- {field: {value, source, ref, confidence, at}} winning value per field
  enrich_status TEXT DEFAULT 'pending',   -- pending | done | partial | failed
  enriched_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_v2_prop_addr ON v2_properties(addr_key);
CREATE INDEX IF NOT EXISTS ix_v2_prop_geo ON v2_properties(latitude, longitude);
CREATE INDEX IF NOT EXISTS ix_v2_prop_market ON v2_properties(market, submarket);
CREATE INDEX IF NOT EXISTS ix_v2_prop_costar ON v2_properties(costar_prop_id);

CREATE TABLE IF NOT EXISTS v2_comps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  property_id INTEGER REFERENCES v2_properties(id),
  record_kind TEXT NOT NULL,              -- sale | lease | listing | land_sale
  status TEXT DEFAULT 'active',           -- active | duplicate | rejected
  dup_of INTEGER,
  file_id INTEGER,
  source_kind TEXT DEFAULT 'broker',
  source_label TEXT,                      -- file name / "CoStar" / "legacy import"
  source_sheet TEXT,
  source_rows TEXT,
  sources_json TEXT,                      -- every source that reported this deal
  sale_price REAL, price_psf REAL, price_per_acre REAL, cap_rate_pct REAL, sale_date TEXT, buyer TEXT, seller TEXT, sale_type TEXT, occupancy_pct REAL, noi REAL,
  tenant TEXT, leased_sf REAL, rent_psf_yr REAL, rent_psf_mo REAL, rent_per_acre_mo REAL, monthly_rent_total REAL, lease_structure TEXT, term_months REAL,
  lease_start TEXT, lease_end TEXT, escalation_pct REAL, free_rent_months REAL, ti_psf REAL, lease_kind TEXT, landlord TEXT,
  asking_rent_psf_yr REAL, asking_price REAL, available_sf REAL,
  txn_date TEXT,
  txn_year REAL,                          -- decimal year for recency math
  snapshot_json TEXT,                     -- the full record as stated by this source (incl. features)
  flags_json TEXT,
  severity TEXT,
  notes TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_v2_comps_prop ON v2_comps(property_id);
CREATE INDEX IF NOT EXISTS ix_v2_comps_kind ON v2_comps(record_kind, status, txn_year);
CREATE INDEX IF NOT EXISTS ix_v2_comps_file ON v2_comps(file_id);

CREATE TABLE IF NOT EXISTS v2_feature_obs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  property_id INTEGER NOT NULL,
  field TEXT NOT NULL,
  value TEXT,
  source TEXT NOT NULL,                   -- broker_sheet | costar | cad | parcel | web | aerial | manual
  source_ref TEXT,
  confidence REAL,
  observed_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_v2_obs_prop ON v2_feature_obs(property_id, field);

-- Upload staging: one row per extracted record awaiting review.
CREATE TABLE IF NOT EXISTS v2_jobs (
  id TEXT PRIMARY KEY,
  file_id INTEGER,
  status TEXT,                            -- surveying | extracting | resolving | review | committed | failed
  plan_json TEXT,                         -- chunk plan from the survey
  progress_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS v2_staged (
  job_id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  chunk INTEGER,
  record_json TEXT NOT NULL,
  match_json TEXT,                        -- dedupe / property match result
  decision TEXT,                          -- add | merge | skip | null
  PRIMARY KEY (job_id, idx)
);

-- CoStar property index (TX industrial buildings with physical attributes).
CREATE TABLE IF NOT EXISTS v2_costar_props (
  prop_id TEXT PRIMARY KEY,
  address TEXT, city TEXT, zip TEXT, county TEXT, market TEXT, submarket TEXT,
  latitude REAL, longitude REAL, addr_key TEXT,
  building_sf REAL, land_acres REAL, year_built INTEGER, year_renovated INTEGER, clear_height_ft REAL,
  dock_doors INTEGER, drive_in_doors INTEGER, building_class TEXT, sec_type TEXT, stories TEXT,
  power TEXT, crane TEXT, rail TEXT, zoning TEXT, parcel TEXT, name TEXT
);
CREATE INDEX IF NOT EXISTS ix_v2_costar_geo ON v2_costar_props(latitude, longitude);
CREATE INDEX IF NOT EXISTS ix_v2_costar_addr ON v2_costar_props(addr_key);

-- Submarket gazetteer (CoStar submarket centroids + analyst aliases).
CREATE TABLE IF NOT EXISTS v2_submarkets (
  name TEXT, market TEXT, latitude REAL, longitude REAL, radius_mi REAL, n INTEGER, aliases TEXT,
  PRIMARY KEY (market, name)
);

CREATE TABLE IF NOT EXISTS v2_geocode_cache (q TEXT PRIMARY KEY, result_json TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS v2_enrich_cache (k TEXT PRIMARY KEY, result_json TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
