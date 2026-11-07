-- Comps overhaul (Nov 2026): deals + comp sets, players, edit history,
-- saved searches / alerts, sortable dates, product tags, inbox provenance.
-- Additive only: nothing existing is renamed or dropped (Atlas mirrors v2_comps).

ALTER TABLE v2_comps ADD COLUMN txn_date_sort TEXT;      -- ISO date for sorting / recency (mid-period for month/quarter/year dates)
ALTER TABLE v2_comps ADD COLUMN date_precision TEXT;     -- day | month | quarter | year | none
ALTER TABLE v2_comps ADD COLUMN net_effective_rent_psf_yr REAL;
ALTER TABLE v2_comps ADD COLUMN rent_basis TEXT;         -- NNN | gross | MG | NN (normalized lease_structure)
ALTER TABLE v2_comps ADD COLUMN tenant_industry TEXT;
ALTER TABLE v2_properties ADD COLUMN product_tags TEXT;  -- '|crane|heavy_power|ios|' style, for LIKE '%|crane|%'
ALTER TABLE v2_files ADD COLUMN via TEXT;                -- upload | paste | email | slack | bulk
ALTER TABLE v2_files ADD COLUMN via_ref TEXT;            -- email sender + subject, slack channel + permalink

CREATE INDEX IF NOT EXISTS idx_v2_comps_sort ON v2_comps(txn_date_sort);
CREATE INDEX IF NOT EXISTS idx_v2_props_tags ON v2_properties(product_tags);

-- Keep txn_date_sort / date_precision in step with txn_date on every write path.
CREATE TRIGGER IF NOT EXISTS trg_v2_comps_datesort_ins AFTER INSERT ON v2_comps BEGIN
  UPDATE v2_comps SET
    txn_date_sort = CASE
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]*' THEN substr(NEW.txn_date, 1, 10)
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]' THEN NEW.txn_date || '-15'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-Q[1-4]' THEN substr(NEW.txn_date, 1, 4) || '-' || printf('%02d', CAST(substr(NEW.txn_date, 7, 1) AS INTEGER) * 3 - 1) || '-15'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]' THEN NEW.txn_date || '-07-01'
      ELSE NULL END,
    date_precision = CASE
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]*' THEN 'day'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]' THEN 'month'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-Q[1-4]' THEN 'quarter'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]' THEN 'year'
      ELSE 'none' END
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_v2_comps_datesort_upd AFTER UPDATE OF txn_date ON v2_comps BEGIN
  UPDATE v2_comps SET
    txn_date_sort = CASE
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]*' THEN substr(NEW.txn_date, 1, 10)
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]' THEN NEW.txn_date || '-15'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-Q[1-4]' THEN substr(NEW.txn_date, 1, 4) || '-' || printf('%02d', CAST(substr(NEW.txn_date, 7, 1) AS INTEGER) * 3 - 1) || '-15'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]' THEN NEW.txn_date || '-07-01'
      ELSE NULL END,
    date_precision = CASE
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]*' THEN 'day'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-[01][0-9]' THEN 'month'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]-Q[1-4]' THEN 'quarter'
      WHEN NEW.txn_date GLOB '[12][0-9][0-9][0-9]' THEN 'year'
      ELSE 'none' END
  WHERE id = NEW.id;
END;

-- Backfill existing rows.
UPDATE v2_comps SET txn_date = txn_date;
UPDATE v2_comps SET rent_basis = CASE
  WHEN lease_structure IS NULL THEN NULL
  WHEN upper(lease_structure) IN ('NNN','TRIPLE NET','NET') THEN 'NNN'
  WHEN upper(lease_structure) IN ('NN','DOUBLE NET','MODIFIED NET') THEN 'NN'
  WHEN upper(lease_structure) IN ('MG','MODIFIED GROSS','IG','INDUSTRIAL GROSS') THEN 'MG'
  WHEN upper(lease_structure) IN ('GROSS','FSG','FULL SERVICE','FULL SERVICE GROSS') THEN 'gross'
  ELSE lease_structure END
WHERE record_kind IN ('lease','listing');

-- Deals: a subject property plus its comp set (sale and lease lanes).
CREATE TABLE IF NOT EXISTS v2_deals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  address TEXT,
  city TEXT,
  market TEXT,
  submarket TEXT,
  latitude REAL,
  longitude REAL,
  geocode_quality TEXT,
  property_id INTEGER,
  lanes TEXT DEFAULT 'sale,lease',      -- which comp types this deal needs
  source_text TEXT,                     -- the pasted write-up / request
  subject_json TEXT,                    -- {field: {value, source, ref}} + conflicts
  search_json TEXT,                     -- radius, since, size band, product overrides
  status TEXT DEFAULT 'active',         -- active | archived
  watch INTEGER DEFAULT 1,
  slack_channel TEXT,
  hubspot_deal_id TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  last_checked_at TEXT,
  last_seen_comp_id INTEGER
);

CREATE TABLE IF NOT EXISTS v2_set_items (
  deal_id INTEGER NOT NULL,
  comp_id INTEGER NOT NULL,
  state TEXT NOT NULL,                  -- add | pass
  reason TEXT,
  note TEXT,
  adjust_json TEXT,                     -- {size: -5, clear: 3, age: 0, location: 0, condition: 0} percent
  position INTEGER,
  score INTEGER,                        -- match score when added
  by_user TEXT,
  at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (deal_id, comp_id)
);
CREATE INDEX IF NOT EXISTS idx_v2_set_items_comp ON v2_set_items(comp_id);

-- Buyers, sellers, tenants, landlords, brokers as entities.
CREATE TABLE IF NOT EXISTS v2_players (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  norm TEXT NOT NULL UNIQUE,
  kinds TEXT,                           -- '|buyer|tenant|'
  n_deals INTEGER DEFAULT 0,
  last_date TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS v2_player_aliases (alias_norm TEXT PRIMARY KEY, player_id INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS v2_comp_players (
  comp_id INTEGER NOT NULL,
  player_id INTEGER NOT NULL,
  role TEXT NOT NULL,                   -- buyer | seller | tenant | landlord
  PRIMARY KEY (comp_id, player_id, role)
);
CREATE INDEX IF NOT EXISTS idx_v2_comp_players_p ON v2_comp_players(player_id);

-- Every manual change, with the original kept.
CREATE TABLE IF NOT EXISTS v2_edits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity TEXT NOT NULL,                 -- comp | property | deal
  entity_id INTEGER NOT NULL,
  field TEXT NOT NULL,
  old_value TEXT,
  new_value TEXT,
  reason TEXT,
  by_user TEXT,
  at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_v2_edits_e ON v2_edits(entity, entity_id);

-- Saved searches and library views (with optional alerts).
CREATE TABLE IF NOT EXISTS v2_saved (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                   -- search | view
  name TEXT NOT NULL,
  query TEXT,
  filters_json TEXT,
  location_json TEXT,
  polygon_json TEXT,
  alert INTEGER DEFAULT 0,
  shared INTEGER DEFAULT 1,
  owner TEXT,
  last_seen_comp_id INTEGER,
  last_run_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- In-app notifications (new comps matching a deal or saved search).
CREATE TABLE IF NOT EXISTS v2_alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner TEXT,
  kind TEXT NOT NULL,                   -- deal | search
  ref_id INTEGER NOT NULL,
  title TEXT,
  comp_ids_json TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  seen_at TEXT
);
