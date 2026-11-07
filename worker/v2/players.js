// Buyers, sellers, tenants and landlords as entities, so "who is buying IOS
// in Houston" and "every Link Logistics deal" are one click.

const LEGAL = /\b(l\.?l\.?c|l\.?l\.?p|l\.?p|inc|incorporated|corp|corporation|co|company|ltd|limited|plc|pllc|n\.?a|trust|the)\b\.?/g;
const JUNK = /^(undisclosed|confidential|private|private (buyer|investor|individual)|individual|owner|owner[- ]user|user|unknown|n\/?a|tbd|none|vacant|spec|various|multiple|-+|\?|withheld|not disclosed|investor)$/i;

// "EastGroup Properties" = "EastGroup", "Link Logistics" = "Link": trailing generic words.
const GENERIC = /\s+(properties|property|realty|real estate|logistics|industrial|holdings|group|investments|investors|reit|realty trust|ventures|development|developments|partners)$/;
export function coreName(norm) {
  let s = norm || "";
  for (let i = 0; i < 3; i++) s = s.replace(GENERIC, "");
  return s.length >= 3 ? s : norm;
}

export function normName(name) {
  if (!name) return null;
  let s = String(name).toLowerCase().replace(/&/g, " and ").replace(/[’'`]/g, "").replace(/[^a-z0-9 ]+/g, " ");
  s = s.replace(LEGAL, " ").replace(/\s+/g, " ").trim();
  if (!s || s.length < 2 || JUNK.test(s) || JUNK.test(String(name).trim())) return null;
  return s;
}

const ROLES = { buyer: "buyer", seller: "seller", tenant: "tenant", landlord: "landlord" };

// Link a comp to its players. Idempotent (comp_id, player_id, role) PK.
export async function linkPlayers(db, compId, rec, date = null) {
  const stmts = [];
  for (const [field, role] of Object.entries(ROLES)) {
    const raw = rec[field];
    if (!raw) continue;
    // "ABC Logistics / XYZ Partners" style joint buyers
    for (const part of String(raw).split(/\s*(?:\/|;| and (?=[A-Z]))\s*/).slice(0, 3)) {
      const norm = normName(part);
      if (!norm) continue;
      const alias = (await db.prepare("SELECT player_id FROM v2_player_aliases WHERE alias_norm = ?").bind(norm).first()) || (await db.prepare("SELECT player_id FROM v2_player_aliases WHERE alias_norm = ?").bind(`core:${coreName(norm)}`).first());
      let pid = alias?.player_id;
      if (!pid) {
        const row = await db
          .prepare("INSERT INTO v2_players (name, norm, kinds, n_deals, last_date) VALUES (?, ?, ?, 0, ?) ON CONFLICT(norm) DO UPDATE SET name = name RETURNING id")
          .bind(String(part).trim().slice(0, 120), norm, `|${role}|`, date)
          .first();
        pid = row.id;
      }
      stmts.push(db.prepare("INSERT OR IGNORE INTO v2_comp_players (comp_id, player_id, role) VALUES (?, ?, ?)").bind(compId, pid, role));
      stmts.push(
        db
          .prepare(
            `UPDATE v2_players SET kinds = CASE WHEN instr(COALESCE(kinds,''), ?) THEN kinds ELSE COALESCE(kinds,'|') || ? END,
             n_deals = (SELECT count(DISTINCT cp.comp_id) FROM v2_comp_players cp JOIN v2_comps c ON c.id = cp.comp_id WHERE cp.player_id = v2_players.id AND c.status = 'active'),
             last_date = (SELECT max(c.txn_date_sort) FROM v2_comp_players cp JOIN v2_comps c ON c.id = cp.comp_id WHERE cp.player_id = v2_players.id AND c.status = 'active' AND c.txn_date_sort <= date('now')) WHERE id = ?`,
          )
          .bind(`|${role}|`, `${role}|`, pid),
      );
    }
  }
  if (stmts.length) await db.batch(stmts);
}
