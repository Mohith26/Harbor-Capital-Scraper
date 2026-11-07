// Fully LLM-driven comp extraction.
//
//   1. survey   - one call reads the whole (abridged) document and maps every
//                 table: which sheet/rows, sale vs lease vs listing, header
//                 rows, units, rows to skip, duplicate tabs, market context.
//   2. extract  - per table chunk, the model reads the actual cells and emits
//                 schema records, citing the cell (or PDF line) behind every
//                 value it outputs.
//   3. verify   - deterministic: every number the model emitted is checked
//                 against the cited cell text (allowing only the unit
//                 conversions the schema defines). Unsupported values are
//                 flagged, never silently trusted.
//   4. normalize/validate happens in validate.js.

import { pool } from "./llm.js";
import { SCHEMA_PROMPT, RECORD_KEYS, NUMERIC_FIELDS } from "./schema.js";

const SURVEY_SYSTEM = `You are a senior commercial real estate analyst at Harbor Capital (Texas infill industrial: DFW, Houston, San Antonio, Austin, Laredo).
You are given a broker comp spreadsheet or PDF rendered cell by cell. Map its structure precisely so a second pass can extract every comp.
Rendering: each line is "<row>: <COL>=\"<cell text>\" | ...". Text like "$6.50 [=6.5]" shows the displayed text and the underlying raw number. PDFs are "<line>: text | text" and multi-line table cells are split across neighbouring lines.
Return JSON only.`;

function surveyUser(doc, preview) {
  return `File name: ${doc.filename}
Document type: ${doc.kind}

${preview}

Return JSON:
{
  "document_summary": "1-2 sentences: what this is (e.g. 'CBRE lease comp set for Lower GSW shallow-bay, 12 comps')",
  "context": {"market": "Houston|DFW|San Antonio|Austin|Laredo|El Paso|Other|null", "city": "city implied by title/filename or null", "submarket": "or null", "as_of": "date of the survey if stated, ISO or null"},
  "tables": [
    {
      "sheet": "exact sheet name (or 'page N' for PDFs)",
      "title": "table title / section name if any",
      "record_kind": "sale | lease | listing | land_sale | mixed | none",
      "header_rows": [row numbers that hold column headers (may be 2 rows)],
      "first_row": first data row number,
      "last_row": last data row number,
      "skip_rows": [row numbers inside the range that are NOT comps: totals, averages, section headers, blank spacers, footnotes, repeated headers],
      "subject_rows": [rows describing the subject property being underwritten, if any],
      "columns": {"<COL letter or column label>": "meaning incl. units ONLY as evidenced by the header text or value magnitudes (rents under ~$2.25 are monthly, $3+ are annual; '$8.75 NNN' is annual). In lease tables a lone size column is the LEASED SF. Examples: 'rent $/SF/yr NNN', 'sale price total $', 'land area in SF', 'cap rate as decimal 0.0625', 'comments - contains building features'"},
      "row_notes": "how one record is laid out (e.g. 'one row per comp', 'each comp spans 2-3 PDF lines; address wraps onto following line', 'portfolio rows share one price')",
      "duplicate_of": "ONLY if this table is an exact repeat of another table with NO additional columns/information (e.g. a hidden copy), name that table; else null",
      "overlaps_with": "if it lists the same comps as another table but adds or differs in columns (summary vs detailed), name that table (both will be extracted and merged), else null"
    }
  ],
  "warnings": ["anything unusual: missing header row (first data row looks like a header), mislabeled sheet name (sheet says Lease but data are sales), mixed monthly/annual rents, scanned pages with no text, etc."]
}
Rules: units you write in "columns" must be justified by the header text or magnitudes - the next pass relies on them. Include EVERY table that holds comps, listings/availabilities or a subject property, including hidden sheets (mark duplicate_of when they repeat visible data). Tables with record_kind "none" (assumptions, charts data, notes) may be listed briefly. Decide sale vs lease from the columns, not the sheet name. If a sheet has several separate tables stacked vertically or side by side, list each separately. If there is NO header row, set header_rows [] and describe the columns from the data.`;
}

const EXTRACT_SYSTEM = `You are a meticulous commercial real estate data extractor at Harbor Capital. You convert broker comp tables into structured records with ZERO invented values.
Every value you output must come from the document. For each value, cite where it came from in "evidence": a spreadsheet cell like "Y5" (column letter + row) or, for PDFs, the line number like "L14". Derived values (allowed conversions only) cite the source cell they were computed from.
${SCHEMA_PROMPT}
Return JSON only.`;

function extractUser({ doc, table, survey, headerText, contextText, chunkText, isPdf }) {
  return `File: ${doc.filename}
Document summary: ${survey.document_summary || ""}
Context: ${JSON.stringify(survey.context || {})}
Table: sheet "${table.sheet}" ${table.title ? `- "${table.title}"` : ""}; expected record_kind: ${table.record_kind}
Column meanings from the survey (verify against the cells, they can be wrong): ${JSON.stringify(table.columns || {})}
Layout notes: ${table.row_notes || ""}
Rows to skip (totals/sections): ${JSON.stringify(table.skip_rows || [])}; subject-property rows: ${JSON.stringify(table.subject_rows || [])}

HEADER ROWS:
${headerText || "(no header row - infer columns from the data)"}
${contextText ? `\nROWS JUST ABOVE THIS CHUNK (context only, already extracted - do not output records for them):\n${contextText}\n` : ""}
ROWS TO EXTRACT:
${chunkText}

Output {"records":[...]} with one record per comp in ROWS TO EXTRACT (and nothing for header/total/section/blank rows).
Each record: only include keys that have a value (omit nulls), always include "record_kind", "source_sheet", "source_rows", and "evidence" (object mapping each numeric/date/name field you filled to its cell${isPdf ? " or line (e.g. \"L14\")" : " (e.g. \"Y5\")"}).
${isPdf ? "PDF tables: a single comp usually spans several consecutive lines (wrapped address, tenant, landlord, comments). Reassemble each comp from its lines and list all of them in source_rows. Columns are separated by ' | ' but wrapped fragments shift positions, so align values to the header by meaning (sizes look like 12,800; dates like 1/1/25; rents like $9.75 NNN; clear heights like 21'; years like 1978).\n" : ""}Be exhaustive and exact: copy numbers digit for digit, convert units only as the schema says.`;
}

function previewText(doc, { maxChars = 120000 } = {}) {
  const parts = [];
  let used = 0;
  for (const s of doc.sheets) {
    if (!s.rows.length) {
      parts.push(`### ${doc.kind === "pdf" ? "PDF " : "Sheet "}"${s.name}" (empty${doc.kind === "pdf" ? " - no text layer, likely scanned image" : ""})`);
      continue;
    }
    const lines = s.text.split("\n");
    const head = lines.slice(0, 2).filter((l) => l.startsWith("###") || l.startsWith("merged"));
    const body = lines.filter((l) => !l.startsWith("###") && !l.startsWith("merged"));
    const budget = Math.max(4000, Math.floor((maxChars - used) / Math.max(1, doc.sheets.length - parts.length)));
    let chosen;
    if (s.text.length <= budget) chosen = body;
    else {
      // Keep the top (titles, headers, first comps) and the tail (totals).
      const keepTop = [];
      let c = 0;
      for (const l of body) {
        if (c + l.length > budget * 0.8) break;
        keepTop.push(l.length > 1500 ? l.slice(0, 1500) + " ..." : l);
        c += Math.min(l.length, 1500);
      }
      const tail = body.slice(-6).map((l) => (l.length > 800 ? l.slice(0, 800) + " ..." : l));
      chosen = [...keepTop, `... (${body.length - keepTop.length - tail.length} more rows not shown; the full table continues in the same layout) ...`, ...tail];
    }
    const block = [...head, ...chosen].join("\n");
    used += block.length;
    parts.push(block);
  }
  return parts.join("\n\n");
}

function sheetByName(doc, name) {
  return doc.sheets.find((s) => s.name === name) || doc.sheets.find((s) => s.name.trim().toLowerCase() === String(name).trim().toLowerCase());
}

function rowLine(r) {
  return `${r.row}: ` + r.cells.map((c) => `${c.col}=${JSON.stringify(c.text)}`).join(" | ");
}

// Split a table's data rows into chunks the model can handle precisely.
function chunkRows(rows, { maxRows, maxChars }) {
  const chunks = [];
  let cur = [];
  let chars = 0;
  for (const r of rows) {
    const l = rowLine(r);
    if (cur.length && (cur.length >= maxRows || chars + l.length > maxChars)) {
      chunks.push(cur);
      cur = [];
      chars = 0;
    }
    cur.push(r);
    chars += l.length;
  }
  if (cur.length) chunks.push(cur);
  return chunks;
}

// PDF chunks must not split one comp across chunks: cut only at lines that
// begin a new record. We overlap by a few lines and let dedupe by
// source_rows resolve doubles.
function chunkPdfRows(rows, { maxRows }) {
  if (rows.length <= maxRows) return [rows];
  const chunks = [];
  for (let i = 0; i < rows.length; i += maxRows - 6) chunks.push(rows.slice(i, i + maxRows));
  return chunks;
}

export async function surveyDocument(doc, llm, opts = {}) {
  const preview = previewText(doc);
  const survey = await llm.chat({ system: SURVEY_SYSTEM, user: surveyUser(doc, preview), tier: opts.surveyTier || "strong", reasoning: opts.surveyReasoning || "low", maxTokens: 16000 });
  survey.tables = Array.isArray(survey.tables) ? survey.tables : [];
  return survey;
}

// Turn a survey into concrete chunk jobs (row numbers only, so the plan can be
// stored and each chunk executed in a separate request).
export function planChunks(doc, survey, { chunkRowsMax = 22 } = {}) {
  const isPdf = doc.kind === "pdf";
  const plan = [];
  survey.tables.forEach((table, ti) => {
    if (!table.record_kind || table.record_kind === "none" || table.duplicate_of) return;
    const sheet = sheetByName(doc, table.sheet);
    if (!sheet) return;
    const headerRows = new Set((table.header_rows || []).map(Number));
    const first = Number(table.first_row) || (headerRows.size ? Math.max(...headerRows) + 1 : 1);
    const last = Number(table.last_row) || Infinity;
    const skip = new Set((table.skip_rows || []).map(Number));
    const data = sheet.rows.filter((r) => r.row >= first && r.row <= last && !headerRows.has(r.row) && !skip.has(r.row));
    if (!data.length) return;
    const chunks = isPdf ? chunkPdfRows(data, { maxRows: 70 }) : chunkRows(data, { maxRows: chunkRowsMax, maxChars: 14000 });
    for (const chunk of chunks) plan.push({ table: ti, sheet: table.sheet, rows: chunk.map((r) => r.row) });
  });
  return plan;
}

export async function extractChunk(doc, survey, job, llm, opts = {}) {
  const isPdf = doc.kind === "pdf";
  const table = survey.tables[job.table];
  const sheet = sheetByName(doc, table.sheet);
  const headerRows = new Set((table.header_rows || []).map(Number));
  const rowSet = new Set(job.rows);
  const headerText = sheet.rows.filter((r) => headerRows.has(r.row)).map(rowLine).join("\n");
  const minHeader = headerRows.size ? Math.min(...headerRows) : job.rows[0];
  const titleRows = sheet.rows.filter((r) => r.row < minHeader).slice(-4).map(rowLine).join("\n");
  const chunkRowsList = sheet.rows.filter((r) => rowSet.has(r.row));
  const prev = sheet.rows.filter((r) => r.row < job.rows[0] && !headerRows.has(r.row) && r.row >= minHeader).slice(-2);
  const out = await llm
    .chat({
      system: EXTRACT_SYSTEM,
      user: extractUser({ doc, table, survey, headerText: (titleRows ? titleRows + "\n" : "") + headerText, contextText: prev.map(rowLine).join("\n"), chunkText: chunkRowsList.map(rowLine).join("\n"), isPdf }),
      tier: opts.extractTier || "strong",
      reasoning: opts.extractReasoning || "off",
      maxTokens: 24000,
    })
    .catch((e) => ({ records: [], error: String(e.message || e) }));
  const recs = (out.records || []).map((r) => ({ ...r, source_sheet: r.source_sheet || table.sheet }));
  let kept = recs.filter((r) => !Array.isArray(r.source_rows) || !r.source_rows.length || r.source_rows.some((x) => rowSet.has(Number(x))));
  if (opts.review !== false && kept.length) {
    const hdr = (titleRows ? titleRows + "\n" : "") + headerText;
    kept = await reviewChunk(doc, table, hdr, chunkRowsList.map(rowLine).join("\n"), kept, llm, opts);
    kept = kept.filter((r) => !Array.isArray(r.source_rows) || !r.source_rows.length || r.source_rows.some((x) => rowSet.has(Number(x))));
  }
  return { records: kept.map((r) => verifyRecord(normalizeKeys(r), doc)), error: out.error };
}

const REVIEW_SYSTEM = `You are auditing a junior analyst's extraction of broker comp rows. Compare the source rows with the extracted records and fix them.
${SCHEMA_PROMPT}
Return JSON only.`;

// Second pass: the model re-reads the same rows next to its own output and
// returns only corrections (missed fields from comment / combined cells,
// table-title implications, wrong units, wrong record_kind, wrong column).
async function reviewChunk(doc, table, headerText, chunkText, records, llm, opts) {
  if (!records.length) return records;
  const compact = records.map((r, i) => {
    const o = { i };
    for (const [k, v] of Object.entries(r)) if (v != null && !k.startsWith("_") && k !== "evidence" && k !== "state" && !(Array.isArray(v) && !v.length)) o[k] = v;
    return o;
  });
  const out = await llm
    .chat({
      system: REVIEW_SYSTEM,
      user: `File: ${doc.filename}; table "${table.sheet}"${table.title ? ` - ${table.title}` : ""}.
HEADER / TITLE ROWS:
${headerText}

SOURCE ROWS:
${chunkText}

EXTRACTED RECORDS:
${JSON.stringify(compact)}

Check every record against its source row(s). Look especially for:
- values stated only inside comment / notes / description / combined cells (escalations "3.75% bumps", free rent "3 months free", year built "2024 build", office SF, land/yard acres, docks, clear height, construction, loading, sale type, occupancy) that were left empty;
- facts implied by the file name, sheet name or a table title for every row (product type such as IOS / manufacturing / cold storage / shallow bay, loading such as cross-dock, construction such as metal, lease structure such as NNN in a rent header, city when the title names one city);
- a tenant / occupant column that was not mapped to tenant;
- availability / for-lease / on-market rows that should be record_kind "listing";
- wrong units (monthly vs annual rent, decimals vs percent) or values taken from the wrong column.
Return {"patches":[{"i": record index, "set": {"field": value, ...}, "evidence": {"field": "cell or line"}, "why": "short"}], "missing_records": [ full records for comps in SOURCE ROWS that were not extracted at all ]}. Return {"patches":[],"missing_records":[]} if everything is right. Never invent values.`,
      tier: opts.reviewTier || opts.extractTier || "strong",
      reasoning: opts.reviewReasoning || "off",
      maxTokens: 12000,
    })
    .catch(() => null);
  if (!out) return records;
  const fixed = records.map((r) => ({ ...r, evidence: { ...(r.evidence || {}) } }));
  for (const p of out.patches || []) {
    const r = fixed[Number(p.i)];
    if (!r || !p.set) continue;
    for (const [k, v] of Object.entries(p.set)) {
      if (!RECORD_KEYS.includes(k) || ["source_sheet", "source_rows"].includes(k)) continue;
      r[k] = v === "" ? null : v;
      if (p.evidence && p.evidence[k]) r.evidence[k] = p.evidence[k];
    }
    r._reviewed = [...(r._reviewed || []), ...Object.keys(p.set)];
  }
  for (const m of out.missing_records || []) if (m && (m.address || m.property_name || m.tenant)) fixed.push({ ...m, source_sheet: m.source_sheet || table.sheet, _added_by_review: true });
  return fixed;
}

export function dedupeChunkOverlap(records) {
  const seen = new Set();
  return records.filter((r) => {
    const k = `${r.source_sheet}|${(r.source_rows || [])[0]}|${r.address || r.property_name || ""}|${r.tenant || ""}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

export async function extractDocument(doc, llm, opts = {}) {
  const { concurrency = 6, onProgress } = opts;
  const t0 = Date.now();
  const survey = await surveyDocument(doc, llm, opts);
  const plan = planChunks(doc, survey, opts);
  let done = 0;
  const results = await pool(plan, concurrency, async (job) => {
    const r = await extractChunk(doc, survey, job, llm, opts);
    done++;
    onProgress && onProgress({ done, total: plan.length });
    return r;
  });
  const errors = results.filter((r) => r.error).map((r) => r.error);
  const records = dedupeChunkOverlap(results.flatMap((r) => r.records));
  return { survey, records, duplicateTables: survey.tables.filter((t) => t.duplicate_of), errors, jobs: plan.length, ms: Date.now() - t0 };
}

function normalizeKeys(r) {
  const out = {};
  for (const k of RECORD_KEYS) out[k] = r[k] === undefined || r[k] === "" ? null : r[k];
  out.evidence = r.evidence && typeof r.evidence === "object" ? r.evidence : {};
  out.source_rows = Array.isArray(r.source_rows) ? r.source_rows.map(Number).filter((x) => !Number.isNaN(x)) : r.source_rows != null ? [Number(r.source_rows)] : [];
  for (const k of NUMERIC_FIELDS) {
    if (out[k] != null && typeof out[k] !== "number") {
      const n = parseNumberLoose(out[k], k);
      out[k] = n;
    }
  }
  return out;
}

export function parseNumberLoose(v, field = "") {
  if (v == null) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  let s = String(v).trim().toLowerCase();
  if (/year/.test(field)) {
    const y = s.match(/(18|19|20)\d{2}/);
    return y ? Number(y[0]) : null;
  }
  // "10.051" style thousands separators in SF fields
  if (/_sf$|^sf$/.test(field) && /^\d{1,3}\.\d{3}$/.test(s)) return Number(s.replace(".", ""));
  if (!s) return null;
  let mult = 1;
  if (/\b(mm|million|m)\b|\dm\b/.test(s)) mult = 1e6;
  else if (/\d\s*k\b|thousand/.test(s)) mult = 1e3;
  const neg = /^\(.*\)$/.test(s) || s.startsWith("-");
  s = s.replace(/[^0-9.]/g, "");
  if (!s || s === ".") return null;
  const n = parseFloat(s) * mult;
  return Number.isFinite(n) ? (neg ? -n : n) : null;
}

// ---------- verification ----------

function numbersIn(text) {
  const out = [];
  const t = String(text);
  const bracket = [...t.matchAll(/\[=(-?[\d.eE+-]+)\]/g)].map((m) => parseFloat(m[1]));
  out.push(...bracket);
  const cleaned = t.replace(/\[=[^\]]*\]/g, " ").replace(/\[date [^\]]*\]/g, " ");
  for (const m of cleaned.matchAll(/-?\$?\s?\d[\d,]*(?:\.\d+)?\s*(?:mm|m|k|million)?/gi)) {
    const n = parseNumberLoose(m[0]);
    if (n != null) out.push(n);
    const plain = parseFloat(m[0].replace(/[^0-9.\-]/g, ""));
    if (Number.isFinite(plain)) out.push(plain);
  }
  return out;
}

const CONVERSIONS = {
  rent_psf_yr: [1, 12],
  rent_psf_mo: [1, 1 / 12],
  cap_rate_pct: [1, 100],
  escalation_pct: [1, 100],
  office_pct: [1, 100],
  occupancy_pct: [1, 100],
  land_acres: [1, 1 / 43560],
  yard_acres: [1, 1 / 43560],
  sale_price: [1, 1e3, 1e6],
  asking_price: [1, 1e3, 1e6],
  term_months: [1, 12],
  free_rent_months: [1, 12],
  building_sf: [1, 1e3],
  leased_sf: [1, 1e3],
  available_sf: [1, 1e3],
};

function close(a, b) {
  if (a === b) return true;
  const d = Math.abs(a - b);
  return d <= Math.max(0.011, Math.abs(b) * 0.006);
}

function cellLookup(doc, sheetName, ref) {
  if (!ref) return null;
  const sheet = sheetByName(doc, sheetName);
  const refs = String(ref)
    .split(/[,;+&\s]+/)
    .map((x) => x.trim())
    .filter(Boolean);
  const texts = [];
  for (const one of refs) {
    let m = /^L?(\d+)$/i.exec(one);
    if (m && doc.kind === "pdf") {
      const r = sheet && sheet.rows.find((x) => x.row === Number(m[1]));
      if (r) texts.push(r.cells.map((c) => c.text).join(" | "));
      continue;
    }
    m = /^(?:'?([^'!]+)'?!)?\$?([A-Z]{1,3})\$?(\d+)$/i.exec(one);
    if (!m) continue;
    const sh = m[1] ? sheetByName(doc, m[1]) : sheet;
    if (!sh) continue;
    const r = sh.rows.find((x) => x.row === Number(m[3]));
    const c = r && r.cells.find((x) => x.col === m[2].toUpperCase());
    if (c) texts.push(c.text);
  }
  return texts.length ? texts.join(" || ") : null;
}

function rowText(doc, rec) {
  const sheet = sheetByName(doc, rec.source_sheet);
  if (!sheet) return "";
  const rows = new Set(rec.source_rows || []);
  return sheet.rows.filter((r) => rows.has(r.row)).map((r) => r.cells.map((c) => c.text).join(" | ")).join(" || ");
}

const DERIVABLE = new Set(["rent_psf_yr", "rent_psf_mo", "land_acres", "price_psf", "office_pct", "monthly_rent_total", "term_months"]);

export function verifyRecord(rec, doc) {
  const flags = [];
  const verified = {};
  const allRow = rowText(doc, rec);
  const rowNums = numbersIn(allRow);
  for (const f of NUMERIC_FIELDS) {
    const v = rec[f];
    if (v == null) continue;
    if (f === "year_built" || f === "year_renovated") {
      verified[f] = allRow.includes(String(v)) || numbersIn(cellLookup(doc, rec.source_sheet, rec.evidence[f]) || "").includes(v);
      if (!verified[f]) flags.push({ field: f, code: "unverified", severity: "warn", msg: `${f}=${v} not found in source row` });
      continue;
    }
    const cellText = cellLookup(doc, rec.source_sheet, rec.evidence[f]);
    const pool = cellText != null ? numbersIn(cellText) : rowNums;
    const factors = CONVERSIONS[f] || [1];
    let ok = pool.some((n) => factors.some((k) => close(v, n * k) || close(v, Math.round(n * k * 100) / 100)));
    if (!ok && cellText != null) ok = rowNums.some((n) => factors.some((k) => close(v, n * k)));
    if (!ok && DERIVABLE.has(f)) {
      verified[f] = "derived";
      continue;
    }
    verified[f] = ok;
    if (!ok) flags.push({ field: f, code: "unverified", severity: "warn", msg: `${f}=${v} does not match its cited source (${rec.evidence[f] || "no citation"}: ${String(cellText ?? "").slice(0, 60)})` });
  }
  // Text fields: the address/tenant should appear in the row text.
  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const rowNorm = norm(allRow);
  for (const f of ["address", "tenant", "buyer", "seller"]) {
    if (!rec[f]) continue;
    const words = String(rec[f]).toLowerCase().split(/[^a-z0-9]+/).filter((w) => w.length > 2);
    const hit = words.filter((w) => rowNorm.includes(w)).length;
    verified[f] = words.length === 0 || hit / words.length >= 0.6;
    if (!verified[f]) flags.push({ field: f, code: "unverified", severity: "warn", msg: `${f} "${rec[f]}" not found in source row` });
  }
  return { ...rec, _verify: verified, _flags: flags };
}
