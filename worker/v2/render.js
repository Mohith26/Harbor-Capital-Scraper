// Document -> LLM-readable text renderer.
//
// Every spreadsheet cell is rendered with its real Excel address so the
// extractor can cite evidence ("D14") and the verifier can check every number
// it produced against the source cell. PDFs are rendered page by page with a
// layout-preserving line rebuild so table columns stay visually aligned.

import * as XLSX from "xlsx";

const MAX_COLS = 60;

function colLetter(c) {
  let s = "";
  c += 1;
  while (c > 0) {
    const m = (c - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    c = Math.floor((c - 1) / 26);
  }
  return s;
}

function cleanText(s) {
  return String(s)
    .replace(/\r\n?/g, "\n")
    .replace(/[\u00a0\u2007\u202f]/g, " ")
    .replace(/\n+/g, " / ")
    .replace(/\s+/g, " ")
    .trim();
}

// Text shown for one cell. We prefer the formatted text (keeps $ , % and
// date formatting the broker chose) and append the raw number when the
// format hides information (percent, rounding, scaled formats, dates).
function cellText(cell) {
  if (!cell || cell.t === "z") return "";
  const raw = cell.v;
  let shown = cell.w != null ? cleanText(cell.w) : raw == null ? "" : cleanText(raw);
  if (cell.t === "n" && typeof raw === "number") {
    const fmt = String(cell.z || "");
    const isDate = XLSX.SSF.is_date(fmt);
    if (isDate) {
      const d = XLSX.SSF.parse_date_code(raw);
      if (d) {
        const iso = `${d.y}-${String(d.m).padStart(2, "0")}-${String(d.d).padStart(2, "0")}`;
        if (!shown.includes(iso)) shown = `${shown} [date ${iso}]`;
      }
    } else {
      const rawStr = Number.isInteger(raw) ? String(raw) : String(+raw.toPrecision(10));
      const digitsShown = shown.replace(/[^0-9.\-]/g, "");
      if (digitsShown !== rawStr && shown !== rawStr) shown = `${shown} [=${rawStr}]`;
    }
  }
  if (cell.t === "b") shown = raw ? "TRUE" : "FALSE";
  if (cell.l && cell.l.Target && !/^#/.test(cell.l.Target)) {
    const raw = String(cell.l.Target);
    let tgt = raw;
    try { tgt = decodeURIComponent(tgt); } catch {}
    const coord = /!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)/.exec(tgt) || /@(-?\d+\.\d+),(-?\d+\.\d+)/.exec(tgt);
    tgt = tgt.replace(/\+/g, " ").replace(/\/data=.*$/, "").replace(/\/@[-\d.,z]+.*$/, "").replace(/\?.*$/, "");
    const link = `link: ${tgt.slice(0, 140)}${coord ? ` (coords ${(+coord[1]).toFixed(6)},${(+coord[2]).toFixed(6)})` : ""}`;
    shown = /^https?:/i.test(shown) ? `[${link}]` : `${shown} [${link}]`;
  }
  if (cell.t === "e") shown = cell.w || "#ERR";
  return shown;
}

export function renderWorkbook(buf, { filename = "", maxRowsPerSheet = 2000 } = {}) {
  const wb = XLSX.read(buf, { type: "array", cellDates: false, cellNF: true, cellText: true, sheetStubs: false });
  const sheets = [];
  wb.SheetNames.forEach((name, idx) => {
    const ws = wb.Sheets[name];
    const meta = (wb.Workbook && wb.Workbook.Sheets && wb.Workbook.Sheets[idx]) || {};
    const hidden = meta.Hidden ? true : false;
    if (!ws || !ws["!ref"]) {
      sheets.push({ name, hidden, rows: [], text: "", nonEmpty: 0 });
      return;
    }
    const range = XLSX.utils.decode_range(ws["!ref"]);
    const merges = (ws["!merges"] || []).map((m) => XLSX.utils.encode_range(m));
    const hiddenRows = new Set();
    (ws["!rows"] || []).forEach((r, i) => r && r.hidden && hiddenRows.add(i));
    const hiddenCols = new Set();
    (ws["!cols"] || []).forEach((c, i) => c && c.hidden && hiddenCols.add(i));
    const rows = [];
    let nonEmpty = 0;
    const lastCol = Math.min(range.e.c, range.s.c + MAX_COLS - 1);
    for (let r = range.s.r; r <= range.e.r && rows.length < maxRowsPerSheet; r++) {
      const cells = [];
      for (let c = range.s.c; c <= lastCol; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const t = cellText(cell);
        if (t !== "") {
          let ref = colLetter(c);
          cells.push({ col: ref, text: t, hiddenCol: hiddenCols.has(c) });
        }
      }
      if (cells.length) {
        nonEmpty += cells.length;
        rows.push({ row: r + 1, hidden: hiddenRows.has(r), cells });
      }
    }
    const lines = rows.map((r) => {
      const body = r.cells.map((c) => `${c.col}=${JSON.stringify(c.text)}${c.hiddenCol ? "(hidden col)" : ""}`).join(" | ");
      return `${r.row}${r.hidden ? "(hidden row)" : ""}: ${body}`;
    });
    let header = `### Sheet "${name}"${hidden ? " (HIDDEN SHEET)" : ""} range ${ws["!ref"]}`;
    if (merges.length) header += `\nmerged cells: ${merges.slice(0, 80).join(", ")}${merges.length > 80 ? " ..." : ""}`;
    sheets.push({ name, hidden, rows, nonEmpty, text: header + "\n" + lines.join("\n") });
  });
  return { kind: "spreadsheet", filename, sheets };
}

export function renderCsv(text, { filename = "" } = {}) {
  const wb = XLSX.read(text, { type: "string", raw: false });
  const ws = wb.Sheets[wb.SheetNames[0]];
  // Reuse workbook path for identical formatting.
  const out = XLSX.write(wb, { type: "array", bookType: "xlsx" });
  const r = renderWorkbook(out, { filename });
  r.kind = "csv";
  if (r.sheets[0]) r.sheets[0].name = filename.replace(/\.csv$/i, "") || "csv";
  return r;
}

// ---------- PDF ----------
// Rebuild visual lines from positioned text items. Large horizontal gaps
// become " | " so columns of a comp table survive as separated fields.
export function linesFromItems(items) {
  const toks = items
    .filter((it) => it.str && it.str.trim())
    .map((it) => ({ x: it.transform[4], y: it.transform[5], w: it.width || 0, h: Math.abs(it.transform[3]) || it.height || 8, s: it.str }));
  if (!toks.length) return [];
  toks.sort((a, b) => b.y - a.y || a.x - b.x);
  const lines = [];
  for (const t of toks) {
    const tol = Math.max(2, t.h * 0.45);
    let line = lines.find((l) => Math.abs(l.y - t.y) <= tol);
    if (!line) {
      line = { y: t.y, toks: [] };
      lines.push(line);
    }
    line.toks.push(t);
  }
  lines.sort((a, b) => b.y - a.y);
  return lines.map((l) => {
    l.toks.sort((a, b) => a.x - b.x);
    let s = "";
    let prevEnd = null;
    for (const t of l.toks) {
      if (prevEnd != null) {
        const gap = t.x - prevEnd;
        const charW = t.h * 0.5;
        if (gap > charW * 2.2) s += " | ";
        else if (gap > charW * 0.25 && !s.endsWith(" ") && !t.s.startsWith(" ")) s += " ";
      }
      s += t.s;
      prevEnd = t.x + t.w;
    }
    return s.replace(/\s+/g, " ").trim();
  });
}

export async function renderPdf(buf, { filename = "", maxPages = 40 } = {}) {
  const { getDocumentProxy } = await import("unpdf");
  const pdf = await getDocumentProxy(new Uint8Array(buf));
  const pages = [];
  const n = Math.min(pdf.numPages, maxPages);
  for (let i = 1; i <= n; i++) {
    const page = await pdf.getPage(i);
    const tc = await page.getTextContent();
    const lines = linesFromItems(tc.items);
    pages.push({ page: i, lines });
  }
  const sheets = pages.map((p) => ({
    name: `page ${p.page}`,
    hidden: false,
    rows: p.lines.map((t, i) => ({ row: i + 1, cells: [{ col: "L", text: t }] })),
    nonEmpty: p.lines.length,
    text: `### PDF page ${p.page}\n` + p.lines.map((t, i) => `${i + 1}: ${t}`).join("\n"),
  }));
  const textChars = pages.reduce((a, p) => a + p.lines.join("").length, 0);
  return { kind: "pdf", filename, sheets, numPages: pdf.numPages, scanned: textChars < 200 };
}

// Pasted text (Slack / email comp blurbs): one "page" of numbered lines, read
// by the same PDF-style extraction prompt.
export function renderText(text, { filename = "pasted.txt" } = {}) {
  const lines = String(text).replace(/\r\n?/g, "\n").split("\n").map((l) => l.replace(/\s+/g, " ").trim());
  const rows = [];
  lines.forEach((t, i) => { if (t) rows.push({ row: i + 1, cells: [{ col: "L", text: t }] }); });
  return { kind: "text", filename, sheets: [{ name: "page 1", hidden: false, rows, nonEmpty: rows.length, text: "### Pasted text\n" + rows.map((r) => `${r.row}: ${r.cells[0].text}`).join("\n") }] };
}

export async function renderDocument(buf, filename) {
  const lower = filename.toLowerCase();
  if (lower.endsWith(".pdf")) return renderPdf(buf, { filename });
  if (lower.endsWith(".txt")) return renderText(new TextDecoder().decode(buf), { filename });
  if (lower.endsWith(".csv")) return renderCsv(new TextDecoder().decode(buf), { filename });
  return renderWorkbook(buf, { filename });
}
