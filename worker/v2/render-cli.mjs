import fs from "node:fs";
import path from "node:path";
import { renderDocument } from "./render.js";
const f = process.argv[2];
const buf = fs.readFileSync(f);
const doc = await renderDocument(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength), path.basename(f));
if (process.argv[3] === "--summary") {
  console.log(path.basename(f), doc.kind, doc.sheets.map(s => `${s.name}${s.hidden?'(H)':''}:${s.rows.length}r`).join(", "));
} else {
  console.log(`# ${path.basename(f)} (${doc.kind})`);
  for (const s of doc.sheets) console.log(s.text + "\n");
}
