import fs from "node:fs"; import path from "node:path";
import { validateRecord } from "../validate.js";
const [inDir, outDir] = process.argv.slice(2); fs.mkdirSync(outDir, { recursive: true });
for (const f of fs.readdirSync(inDir).filter((x) => x.endsWith(".json"))) {
  const d = JSON.parse(fs.readFileSync(path.join(inDir, f), "utf8"));
  d.records = d.records.map((r) => validateRecord({ ...r, _flags: (r._flags || []).filter((x) => x.code === "unverified") }));
  fs.writeFileSync(path.join(outDir, f), JSON.stringify(d));
}
