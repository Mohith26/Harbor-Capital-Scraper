import fs from "node:fs";
import { getDocumentProxy } from "unpdf";
const f = process.argv[2];
const pdf = await getDocumentProxy(new Uint8Array(fs.readFileSync(f)));
for (let i=1;i<=pdf.numPages;i++){ const p=await pdf.getPage(i); const tc=await p.getTextContent();
  console.log(`### page ${i}`);
  // group by x (rotated pages) or y
  const items=tc.items.filter(t=>t.str.trim()).map(t=>({s:t.str,x:Math.round(t.transform[4]),y:Math.round(t.transform[5]),r:Math.round(Math.atan2(t.transform[1],t.transform[0])*57)}));
  console.log(JSON.stringify(items));
}
