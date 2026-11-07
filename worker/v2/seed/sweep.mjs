import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "content-type": "application/json" };
const dry = process.argv[2] !== "apply";
let after = 0, acts = [];
for (;;) {
  const r = await fetch("https://comps.harborcap.dev/api/v2/admin/dedupe-sweep", { method: "POST", headers: H, body: JSON.stringify({ after, dry }) }).then((x) => x.json());
  if (r.error) { console.log("ERR", r.error); break; }
  acts.push(...r.actions);
  if (!r.last || r.scanned === 0) break;
  after = r.last;
}
const props = acts.filter((a) => a.merge_property), deals = acts.filter((a) => a.duplicate_comp);
console.log(dry ? "DRY" : "APPLIED", "property merges", props.length, "deal duplicates", deals.length);
fs.writeFileSync(process.argv[3] || "/tmp/sweep.json", JSON.stringify(acts, null, 1));
for (const a of props.slice(0, 25)) console.log(" P", a.a, "<=", a.b, a.reasons.join(", "));
for (const a of deals.slice(0, 15)) console.log(" D", a.duplicate_comp, "of", a.of, a.reasons.join(", "));
