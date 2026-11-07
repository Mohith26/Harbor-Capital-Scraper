// Re-validate held records of every job and commit the ones that now pass.
import fs from "node:fs";
const tok = JSON.parse(fs.readFileSync("/Users/mohithgajjela/.aside/u/0/credentials/harbor-access-service-token.json", "utf8"));
const H = { "CF-Access-Client-Id": tok.client_id, "CF-Access-Client-Secret": tok.client_secret, "x-harbor-user": "seed@harborcap.com", "content-type": "application/json" };
const B = "https://comps.harborcap.dev";
const j = async (p, o = {}) => (await fetch(B + p, { ...o, headers: H })).json();
const { jobs } = await j("/api/v2/admin/jobs");
for (const job of jobs.filter((x) => x.held > 0 && x.status !== "extracting" && x.status !== "surveying")) {
  const r = await j(`/api/v2/jobs/${job.id}/resolve`, { method: "POST" });
  const c = await j(`/api/v2/jobs/${job.id}/commit`, { method: "POST", body: "{}" });
  console.log(`${job.filename}: held ${job.held} -> added ${c.added}, merged ${c.merged}, still held ${c.review_left}`);
}
