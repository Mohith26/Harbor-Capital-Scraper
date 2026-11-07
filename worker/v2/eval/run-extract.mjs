// Local harness: extract corpus files with the v2 LLM pipeline and save JSON.
// usage: node worker/v2/eval/run-extract.mjs <outdir> <file> [<file>...]
import fs from "node:fs";
import path from "node:path";
import { renderDocument } from "../render.js";
import { makeLLM, pool } from "../llm.js";
import { extractDocument } from "../extract.js";
import { validateRecord } from "../validate.js";

const envFile = process.env.LLM_ENV || "/Users/mohithgajjela/.aside/u/0/sessions/2026-11-06_nkAXoYZwSAI5I8N3/tmp/.llm.env";
const env = { ...process.env };
for (const line of fs.readFileSync(envFile, "utf8").split("\n")) {
  const m = /^([A-Z_]+)=(.*)$/.exec(line.trim());
  if (m) env[m[1]] = m[2];
}
const [outdir, ...files] = process.argv.slice(2);
fs.mkdirSync(outdir, { recursive: true });

await pool(files, Number(env.FILE_CONCURRENCY || 3), async (f) => {
  const name = path.basename(f);
  const buf = fs.readFileSync(f);
  const doc = await renderDocument(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength), name);
  const llm = makeLLM(env);
  try {
    const res = await extractDocument(doc, llm, { surveyTier: env.SURVEY_TIER, surveyReasoning: env.SURVEY_REASONING, extractTier: env.EXTRACT_TIER, extractReasoning: env.EXTRACT_REASONING, review: env.REVIEW !== "0" });
    const records = res.records.map((r) => validateRecord(r));
    fs.writeFileSync(path.join(outdir, name + ".json"), JSON.stringify({ file: name, survey: res.survey, records, errors: res.errors, usage: llm.usage, ms: res.ms }, null, 1));
    const flags = records.reduce((a, r) => a + r._flags.length, 0);
    console.log(`${name}: ${records.length} records, ${flags} flags, ${res.jobs} chunks, ${(res.ms / 1000).toFixed(1)}s, tokens ${llm.usage.prompt_tokens}/${llm.usage.completion_tokens}${res.errors.length ? " ERR " + res.errors[0] : ""}`);
  } catch (e) {
    console.log(`${name}: FAILED ${e.message}`);
  }
});
