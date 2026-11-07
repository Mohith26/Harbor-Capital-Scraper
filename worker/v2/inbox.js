// Inbox: comps that arrive by email (comps@harborcap.dev, Cloudflare Email
// Routing -> this Worker) are queued and run through the same extraction
// pipeline as browser uploads by the cron. Clean rows commit automatically;
// anything that needs a person is held in the Inbox.

import PostalMime from "postal-mime";

const OK_TYPES = /\.(xlsx|xlsm|xls|csv|pdf|txt)$/i;

export function senderAllowed(env, from) {
  const allow = String(env.INBOX_ALLOW || "@harborcap.com,mohithgajjela@gmail.com").toLowerCase().split(",").map((s) => s.trim()).filter(Boolean);
  const f = String(from || "").toLowerCase();
  return allow.some((a) => (a.startsWith("@") ? f.endsWith(a) : f === a));
}

async function internal(handleApi, env, ctx, path, init) {
  const r = await handleApi(new Request(`https://comps.harborcap.dev${path}`, init), env, ctx);
  const t = await r.text();
  let j;
  try { j = JSON.parse(t); } catch { j = { error: t.slice(0, 200) }; }
  if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
  return j;
}

export async function receiveEmail(message, env, ctx, handleApi) {
  const from = message.from;
  if (!senderAllowed(env, from)) {
    message.setReject?.("Sender not allowed for comps@harborcap.dev");
    return { rejected: from };
  }
  const raw = await new Response(message.raw).arrayBuffer();
  const mail = await PostalMime.parse(raw);
  const subject = mail.subject || "(no subject)";
  const ref = `${from} · ${subject}`.slice(0, 300);
  const files = (mail.attachments || []).filter((a) => OK_TYPES.test(a.filename || "") && a.content);
  const made = [];
  for (const a of files) {
    const form = new FormData();
    form.append("file", new File([a.content], a.filename, { type: a.mimeType || "application/octet-stream" }));
    const up = await internal(handleApi, env, ctx, "/api/v2/upload", { method: "POST", body: form, headers: { "x-harbor-user": from } });
    made.push(up);
  }
  // No attachment: a pasted comp table in the body.
  const body = (mail.text || "").trim();
  if (!files.length && body.length > 80 && /\d/.test(body)) {
    const form = new FormData();
    form.append("text", body.slice(0, 200000));
    form.append("title", subject);
    made.push(await internal(handleApi, env, ctx, "/api/v2/upload", { method: "POST", body: form }));
  }
  const db = env.COMPS_DB;
  for (const m of made) {
    await db.batch([
      db.prepare("UPDATE v2_files SET via = 'email', via_ref = ?, uploaded_by = ? WHERE id = ?").bind(ref, from, m.file_id),
      db.prepare("UPDATE v2_jobs SET status = ?, created_by = ? WHERE id = ?").bind(m.scanned ? "needs_vision" : "queued", from, m.job_id),
    ]);
  }
  return { from, subject, jobs: made.map((m) => m.job_id) };
}

// Cron: run queued jobs end to end (survey -> chunks -> resolve -> commit clean rows).
export async function runQueuedJobs(env, ctx, handleApi, { budgetMs = 8 * 60 * 1000 } = {}) {
  const db = env.COMPS_DB;
  const t0 = Date.now();
  const out = [];
  const { results } = await db.prepare("SELECT id FROM v2_jobs WHERE status IN ('queued','q_extracting') ORDER BY created_at LIMIT 3").all();
  for (const j of results) {
    if (Date.now() - t0 > budgetMs) break;
    try {
      const job = await db.prepare("SELECT * FROM v2_jobs WHERE id = ?").bind(j.id).first();
      let plan = JSON.parse(job.plan_json || "{}");
      if (!plan.survey) {
        await internal(handleApi, env, ctx, `/api/v2/jobs/${j.id}/survey`, { method: "POST" });
        plan = JSON.parse((await db.prepare("SELECT plan_json FROM v2_jobs WHERE id = ?").bind(j.id).first()).plan_json);
      }
      await db.prepare("UPDATE v2_jobs SET status = 'q_extracting' WHERE id = ?").bind(j.id).run();
      const done = new Set((plan.done_chunks || []).map(Number));
      for (let n = 0; n < (plan.chunks || []).length; n++) {
        if (done.has(n)) continue;
        if (Date.now() - t0 > budgetMs) throw new Error("budget");
        await internal(handleApi, env, ctx, `/api/v2/jobs/${j.id}/chunk/${n}`, { method: "POST" });
        done.add(n);
        const cur = JSON.parse((await db.prepare("SELECT plan_json FROM v2_jobs WHERE id = ?").bind(j.id).first()).plan_json);
        cur.done_chunks = [...done];
        await db.prepare("UPDATE v2_jobs SET plan_json = ?, status = 'q_extracting' WHERE id = ?").bind(JSON.stringify(cur), j.id).run();
      }
      await internal(handleApi, env, ctx, `/api/v2/jobs/${j.id}/resolve`, { method: "POST" });
      const c = await internal(handleApi, env, ctx, `/api/v2/jobs/${j.id}/commit`, { method: "POST", body: "{}", headers: { "content-type": "application/json" } });
      out.push({ job: j.id, ...c });
    } catch (e) {
      if (String(e.message) !== "budget") {
        await db.prepare("UPDATE v2_jobs SET status = 'failed', plan_json = json_set(COALESCE(plan_json,'{}'), '$.error', ?) WHERE id = ?").bind(String(e.message).slice(0, 300), j.id).run();
        out.push({ job: j.id, error: e.message });
      }
    }
  }
  return out;
}
