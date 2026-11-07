import { Container, getContainer } from "@cloudflare/containers";
import { handleApi, UI_HTML, enrichProperty } from "./v2/api.js";
import { checkDeals } from "./v2/deals.js";
import { checkSaved } from "./v2/library.js";
import { receiveEmail, runQueuedJobs } from "./v2/inbox.js";
import LOGO from "./v2/logo-white.png";
import FAVICON from "./v2/favicon-32.png";
import TOUCH from "./v2/apple-touch-icon.png";
import { LOGO_BLACK } from "./v2/api.js";

/**
 * Wraps the FastAPI comp-database image.
 *
 * A single long-lived instance is used rather than one per visitor: the app
 * keeps per-upload mapping state in process memory, so two analysts working at
 * the same time must land on the same container.
 */
export class CompDatabaseContainer extends Container {
  defaultPort = 8501;

  // Uploading and LLM-mapping a large spreadsheet can take a while, so keep the
  // instance warm well past the 10 minute default.
  sleepAfter = "45m";

  constructor(ctx, env) {
    super(ctx, env);
    // Secrets live as Worker secrets (`wrangler secret put`) and are handed to
    // the container as environment variables at start time, so nothing
    // sensitive is baked into the image.
    this.envVars = {
      CLOUDFLARE_ACCOUNT_ID: env.CLOUDFLARE_ACCOUNT_ID ?? "",
      D1_DATABASE_ID: env.D1_DATABASE_ID ?? "",
      CLOUDFLARE_API_TOKEN: env.CLOUDFLARE_API_TOKEN ?? "",
      OPENAI_API_KEY: env.OPENAI_API_KEY ?? "",
      GOOGLE_API_KEY: env.GOOGLE_API_KEY ?? "",
      SECRET_KEY: env.SECRET_KEY ?? "",
      CF_ACCESS_TEAM_DOMAIN: env.CF_ACCESS_TEAM_DOMAIN ?? "",
      CF_ACCESS_AUD: env.CF_ACCESS_AUD ?? "",
      COOKIE_SECURE: "true",
      PORT: "8501",
    };
  }

  onStart() {
    console.log("comp database container started");
  }

  onStop(stopParams) {
    console.log(`comp database container stopped (${stopParams?.reason ?? "unknown"})`);
  }

  onError(error) {
    console.error("comp database container error:", error);
    throw error;
  }
}

const V2_PAGES = new Set(["/", "/app", "/find", "/v2"]);

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/__worker-health") return new Response("ok", { status: 200 });
    // v2: LLM extraction pipeline, property/feature store and comp finder run
    // in the Worker itself (D1 + R2); the legacy FastAPI app stays in the container.
    if (url.pathname.startsWith("/api/v2/")) return handleApi(request, env, ctx);
    if (url.pathname === "/favicon-32.png" || url.pathname === "/favicon.ico") return new Response(FAVICON, { headers: { "content-type": "image/png", "cache-control": "public, max-age=86400" } });
    if (url.pathname === "/apple-touch-icon.png") return new Response(TOUCH, { headers: { "content-type": "image/png", "cache-control": "public, max-age=86400" } });
    if (url.pathname === "/logo-black.svg") return new Response(LOGO_BLACK, { headers: { "content-type": "image/svg+xml", "cache-control": "public, max-age=86400" } });
    if (url.pathname === "/logo-white.png") return new Response(LOGO, { headers: { "content-type": "image/png", "cache-control": "public, max-age=86400" } });
    // The classic (legacy login) database and analytics pages are retired: the Library replaces them.
    if (request.method === "GET" && ["/database", "/analytics", "/login"].includes(url.pathname)) return Response.redirect(`${url.origin}/#lib`, 302);
    if (url.pathname === "/mcp") return handleApi(new Request(`${url.origin}/api/v2/mcp`, request), env, ctx);
    // The old finder and upload pages are replaced by the v2 app.
    if (request.method === "GET" && (url.pathname === "/finder" || url.pathname === "/upload")) {
      return Response.redirect(`${url.origin}/#${url.pathname === "/finder" ? "find" : "up"}`, 302);
    }
    if (V2_PAGES.has(url.pathname)) {
      return new Response(UI_HTML, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" } });
    }
    const container = getContainer(env.COMP_DATABASE, "singleton");
    return container.fetch(request);
  },

  // comps@harborcap.dev (Cloudflare Email Routing) -> Inbox.
  async email(message, env, ctx) {
    try {
      const r = await receiveEmail(message, env, ctx, handleApi);
      console.log("inbox email", JSON.stringify(r));
    } catch (e) {
      console.error("inbox email failed", e);
    }
  },

  // Background work: queued inbox jobs, deal / saved-search watches, feature sourcing.
  async scheduled(event, env, ctx) {
    const db = env.COMPS_DB;
    try { console.log("queued jobs", JSON.stringify(await runQueuedJobs(env, ctx, handleApi))); } catch (e) { console.error("jobs", e); }
    try { console.log("deal watch", JSON.stringify(await checkDeals(env, db))); } catch (e) { console.error("deal watch", e); }
    try { console.log("saved searches", JSON.stringify(await checkSaved(env, db))); } catch (e) { console.error("saved", e); }
    const { results } = await db.prepare("SELECT id FROM v2_properties WHERE enrich_status = 'pending' AND latitude IS NOT NULL ORDER BY id LIMIT 60").all();
    for (const r of results) {
      try {
        await enrichProperty(env, db, r.id, {});
      } catch (e) {
        await db.prepare("UPDATE v2_properties SET enrich_status = 'failed' WHERE id = ?").bind(r.id).run();
      }
    }
  },
};
