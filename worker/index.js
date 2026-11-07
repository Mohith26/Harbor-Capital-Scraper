import { Container, getContainer } from "@cloudflare/containers";
import { handleApi, UI_HTML, enrichProperty } from "./v2/api.js";

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
    if (V2_PAGES.has(url.pathname)) {
      return new Response(UI_HTML, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" } });
    }
    const container = getContainer(env.COMP_DATABASE, "singleton");
    return container.fetch(request);
  },

  // Background feature sourcing for properties that have not been enriched yet.
  async scheduled(event, env, ctx) {
    const db = env.COMPS_DB;
    const { results } = await db.prepare("SELECT id FROM v2_properties WHERE enrich_status = 'pending' AND latitude IS NOT NULL ORDER BY id LIMIT 25").all();
    for (const r of results) {
      try {
        await enrichProperty(env, db, r.id, {});
      } catch (e) {
        await db.prepare("UPDATE v2_properties SET enrich_status = 'failed' WHERE id = ?").bind(r.id).run();
      }
    }
  },
};
