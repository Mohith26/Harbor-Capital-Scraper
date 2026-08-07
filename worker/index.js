import { Container, getContainer } from "@cloudflare/containers";

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

export default {
  async fetch(request, env) {
    // Cheap liveness probe that does not need to wake the container.
    const url = new URL(request.url);
    if (url.pathname === "/__worker-health") {
      return new Response("ok", { status: 200 });
    }

    const container = getContainer(env.COMP_DATABASE, "singleton");
    return container.fetch(request);
  },
};
