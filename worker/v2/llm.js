// Provider-agnostic chat client (OpenAI-compatible: DeepSeek, OpenAI).
// Returns parsed JSON for json-mode calls, with retries and usage accounting.

const PROVIDERS = {
  deepseek: { base: "https://api.deepseek.com", keyEnv: "DEEPSEEK_API_KEY" },
  openai: { base: "https://api.openai.com/v1", keyEnv: "OPENAI_API_KEY" },
};

// Providers that returned "no credits / quota exhausted" are skipped for a while
// (per isolate) instead of burning ~90 s of retries on every call.
const disabledUntil = {};
export function providerAvailable(p) {
  return !(disabledUntil[p] && disabledUntil[p] > Date.now());
}

export function makeLLM(env, opts = {}) {
  let provider = opts.provider || env.LLM_PROVIDER || (env.DEEPSEEK_API_KEY ? "deepseek" : "openai");
  if (!opts.noFallback && !providerAvailable(provider)) {
    const other = provider === "openai" ? "deepseek" : "openai";
    if (env[PROVIDERS[other].keyEnv] && providerAvailable(other)) provider = other;
  }
  const p = PROVIDERS[provider];
  if (!p) throw new Error(`unknown LLM provider ${provider}`);
  const key = env[p.keyEnv];
  if (!key) throw new Error(`${p.keyEnv} is not configured`);
  const envModel = (v, re) => (v && re.test(v) ? v : null);
  const models = {
    deepseek: { strong: envModel(env.DEEPSEEK_MODEL_STRONG, /^deepseek/) || envModel(env.LLM_MODEL_STRONG, /^deepseek/) || "deepseek-flash", fast: envModel(env.LLM_MODEL_FAST, /^deepseek/) || "deepseek-flash", vision: "deepseek-flash" },
    openai: { strong: envModel(env.LLM_MODEL_STRONG, /^(gpt|o\d)/) || "gpt-5.5", fast: envModel(env.LLM_MODEL_FAST, /^(gpt|o\d)/) || "gpt-5.4-mini", vision: envModel(env.LLM_MODEL_VISION, /^(gpt|o\d)/) || "gpt-5.5" },
  }[provider];
  if (opts.strong && (provider === "deepseek" ? /^deepseek/.test(opts.strong) : !/^deepseek/.test(opts.strong))) models.strong = opts.strong;
  if (opts.fast) models.fast = opts.fast;
  const usage = { calls: 0, prompt_tokens: 0, completion_tokens: 0, ms: 0 };

  async function chat({ system, user, tier = "strong", json = true, maxTokens, temperature = 0, images = [], timeoutMs = 240000, reasoning = "default" }) {
    const model = models[tier] || tier;
    const content = images.length
      ? [{ type: "text", text: user }, ...images.map((url) => ({ type: "image_url", image_url: { url } }))]
      : user;
    const body = {
      model,
      messages: [
        ...(system ? [{ role: "system", content: system }] : []),
        { role: "user", content },
      ],
      temperature,
    };
    if (json) body.response_format = { type: "json_object" };
    if (provider === "deepseek") {
      if (reasoning === "off") body.thinking = { type: "disabled" };
      else if (reasoning === "low" || reasoning === "high" || reasoning === "max") body.reasoning_effort = reasoning;
    } else if (provider === "openai" && /^(o\d|gpt-[5-9])/.test(model) && reasoning !== "default") {
      body.reasoning_effort = reasoning === "off" ? (/^gpt-5(-|$)/.test(model) ? "minimal" : "none") : reasoning;
      delete body.temperature;
    }
    if (maxTokens) body[provider === "openai" ? "max_completion_tokens" : "max_tokens"] = maxTokens;
    let lastErr;
    for (let attempt = 0; attempt < 6; attempt++) {
      const t0 = Date.now();
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        const res = await fetch(`${p.base}/chat/completions`, {
          method: "POST",
          headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
          body: JSON.stringify(body),
          signal: ctrl.signal,
        });
        clearTimeout(timer);
        if (res.status === 429 || res.status >= 500) {
          const bodyText = await res.text();
          lastErr = new Error(`LLM ${res.status}: ${bodyText.slice(0, 300)}`);
          if (/insufficient_quota|credit_balance_exhausted|billing/i.test(bodyText)) {
            disabledUntil[provider] = Date.now() + 10 * 60 * 1000;
            lastErr.quota = true;
            break;
          }
          const ra = Number(res.headers.get("retry-after")) || 0;
          const wait = Math.max(ra * 1000, 2000 * 2 ** attempt) + Math.random() * 1000;
          await new Promise((r) => setTimeout(r, Math.min(wait, 30000)));
          continue;
        }
        if (!res.ok) {
          const bt = await res.text();
          const err = new Error(`LLM ${res.status}: ${bt.slice(0, 500)}`);
          if (res.status === 402 || /insufficient|credit|balance|quota/i.test(bt)) {
            disabledUntil[provider] = Date.now() + 10 * 60 * 1000;
            err.quota = true;
            lastErr = err;
            break;
          }
          throw err;
        }
        const data = await res.json();
        usage.calls++;
        usage.ms += Date.now() - t0;
        usage.prompt_tokens += data.usage?.prompt_tokens || 0;
        usage.completion_tokens += data.usage?.completion_tokens || 0;
        const text = data.choices?.[0]?.message?.content || "";
        if (!json) return text;
        try {
          return parseJsonLoose(text);
        } catch (e) {
          lastErr = new Error(`bad JSON from model: ${text.slice(0, 200)}`);
          continue;
        }
      } catch (e) {
        clearTimeout(timer);
        lastErr = e;
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
      }
    }
    throw lastErr;
  }

  // If the primary provider keeps failing (rate limits / outage), retry the
  // same request once on the other configured provider.
  async function chatWithFallback(args) {
    try {
      return await chat(args);
    } catch (e) {
      const other = provider === "openai" ? "deepseek" : "openai";
      if (opts.noFallback || !env[PROVIDERS[other].keyEnv] || !providerAvailable(other)) {
        if (e.quota) { const q = new Error("AI_CREDITS_EXHAUSTED: the AI provider account is out of credits - extraction is paused until it is topped up"); q.quota = true; throw q; }
        throw e;
      }
      const alt = makeLLM(env, { provider: other, noFallback: true });
      try {
        const out = await alt.chat({ ...args, tier: args.tier === "vision" && other === "deepseek" ? "vision" : args.tier });
        usage.fallbacks = (usage.fallbacks || 0) + 1;
        return out;
      } catch (e2) {
        if (e.quota && e2.quota) { const q = new Error("AI_CREDITS_EXHAUSTED: both AI provider accounts (DeepSeek and OpenAI) are out of credits - extraction is paused until one is topped up"); q.quota = true; throw q; }
        throw new Error(`${provider}: ${String(e.message || e).slice(0, 200)} | ${other}: ${String(e2.message || e2).slice(0, 120)}`);
      }
    }
  }

  return { chat: chatWithFallback, usage, provider, models };
}

export function parseJsonLoose(text) {
  let t = String(text).trim();
  t = t.replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/i, "");
  try {
    return JSON.parse(t);
  } catch {
    const a = t.indexOf("{");
    const b = t.lastIndexOf("}");
    if (a >= 0 && b > a) return JSON.parse(t.slice(a, b + 1));
    throw new Error("unparseable JSON");
  }
}

// Run async tasks with bounded concurrency, preserving order.
export async function pool(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return out;
}
