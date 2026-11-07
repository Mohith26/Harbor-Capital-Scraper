// Read-only MCP endpoint (Streamable HTTP, JSON responses) so Claude / HC Brain,
// Jarvis or Excel agents can query Harbor's comps and deal comp sets.
// Auth: Cloudflare Access (user or service token) in front of the whole host.
// If MCP_TOKEN is set, a matching "Authorization: Bearer" header is also accepted.

import { findComps } from "./finder.js";
import { makeLLM } from "./llm.js";

const VERSION = "2025-06-18";
const rpc = (id, result) => new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), { headers: { "content-type": "application/json" } });
const rpcErr = (id, code, message) => new Response(JSON.stringify({ jsonrpc: "2.0", id, error: { code, message } }), { headers: { "content-type": "application/json" } });

const TOOLS = [
  {
    name: "search_comps",
    description: "Find Texas industrial sale / lease comps in Harbor Capital's comp database from a plain-English request (deal description, address, submarket, size band, product type like crane-served, IOS, shallow bay, recency). Returns ranked comps with source file, date, size, $/SF, cap rate, rent and match score.",
    inputSchema: { type: "object", properties: { query: { type: "string" }, limit: { type: "number", default: 15 } }, required: ["query"] },
  },
  {
    name: "list_deals",
    description: "List Harbor deals that have comp sets (name, address, market, number of sale and lease comps chosen).",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "get_deal_comp_set",
    description: "Get one deal's subject facts (with sources) and the comps the team chose for it, with low / median / high.",
    inputSchema: { type: "object", properties: { deal_id: { type: "number" } }, required: ["deal_id"] },
  },
  {
    name: "market_stats",
    description: "Median price/SF, cap rate and rent for active comps, filtered by market (Houston, DFW, San Antonio, Austin, Laredo), kind (sale|lease), product tag (crane, ios, shallow_bay, bulk, manufacturing...) and a since date.",
    inputSchema: { type: "object", properties: { market: { type: "string" }, kind: { type: "string" }, tag: { type: "string" }, since: { type: "string", description: "YYYY-MM-DD" } } },
  },
];

const median = (a) => { const v = a.filter((x) => x != null).sort((x, y) => x - y); if (!v.length) return null; const m = v.length >> 1; return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2; };
const compact = (r) => ({ id: r.id, kind: r.record_kind, date: r.txn_date, address: r.address || r.property_name, city: r.city, submarket: r.submarket, sf: r.size_sf ?? r.building_sf ?? r.leased_sf, year_built: r.year_built, clear_ft: r.clear_height_ft, price: r.sale_price, price_psf: r.price_psf, cap_rate_pct: r.cap_rate_pct, buyer: r.buyer, seller: r.seller, tenant: r.tenant, rent_psf_yr: r.rent_psf_yr, rent_per_acre_mo: r.rent_per_acre_mo, price_per_acre: r.price_per_acre, land_acres: r.land_acres, lease_basis: r.rent_basis || r.lease_structure, term_months: r.term_months, source: r.source_label, score: r.score, distance_mi: r.distance_mi, tags: String(r.product_tags || "").split("|").filter(Boolean) });

async function callTool(env, name, args, base) {
  const db = env.COMPS_DB;
  if (name === "search_comps") {
    const llm = (() => { try { return makeLLM(env, { provider: "deepseek", noFallback: true }); } catch { return null; } })();
    const res = await findComps(env, db, llm, { query: String(args.query || ""), limit: Math.min(Number(args.limit) || 15, 40) });
    return { interpretation: res.parsed?.interpretation || null, location: res.location?.label || null, matched: res.matched, stats: res.stats, comps: res.results.map(compact), link: `${base}/#find` };
  }
  if (name === "list_deals") {
    const { results } = await db.prepare("SELECT d.id, d.name, d.address, d.market, (SELECT count(*) FROM v2_set_items i WHERE i.deal_id = d.id AND i.state = 'add') AS comps FROM v2_deals d WHERE d.status = 'active' ORDER BY d.updated_at DESC LIMIT 100").all();
    return { deals: results.map((d) => ({ ...d, link: `${base}/#deal/${d.id}` })) };
  }
  if (name === "get_deal_comp_set") {
    const d = await db.prepare("SELECT * FROM v2_deals WHERE id = ?").bind(Number(args.deal_id)).first();
    if (!d) throw new Error("deal not found");
    const { results } = await db.prepare(`SELECT i.note, c.*, p.address, p.city, p.submarket, p.building_sf, p.year_built, p.clear_height_ft, p.product_tags FROM v2_set_items i JOIN v2_comps c ON c.id = i.comp_id JOIN v2_properties p ON p.id = c.property_id WHERE i.deal_id = ? AND i.state = 'add' ORDER BY i.position`).bind(d.id).all();
    const sales = results.filter((r) => r.record_kind === "sale" || r.record_kind === "land_sale");
    const leases = results.filter((r) => r.record_kind === "lease" || r.record_kind === "listing");
    const subj = JSON.parse(d.subject_json || "{}");
    return {
      deal: { id: d.id, name: d.name, address: d.address, market: d.market, link: `${base}/#deal/${d.id}` },
      subject: Object.fromEntries(Object.entries(subj.facts || {}).map(([k, v]) => [k, { value: v.value, source: v.source }])),
      sales: { n: sales.length, median_price_psf: median(sales.map((r) => r.price_psf)), low: Math.min(...sales.map((r) => r.price_psf).filter(Boolean)), high: Math.max(...sales.map((r) => r.price_psf).filter(Boolean)), median_cap: median(sales.map((r) => r.cap_rate_pct)), comps: sales.map((r) => ({ ...compact(r), note: r.note })) },
      leases: { n: leases.length, median_rent_psf_yr: median(leases.map((r) => r.rent_psf_yr)), comps: leases.map((r) => ({ ...compact(r), note: r.note })) },
    };
  }
  if (name === "market_stats") {
    const where = ["c.status = 'active'"];
    const b = [];
    if (args.market) { where.push("p.market = ?"); b.push(args.market); }
    if (args.kind) { where.push(args.kind === "sale" ? "c.record_kind IN ('sale','land_sale')" : "c.record_kind = ?"); if (args.kind !== "sale") b.push(args.kind); }
    if (args.tag) { where.push("p.product_tags LIKE ?"); b.push(`%|${args.tag}|%`); }
    if (args.since) { where.push("c.txn_date_sort >= ?"); b.push(args.since); }
    const { results } = await db.prepare(`SELECT c.price_psf, c.cap_rate_pct, c.rent_psf_yr FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE ${where.join(" AND ")} LIMIT 20000`).bind(...b).all();
    return { n: results.length, median_price_psf: median(results.map((r) => r.price_psf)), median_cap_rate_pct: median(results.map((r) => r.cap_rate_pct)), median_rent_psf_yr: median(results.map((r) => r.rent_psf_yr)), n_with_price: results.filter((r) => r.price_psf).length, n_with_cap: results.filter((r) => r.cap_rate_pct).length, n_with_rent: results.filter((r) => r.rent_psf_yr).length };
  }
  throw new Error(`unknown tool ${name}`);
}

export async function handleMcp(req, env, ctx, user) {
  const base = new URL(req.url).origin;
  if (req.method === "GET") return new Response(JSON.stringify({ name: "harbor-comps", tools: TOOLS.map((t) => t.name), transport: "streamable-http (POST JSON-RPC)" }), { headers: { "content-type": "application/json" } });
  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });
  if ((!user || user === "unknown") && env.MCP_TOKEN && req.headers.get("authorization") !== `Bearer ${env.MCP_TOKEN}`) return new Response("unauthorized", { status: 401 });
  let msg;
  try { msg = await req.json(); } catch { return rpcErr(null, -32700, "parse error"); }
  if (Array.isArray(msg)) return rpcErr(null, -32600, "batching not supported");
  const { id, method, params } = msg;
  if (id === undefined) return new Response(null, { status: 202 });
  if (method === "initialize") return rpc(id, { protocolVersion: params?.protocolVersion || VERSION, capabilities: { tools: {} }, serverInfo: { name: "harbor-comps", version: "2.0.0" }, instructions: "Harbor Capital comp database (Texas industrial). Use search_comps for comps, get_deal_comp_set for comps already chosen for a deal. Every comp carries its source file; cite it." });
  if (method === "ping") return rpc(id, {});
  if (method === "tools/list") return rpc(id, { tools: TOOLS });
  if (method === "tools/call") {
    try {
      const out = await callTool(env, params?.name, params?.arguments || {}, base);
      return rpc(id, { content: [{ type: "text", text: JSON.stringify(out) }], structuredContent: out });
    } catch (e) {
      return rpc(id, { content: [{ type: "text", text: `Error: ${e.message}` }], isError: true });
    }
  }
  return rpcErr(id, -32601, "method not found");
}
