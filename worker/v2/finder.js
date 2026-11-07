// Comp Finder: deal description and/or location -> ranked, explained comps.
//
// 1. parse    LLM turns analyst shorthand ("45k SF rear-load tilt, 28' clear,
//             NW Houston off 290, sale comps last 2 yrs") into a structured
//             subject + constraints. Deterministic regex fallback if the LLM fails.
// 2. locate   address -> geocode; submarket / corridor names -> gazetteer
//             (CoStar submarket centroids + Harbor's own aliases); anything
//             else -> geocode the phrase in Texas.
// 3. retrieve SQL candidates around the location (or the market).
// 4. score    transparent per-factor similarity, hard filters only where the
//             analyst was explicit.
// 5. explain  optional LLM pass writes a one-line "why / adjust" per top comp.

import { geocode, haversineMi, bbox, marketFor, MARKETS } from "./geo.js";
import { dateToNumber } from "./validate.js";
import { matchCostar, costarObservations, parcelObservations, resolveFeatures } from "./enrich.js";

// When the analyst gives a subject address but not its specs, look the building
// up (our DB -> CoStar index -> county appraisal) and use its size / vintage /
// clear height / product as soft targets. Explicit values always win.
export async function describeSubject(db, loc, parsed) {
  if (!loc || !["address", "pin"].includes(loc.kind) || (loc.kind === "address" && !["rooftop", "interpolated", "parcel", "source"].includes(loc.quality))) return null;
  const probe = { latitude: loc.lat, longitude: loc.lng, address: parsed.subject?.address || null };
  const obs = [];
  const b = bbox(loc.lat, loc.lng, 0.05);
  const own = await db.prepare("SELECT * FROM v2_properties WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ? LIMIT 5").bind(b.minLat, b.maxLat, b.minLng, b.maxLng).all().catch(() => ({ results: [] }));
  let ownId = null;
  for (const p of own.results || []) {
    if (haversineMi(loc.lat, loc.lng, p.latitude, p.longitude) > 0.03) continue;
    ownId = ownId || p.id;
    for (const f of ["building_sf", "year_built", "clear_height_ft", "property_subtype", "loading", "construction", "land_acres", "dock_doors", "drive_in_doors", "cranes", "power", "building_class", "tenancy", "yard_acres", "rail_served"]) if (p[f] != null) obs.push({ field: f, value: p[f], source: "broker_sheet", source_ref: `comp DB property #${p.id}` });
  }
  const cs = await matchCostar(db, probe).catch(() => null);
  if (cs) obs.push(...costarObservations(cs.row));
  const pc = await parcelObservations(db, probe).catch(() => ({ obs: [] }));
  obs.push(...pc.obs.filter((o) => ["building_sf", "year_built", "land_acres", "property_subtype"].includes(o.field)));
  if (!obs.length) return null;
  const { best } = resolveFeatures(obs);
  const v = (f) => best[f]?.value ?? null;
  return { property_id: ownId, detail: Object.fromEntries(Object.entries(best).map(([k, x]) => [k, { value: x.value, source: x.source, ref: x.source_ref || null }])), building_sf: v("building_sf"), year_built: v("year_built"), clear_height_ft: v("clear_height_ft"), property_subtype: v("property_subtype"), loading: v("loading"), construction: v("construction"), land_acres: v("land_acres"), sources: [...new Set(Object.values(best).map((x) => x.source))] };
}

// Harbor vocabulary -> gazetteer aliases (from Slack usage). Values are
// [market, lat, lng, radius_mi].
export const ALIASES = {
  // Houston
  "nw houston": ["Houston", 29.88, -95.55, 8], northwest: ["Houston", 29.88, -95.55, 8], "290": ["Houston", 29.9, -95.58, 6], "290 corridor": ["Houston", 29.9, -95.58, 6], "hwy 290": ["Houston", 29.9, -95.58, 6],
  "beltway 8": ["Houston", 29.76, -95.37, 16], bw8: ["Houston", 29.76, -95.37, 16], "grand parkway": ["Houston", 29.85, -95.75, 10], "grand pkwy": ["Houston", 29.85, -95.75, 10],
  katy: ["Houston", 29.79, -95.82, 6], brookshire: ["Houston", 29.79, -95.95, 6], "far west houston": ["Houston", 29.8, -95.75, 8], "west houston": ["Houston", 29.8, -95.62, 8],
  humble: ["Houston", 29.99, -95.26, 5], spring: ["Houston", 30.05, -95.42, 6], aldine: ["Houston", 29.93, -95.35, 5], conroe: ["Houston", 30.31, -95.46, 7], "the woodlands": ["Houston", 30.17, -95.47, 6],
  baytown: ["Houston", 29.75, -94.98, 6], "cedar port": ["Houston", 29.72, -94.95, 4], cypress: ["Houston", 29.96, -95.67, 6], tomball: ["Houston", 30.08, -95.62, 5], brittmoore: ["Houston", 29.83, -95.56, 3],
  "north houston": ["Houston", 29.92, -95.4, 7], "ne houston": ["Houston", 29.85, -95.25, 7], northeast: ["Houston", 29.85, -95.25, 7], "se houston": ["Houston", 29.66, -95.2, 8], southeast: ["Houston", 29.66, -95.2, 8],
  pasadena: ["Houston", 29.68, -95.17, 5], "ship channel": ["Houston", 29.73, -95.15, 6], "sw houston": ["Houston", 29.66, -95.55, 7], southwest: ["Houston", 29.66, -95.55, 7], stafford: ["Houston", 29.62, -95.57, 4],
  "south belt": ["Houston", 29.6, -95.25, 5], southbelt: ["Houston", 29.6, -95.25, 5], pearland: ["Houston", 29.56, -95.29, 5], channelview: ["Houston", 29.78, -95.12, 4], "generation park": ["Houston", 29.92, -95.17, 3],
  greens: ["Houston", 29.94, -95.38, 4], iah: ["Houston", 29.97, -95.34, 5], jacintoport: ["Houston", 29.76, -95.1, 3], "610": ["Houston", 29.76, -95.37, 8], "inner loop": ["Houston", 29.76, -95.37, 7], "hwy 90": ["Houston", 29.83, -95.18, 6],
  // DFW
  "great southwest": ["DFW", 32.77, -97.05, 5], gsw: ["DFW", 32.77, -97.05, 5], "upper gsw": ["DFW", 32.8, -97.04, 3.5], "lower gsw": ["DFW", 32.73, -97.07, 4], arlington: ["DFW", 32.7, -97.11, 6], "grand prairie": ["DFW", 32.72, -97.0, 5],
  brookhollow: ["DFW", 32.82, -96.88, 3], stemmons: ["DFW", 32.84, -96.89, 5], "south stemmons": ["DFW", 32.82, -96.88, 4], "north stemmons": ["DFW", 32.92, -96.9, 4], valwood: ["DFW", 32.94, -96.9, 3],
  "east dallas": ["DFW", 32.78, -96.68, 6], mesquite: ["DFW", 32.77, -96.6, 5], forney: ["DFW", 32.75, -96.47, 5], garland: ["DFW", 32.9, -96.64, 5], "south dallas": ["DFW", 32.67, -96.75, 7], "i-20": ["DFW", 32.67, -96.75, 10],
  denton: ["DFW", 33.21, -97.13, 7], alliance: ["DFW", 32.98, -97.32, 6], "ne tarrant": ["DFW", 32.88, -97.25, 6], "north fort worth": ["DFW", 32.88, -97.33, 7], "east fort worth": ["DFW", 32.74, -97.25, 5], "fort worth": ["DFW", 32.75, -97.33, 10],
  "dfw airport": ["DFW", 32.9, -97.04, 5], irving: ["DFW", 32.85, -96.96, 5], "las colinas": ["DFW", 32.88, -96.94, 3], addison: ["DFW", 32.96, -96.83, 3], richardson: ["DFW", 32.95, -96.73, 4], redbird: ["DFW", 32.68, -96.87, 3],
  wilmer: ["DFW", 32.59, -96.68, 4], lancaster: ["DFW", 32.6, -96.76, 4], mckinney: ["DFW", 33.2, -96.62, 6], allen: ["DFW", 33.1, -96.67, 4], plano: ["DFW", 33.02, -96.7, 6], frisco: ["DFW", 33.15, -96.82, 6], "635": ["DFW", 32.9, -96.8, 10], "820": ["DFW", 32.8, -97.2, 10],
  // San Antonio
  "ne san antonio": ["San Antonio", 29.55, -98.35, 6], "northeast san antonio": ["San Antonio", 29.55, -98.35, 6], "tri county": ["San Antonio", 29.58, -98.3, 3], "tri-county": ["San Antonio", 29.58, -98.3, 3], schertz: ["San Antonio", 29.58, -98.27, 4], selma: ["San Antonio", 29.59, -98.31, 3],
  "new braunfels": ["San Antonio", 29.7, -98.12, 6], seguin: ["San Antonio", 29.57, -97.96, 5], kelly: ["San Antonio", 29.38, -98.58, 4], "port san antonio": ["San Antonio", 29.38, -98.58, 4], "sw san antonio": ["San Antonio", 29.35, -98.6, 6], "loop 410": ["San Antonio", 29.47, -98.48, 9], "410": ["San Antonio", 29.47, -98.48, 9], rittiman: ["San Antonio", 29.49, -98.39, 3], "pop gunn": ["San Antonio", 29.5, -98.41, 4],
  // Austin
  "sh 130": ["Austin", 30.25, -97.6, 8], "130": ["Austin", 30.25, -97.6, 8], kyle: ["Austin", 29.99, -97.88, 5], buda: ["Austin", 30.08, -97.84, 4], "san marcos": ["Austin", 29.88, -97.94, 5], manor: ["Austin", 30.34, -97.56, 4], hutto: ["Austin", 30.54, -97.55, 4],
  georgetown: ["Austin", 30.63, -97.68, 6], "round rock": ["Austin", 30.51, -97.68, 5], "se austin": ["Austin", 30.18, -97.7, 7], "south austin": ["Austin", 30.18, -97.78, 7], "north austin": ["Austin", 30.42, -97.7, 7], "mustang ridge": ["Austin", 30.06, -97.69, 4],
  // Laredo
  killam: ["Laredo", 27.62, -99.45, 3], "north laredo": ["Laredo", 27.68, -99.47, 5], "world trade bridge": ["Laredo", 27.6, -99.53, 3], "mines road": ["Laredo", 27.62, -99.56, 5], "el portal": ["Laredo", 27.55, -99.43, 3], pinnacle: ["Laredo", 27.68, -99.44, 3], unitec: ["Laredo", 27.7, -99.46, 3], eastpoint: ["Laredo", 27.55, -99.42, 3],
};

const PARSE_SYSTEM = `You turn a Harbor Capital analyst's comp request into a structured search. Harbor buys Class B infill industrial in Texas (DFW, Houston, San Antonio, Austin, Laredo).
Vocabulary: "45k SF"/"45ksf" = 45,000 SF; "28' clear"/"CH 28" = clear height; DH = dock-high doors; GL / drive-ins / ramps = grade-level; RL rear-load, FL front-load, cross-dock; tilt / tilt-wall / metal / masonry = construction; IOS = industrial outdoor storage (yard, priced per acre); MT = multi-tenant; shallow bay / small bay; flex; crane-served / heavy power = manufacturing; SLB = sale-leaseback; user sale = owner-user; BTS = build-to-suit; "PSF"/"/SF" price or rent; rents under ~$2 are $/SF/month, else $/SF/year; NNN / MG / gross; cap / going-in cap; WALT; "last 24 months", "2025-YTD"; submarkets such as NW Houston, 290, Beltway 8/BW8, Grand Pkwy, Katy/Brookshire, Humble, Aldine, Baytown, South Belt, GSW/Upper GSW, Brookhollow/South Stemmons, East Dallas, Denton, Alliance, Tri County, NE Loop 410, SH 130, Kyle, Laredo parks.
Return JSON only.`;

function parseUser(q, today) {
  return `Today is ${today}. Request: """${q}"""
Return:
{
 "interpretation": "one short sentence restating what comps they want",
 "comp_types": ["sale"|"lease"|"listing"|"land_sale", ...]  (what they asked for; if unclear, ["sale","lease"]),
 "subject": {"address": "street address if one is given else null", "property_name": null, "location_text": "the place phrase to search around if not a street address (e.g. 'NW Houston near US-290 & Beltway 8', 'Upper GSW', 'Laredo') else null", "market": "Houston|DFW|San Antonio|Austin|Laredo|El Paso|null", "radius_mi": number or null (only if stated, e.g. 'within 3 miles')},
 "size_sf": {"min": n|null, "max": n|null, "target": n|null},   ("45k SF building" -> target 45000; "450,000 - 1,000,000" -> min/max)
 "land_acres": {"min": n|null, "max": n|null, "target": n|null},
 "clear_height_ft": {"min": n|null, "target": n|null},
 "year_built": {"min": n|null, "max": n|null, "target": n|null},
 "loading": [ "rear"|"front"|"cross_dock"|"side" ],
 "construction": [ "tilt_wall"|"metal"|"masonry"|"concrete_block" ],
 "property_subtype": [ "bulk_distribution"|"shallow_bay"|"flex"|"manufacturing"|"ios"|"cold_storage"|"multi_tenant"|"service"|"warehouse" ],
 "building_class": [ "A"|"B"|"C" ],
 "sale_type": [ "investment"|"owner_user"|"sale_leaseback"|"portfolio" ],
 "lease_structure": [ "NNN"|"MG"|"Gross" ],
 "tenancy": "single"|"multi"|null,
 "needs": {"yard": bool, "rail": bool, "heavy_power": bool, "cranes": bool, "cap_rate": bool},
 "price_psf": {"min": n|null, "max": n|null}, "rent_psf_yr": {"min": n|null, "max": n|null}, "cap_rate_pct": {"min": n|null, "max": n|null},
 "since": "YYYY-MM-DD lower bound on deal date if they gave recency ('last 24 months', 'since 2024', '2025-YTD') else null",
 "strict": ["fields the analyst made non-negotiable, e.g. 'size_sf' for an explicit range, 'property_subtype' for 'IOS comps only', 'comp_types', 'since', 'building_class'"],
 "priorities": ["location"|"size"|"recency"|"product"|"clear_height"|"vintage" ... the things they emphasised],
 "sort": "relevance"|"newest"
}
Use null / [] for anything not stated. Do not invent constraints.`;
}

// Deterministic fallback parser for when the LLM is unavailable.
export function heuristicParse(q) {
  const s = q.toLowerCase();
  const out = { interpretation: `Read without AI (provider unavailable): ${q}`, comp_types: [], subject: {}, size_sf: {}, land_acres: {}, clear_height_ft: {}, year_built: {}, loading: [], construction: [], property_subtype: [], building_class: [], sale_type: [], lease_structure: [], needs: {}, price_psf: {}, rent_psf_yr: {}, cap_rate_pct: {}, strict: [], priorities: [] };
  if (/\blease|rent/.test(s)) out.comp_types.push("lease");
  if (/\bsale|sold|cap\b/.test(s)) out.comp_types.push("sale");
  if (!out.comp_types.length) out.comp_types = ["sale", "lease"];
  const k = (x) => (/k$/i.test(x) ? parseFloat(x) * 1000 : /m$/i.test(x) ? parseFloat(x) * 1e6 : parseFloat(x.replace(/,/g, "")));
  const range = /(\d[\d,.]*\s*[km]?)\s*(?:-|to|–)\s*(\d[\d,.]*\s*[km]?)\s*(?:sf|sq)/i.exec(q) || /(\d{1,3}(?:,\d{3})+|\d+\s*k)\s*(?:-|to|–)\s*(\d{1,3}(?:,\d{3})+|\d+\s*k)\b/i.exec(q);
  if (range) {
    let a = k(range[1].trim()), b = k(range[2].trim());
    if (/k$/i.test(range[2].trim()) && !/[km]$/i.test(range[1].trim()) && a < 1000) a *= 1000;
    if (/m$/i.test(range[2].trim()) && !/[km]$/i.test(range[1].trim()) && a < 100) a *= 1e6;
    Object.assign(out.size_sf, { min: a, max: b });
  }
  else {
    const one = /(\d[\d,.]*\s*[km]?)\s*(?:sf|sq\s*ft|square feet)/i.exec(q);
    if (one) out.size_sf.target = k(one[1].trim());
  }
  const ch = /(\d{2})\s*['’]?\s*(?:ft\s*)?clear/i.exec(q);
  if (ch) out.clear_height_ft.target = Number(ch[1]);
  if (/rear[- ]?load/.test(s)) out.loading.push("rear");
  if (/front[- ]?load/.test(s)) out.loading.push("front");
  if (/cross[- ]?dock/.test(s)) out.loading.push("cross_dock");
  if (/tilt/.test(s)) out.construction.push("tilt_wall");
  if (/metal/.test(s)) out.construction.push("metal");
  if (/\bios\b|outdoor storage|truck (yard|terminal)/.test(s)) out.property_subtype.push("ios");
  if (/crane|manufactur/.test(s)) out.property_subtype.push("manufacturing");
  if (/heavy power|\bamps?\b|kva/.test(s)) out.needs.heavy_power = true;
  if (/shallow[- ]bay|small[- ]bay/.test(s)) out.property_subtype.push("shallow_bay");
  if (/multi[- ]tenant|\bmt\b/.test(s)) out.property_subtype.push("multi_tenant");
  if (/flex/.test(s)) out.property_subtype.push("flex");
  if (/\bavailab|for lease|competitive set|comp set availab/.test(s)) out.comp_types.push("listing");
  if (/class\s*a\b/.test(s)) out.building_class.push("A");
  if (/class\s*b\b/.test(s)) out.building_class.push("B");
  if (/class\s*c\b/.test(s)) out.building_class.push("C");
  if (/cold storage|freezer/.test(s)) out.property_subtype.push("cold_storage");
  if (/\bbulk\b|big[- ]box|distribution center/.test(s)) out.property_subtype.push("bulk_distribution");
  if (/rail/.test(s)) out.needs.rail = true;
  if (/yard|laydown/.test(s)) out.needs.yard = true;
  if (/cap rate/.test(s)) out.needs.cap_rate = /with cap|has cap/.test(s);
  if (/masonry/.test(s)) out.construction.push("masonry");
  const yb = /(?:built|vintage|yoc)\s*(?:in\s*)?((?:19|20)\d{2})/.exec(s);
  if (yb) out.year_built.target = Number(yb[1]);
  const sinceY = /since\s+((?:19|20)\d{2})|((?:19|20)\d{2})\s*\+|((?:20)\d{2})[- ]ytd/.exec(s);
  if (sinceY) out.since = `${sinceY[1] || sinceY[2] || sinceY[3]}-01-01`;
  const rad = /within\s+(\d+(?:\.\d+)?)\s*(?:mi|miles)/.exec(s);
  if (rad) out.subject.radius_mi = Number(rad[1]);
  const months = /last\s+(\d+)\s+months/.exec(s);
  const years = /last\s+(\d+)\s+years/.exec(s);
  const now = new Date();
  if (months) out.since = new Date(now.getFullYear(), now.getMonth() - Number(months[1]), 1).toISOString().slice(0, 10);
  else if (years) out.since = new Date(now.getFullYear() - Number(years[1]), now.getMonth(), 1).toISOString().slice(0, 10);
  const SUF = "st|street|rd|road|dr|drive|blvd|boulevard|pkwy|parkway|pky|fwy|freeway|hwy|highway|ln|lane|ave|avenue|way|ct|court|loop|trl|trail|row|cir|circle|pl|place|pike|xing|crossing";
  const addrRe = new RegExp(`(?<![\\d,.$-]\\s?)\\b(\\d{2,6}(?:-\\d{2,6})?)\\s+((?:[nsew]\\.?\\s+)?(?:(?!sf\\b|ac\\b|acres\\b)[a-z][\\w.'-]*\\s+){0,4}?(?:${SUF}))\\b\\.?(?:,?\\s+(?:in\\s+)?([a-z][a-z .]{2,20}?)(?=,|\\s+tx\\b|\\s+-|\\s+(?:sale|lease|comps?|need)\\b|$))?`, "i");
  let addr = addrRe.exec(q);
  if (!addr) {
    // "built 2001 - 8901 Forney Rd Dallas": street names are capitalized in analyst text.
    const capRe = new RegExp(`(?:^|[\\s(])(\\d{3,6})\\s+((?:[NSEW]\\.?\\s+)?(?:[A-Z][\\w.'-]*\\s+){0,4}?(?:${SUF.split("|").map((x) => x[0].toUpperCase() + x.slice(1)).join("|")}))\\b\\.?(?:,?\\s+([A-Z][a-zA-Z .]{2,20}?)(?=,|\\s+TX\\b|\\s+-|\\s+[a-z]|$))?`);
    addr = capRe.exec(q);
  }
  if (addr) out.subject.address = `${addr[1]} ${addr[2]}${addr[3] ? ", " + addr[3].trim() : ""}`;

  for (const a of Object.keys(ALIASES).sort((x, y) => y.length - x.length)) {
    if (new RegExp(`(^|[^a-z0-9])${a.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}([^a-z0-9]|$)`).test(s)) {
      out.subject.location_text = a;
      break;
    }
  }
  for (const m of Object.keys(MARKETS)) if (s.includes(m.toLowerCase())) out.subject.market = m;
  if (/\bdallas|fort worth|dfw\b/.test(s)) out.subject.market = "DFW";
  return out;
}

// Query parsing is latency-critical: race every configured provider and take
// the first valid answer; fall back to the deterministic parser after 9 s.
// Deterministic cues the model sometimes drops.
function applyCues(p, q) {
  const s = q.toLowerCase();
  p.sale_type = p.sale_type || [];
  if (/\buser sales?\b|owner[- ]user/.test(s) && !p.sale_type.includes("owner_user")) p.sale_type.push("owner_user");
  if (/\bslb\b|sale[- ]leaseback/.test(s) && !p.sale_type.includes("sale_leaseback")) p.sale_type.push("sale_leaseback");
  if (/\bios\b|outdoor storage|truck (yard|terminal)/.test(s) && !(p.property_subtype || []).includes("ios")) p.property_subtype = [...(p.property_subtype || []), "ios"];
  if (/crane/.test(s)) p.needs = { ...(p.needs || {}), cranes: true };
  if (/with cap rates?|has cap|cap rates? (only|required)/.test(s)) p.needs = { ...(p.needs || {}), cap_rate: true };
  const ac = /(\d+(?:\.\d+)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?)\s*(?:ac|acres)\b/.exec(s);
  if (ac && !(p.land_acres && (p.land_acres.min || p.land_acres.max))) p.land_acres = { min: +ac[1], max: +ac[2], target: null };
  return p;
}

export async function parseQuery(llm, q, llms = null) {
  const today = new Date().toISOString().slice(0, 10);
  const pool = (llms && llms.length ? llms : llm ? [llm] : []).map((m) =>
    m.chat({ system: PARSE_SYSTEM, user: parseUser(q, today), tier: "fast", reasoning: "off", maxTokens: 1500, timeoutMs: 12000 }).then((p) => {
      if (p && p.comp_types) return applyCues({ ...p, _parser: `llm:${m.provider}` }, q);
      throw new Error("bad parse");
    }),
  );
  if (pool.length) {
    const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 9000));
    try {
      return await Promise.race([Promise.any(pool), timeout]);
    } catch {
      /* fall through */
    }
  }
  return applyCues({ ...heuristicParse(q), _parser: "heuristic" }, q);
}

async function resolveLocation(env, db, parsed, explicit) {
  // explicit {lat,lng,radius_mi,label} from the map wins
  if (explicit && explicit.lat != null) return { lat: explicit.lat, lng: explicit.lng, radius: explicit.radius_mi || 5, label: explicit.label || "map pin", kind: "pin" };
  const subj = parsed.subject || {};
  const stated = subj.radius_mi || null;
  if (subj.address) {
    const g = await geocode(env, { address: subj.address, city: null, state: "TX" }, { cache: geoCache(db) });
    if (g.lat != null) return { lat: g.lat, lng: g.lng, radius: stated || 5, label: g.formatted || subj.address, kind: "address", quality: g.quality };
  }
  let text = (subj.location_text || "").toLowerCase().trim();
  const MARKET_WORDS = { dfw: "DFW", "dallas-fort worth": "DFW", "dallas fort worth": "DFW", metroplex: "DFW", "north texas": "DFW", houston: "Houston", "greater houston": "Houston", "san antonio": "San Antonio", sa: "San Antonio", austin: "Austin", atx: "Austin", laredo: "Laredo", "el paso": "El Paso" };
  const mw = MARKET_WORDS[text.replace(/,?\s*(tx|texas)$/, "").replace(/\s+(metro|market|area)$/, "")];
  if (mw && MARKETS[mw]) {
    const m = MARKETS[mw];
    if (!subj.market) parsed.subject = { ...subj, market: mw };
    return { lat: m.lat, lng: m.lng, radius: stated || m.r, label: mw, kind: "market" };
  }
  if (text) {
    const key = Object.keys(ALIASES).sort((a, b) => b.length - a.length).find((a) => text === a || text.includes(a));
    // CoStar submarket names in the gazetteer table
    const row = await db
      .prepare("SELECT * FROM v2_submarkets WHERE lower(name) = ? OR lower(aliases) LIKE ? LIMIT 1")
      .bind(text, `%|${text}|%`)
      .first()
      .catch(() => null);
    if (row) return { lat: row.latitude, lng: row.longitude, radius: stated || row.radius_mi, label: `${row.name} (${row.market})`, kind: "submarket" };
    if (key && (text === key || text.length < key.length + 12)) {
      const [market, lat, lng, r] = ALIASES[key];
      return { lat, lng, radius: stated || r, label: `${key} (${market})`, kind: "alias" };
    }
    const g = await geocode(env, { address: subj.location_text, state: "TX" }, { cache: geoCache(db) });
    if (g.lat != null && !/^texas,? usa$/i.test(String(g.formatted || "").trim())) {
      const r = { rooftop: 3, interpolated: 3, intersection: 3, street: 4, postal: 4, city: 10 }[g.quality] || 6;
      return { lat: g.lat, lng: g.lng, radius: stated || r, label: g.formatted, kind: g.quality };
    }
    if (key) {
      const [market, lat, lng, r] = ALIASES[key];
      return { lat, lng, radius: stated || r, label: `${key} (${market})`, kind: "alias" };
    }
  }
  if (subj.market && MARKETS[subj.market]) {
    const m = MARKETS[subj.market];
    return { lat: m.lat, lng: m.lng, radius: stated || m.r, label: subj.market, kind: "market" };
  }
  return null;
}

function geoCache(db) {
  return {
    get: async (q) => {
      const r = await db.prepare("SELECT result_json FROM v2_geocode_cache WHERE q = ?").bind(q).first().catch(() => null);
      return r ? JSON.parse(r.result_json) : null;
    },
    put: async (q, v) => db.prepare("INSERT OR REPLACE INTO v2_geocode_cache (q, result_json) VALUES (?, ?)").bind(q, JSON.stringify(v)).run().catch(() => null),
  };
}
export { geoCache };

const RELATED = [
  ["warehouse", "bulk_distribution", "shallow_bay", "multi_tenant"],
  ["shallow_bay", "multi_tenant", "flex", "service"],
  ["manufacturing", "service", "warehouse"],
  ["ios", "land"],
];

// Typical space size by product (leased / available SF for leases, building SF for sales).
const SIZE_BANDS = { shallow_bay: [1500, 60000], multi_tenant: [1500, 80000], flex: [1000, 50000], service: [2000, 60000], bulk_distribution: [150000, 3e6], warehouse: [10000, 400000], manufacturing: [10000, 1e6], cold_storage: [5000, 600000] };
function productScore(want, have, sf) {
  if (!want || !want.length) return null;
  if (!have) {
    // Unknown product: judge plausibility from size.
    if (sf) {
      const fits = want.some((w) => SIZE_BANDS[w] && sf >= SIZE_BANDS[w][0] * 0.7 && sf <= SIZE_BANDS[w][1] * 1.3);
      const known = want.some((w) => SIZE_BANDS[w]);
      if (known) return fits ? 0.62 : 0.15;
    }
    return 0.45;
  }
  if (want.includes(have)) return 1;
  if (RELATED.some((g) => g.includes(have) && want.some((w) => g.includes(w)))) return 0.6;
  return 0.12;
}
function rangeScore(v, { min, max, target } = {}, width = 0.45) {
  if (min == null && max == null && target == null) return null;
  if (v == null) return 0.45;
  if (target != null) return Math.exp(-Math.abs(Math.log(v / target)) / width);
  if ((min == null || v >= min) && (max == null || v <= max)) return 1;
  const edge = v < (min ?? -Infinity) ? min : max;
  return Math.exp(-Math.abs(Math.log(v / edge)) / (width * 0.6));
}
function linearScore(v, target, span) {
  if (target == null) return null;
  if (v == null) return 0.45;
  return Math.max(0, 1 - Math.abs(v - target) / span);
}

// Product tags (see tags.js). Defining tags decide whether a comp is the same
// kind of building; related tags count partly; a comp with no defining tag is
// "type unknown" (kept, scored lower); a different defining tag is a mismatch.
export const TAG_GROUPS = {
  crane: ["crane", "manufacturing", "heavy_power"], manufacturing: ["manufacturing", "crane", "heavy_power"], heavy_power: ["heavy_power", "manufacturing", "crane"],
  ios: ["ios", "yard", "truck_terminal"], truck_terminal: ["truck_terminal", "ios", "cross_dock"],
  shallow_bay: ["shallow_bay", "multi_tenant", "flex", "service"], multi_tenant: ["multi_tenant", "shallow_bay", "flex", "service"], flex: ["flex", "shallow_bay", "service"], service: ["service", "shallow_bay", "flex"],
  bulk: ["bulk", "cross_dock"], cold_storage: ["cold_storage"],
};
const DEFINING = new Set(Object.keys(TAG_GROUPS));
export function tagScore(want, haveStr) {
  const w = (want || []).filter((x) => DEFINING.has(x));
  if (!w.length) return null;
  const have = new Set(String(haveStr || "").split("|").filter(Boolean));
  if (w.some((x) => have.has(x))) return 1;
  if (w.some((x) => TAG_GROUPS[x].some((y) => have.has(y)))) return 0.72;
  if (![...have].some((x) => DEFINING.has(x))) return 0.42;
  return 0.1;
}
function queryTags(parsed, q) {
  const t = new Set();
  const s = String(q || "").toLowerCase();
  const sub = parsed.property_subtype || [];
  if (/crane/.test(s) || parsed.needs?.cranes) t.add("crane");
  if (parsed.needs?.heavy_power || /heavy power/.test(s)) t.add("heavy_power");
  if (sub.includes("manufacturing")) t.add("manufacturing");
  if (sub.includes("ios") || /\bios\b|outdoor storage/.test(s)) t.add("ios");
  if (/truck terminal/.test(s)) t.add("truck_terminal");
  if (sub.includes("shallow_bay") || /shallow[- ]bay|small[- ]bay/.test(s)) t.add("shallow_bay");
  if (sub.includes("multi_tenant")) t.add("multi_tenant");
  if (sub.includes("flex")) t.add("flex");
  if (sub.includes("bulk_distribution") || /big[- ]box|\bbulk\b/.test(s)) t.add("bulk");
  if (sub.includes("cold_storage")) t.add("cold_storage");
  for (const l of parsed.loading || []) t.add({ rear: "rear_load", front: "front_load", cross_dock: "cross_dock" }[l]);
  if (parsed.needs?.yard) t.add("yard");
  if (parsed.needs?.rail) t.add("rail");
  t.delete(undefined);
  return [...t];
}

// SF a sale actually priced: price / price-per-SF when both are known (the
// building record can hold a different size, e.g. one building of a park).
export function saleSf(c) {
  if (c.sale_price > 0 && c.price_psf > 0 && c.sale_type !== "portfolio") {
    const v = c.sale_price / c.price_psf;
    if (v > 500 && v < 5e7) return Math.round(v);
  }
  return c.building_sf ?? c.leased_sf ?? null;
}

const BASE_WEIGHTS = { location: 0.27, size: 0.2, recency: 0.15, product: 0.15, clear_height: 0.07, vintage: 0.07, features: 0.06, data: 0.03 };

export function scoreComp(c, parsed, loc, nowYear) {
  const parts = {};
  const notes = [];
  // location
  if (loc) {
    const d = c.latitude != null ? haversineMi(loc.lat, loc.lng, c.latitude, c.longitude) : null;
    c._dist = d;
    if (d == null) parts.location = 0.2;
    else if (d <= loc.radius) parts.location = 1 - 0.45 * Math.pow(d / loc.radius, 1.4);
    else parts.location = Math.max(0, 0.55 * (1 - (d - loc.radius) / (loc.radius * 1.5)));
    if (c.geocode_quality && ["city", "postal", "street", "submarket"].includes(c.geocode_quality)) {
      parts.location *= 0.8;
      notes.push("approx. location");
    }
  }
  // size
  const sf = c.record_kind === "lease" ? c.leased_sf ?? c.building_sf : c.record_kind === "listing" ? c.available_sf ?? c.building_sf : saleSf(c);
  parts.size = rangeScore(sf, parsed.size_sf || {});
  if (parsed.land_acres && (parsed.land_acres.target || parsed.land_acres.min)) {
    const la = rangeScore(c.land_acres, parsed.land_acres);
    parts.size = parts.size == null ? la : (parts.size + la) / 2;
  }
  // recency
  const y = c.txn_year;
  if (y == null) parts.recency = 0.3;
  else {
    const age = Math.max(0, nowYear - y);
    parts.recency = age <= 1 ? 1 : age <= 2 ? 1 - 0.2 * (age - 1) : age <= 3 ? 0.8 - 0.3 * (age - 2) : Math.max(0.05, 0.5 - 0.12 * (age - 3));
  }
  // product
  const pt = productScore(parsed.property_subtype, c.property_subtype, c.record_kind === "lease" ? c.leased_sf ?? c.building_sf : c.record_kind === "listing" ? c.available_sf ?? c.building_sf : saleSf(c));
  const cls = parsed.building_class && parsed.building_class.length ? (c.building_class ? (parsed.building_class.includes(c.building_class) ? 1 : 0.3) : 0.5) : null;
  const st = parsed.sale_type && parsed.sale_type.length && c.record_kind === "sale" ? (c.sale_type ? (parsed.sale_type.includes(c.sale_type) ? 1 : 0.35) : 0.5) : null;
  const tg = tagScore(parsed.tags_want, c.product_tags);
  if (tg != null) c._tagState = tg >= 0.99 ? "match" : tg >= 0.7 ? "related" : tg >= 0.4 ? "unknown" : "conflict";
  const pparts = (tg != null ? [tg, cls, st] : [pt, cls, st]).filter((x) => x != null);
  parts.product = pparts.length ? pparts.reduce((a, b) => a + b, 0) / pparts.length : null;
  // clear height / vintage
  const chT = parsed.clear_height_ft?.target ?? parsed.clear_height_ft?.min ?? null;
  parts.clear_height = chT != null ? (c.clear_height_ft == null ? 0.45 : parsed.clear_height_ft.min && !parsed.clear_height_ft.target ? (c.clear_height_ft >= chT ? 1 : Math.max(0, 1 - (chT - c.clear_height_ft) / 8)) : linearScore(c.clear_height_ft, chT, 12)) : null;
  const ybT = parsed.year_built?.target ?? (parsed.year_built?.min && parsed.year_built?.max ? (parsed.year_built.min + parsed.year_built.max) / 2 : parsed.year_built?.min ?? parsed.year_built?.max ?? null);
  parts.vintage = ybT != null ? linearScore(c.year_built, ybT, 30) : null;
  // features
  const f = [];
  if (parsed.loading?.length) f.push(c.loading ? (parsed.loading.includes(c.loading) ? 1 : 0.25) : 0.45);
  if (parsed.construction?.length) f.push(c.construction ? (parsed.construction.includes(c.construction) ? 1 : 0.3) : 0.45);
  if (parsed.tenancy) f.push(c.tenancy ? (c.tenancy === parsed.tenancy ? 1 : 0.3) : 0.5);
  const n = parsed.needs || {};
  if (n.rail) f.push(c.rail_served ? 1 : c.rail_served === 0 ? 0.1 : 0.4);
  if (n.yard) f.push(c.yard_acres || c.property_subtype === "ios" || /yard|ios|outdoor/i.test(c.notes || "") ? 1 : 0.35);
  for (const ft of (parsed.tags_want || []).filter((x) => ["rear_load", "front_load", "cross_dock", "yard", "rail"].includes(x))) f.push(String(c.product_tags || "").includes(`|${ft}|`) ? 1 : 0.4);
  if (n.cranes || n.heavy_power) f.push(/crane|amp|kva|heavy power|480/i.test(`${c.power || ""} ${c.cranes || ""} ${c.notes || ""}`) || c.property_subtype === "manufacturing" ? 1 : 0.35);
  parts.features = f.length ? f.reduce((a, b) => a + b, 0) / f.length : null;
  // data completeness (has the numbers an analyst needs)
  const hasPrice = c.record_kind === "sale" ? (c.sale_price || c.price_psf ? 1 : 0) + (c.cap_rate_pct ? 0.5 : 0) : (c.rent_psf_yr ? 1 : 0) + (c.term_months ? 0.25 : 0) + (c.lease_structure ? 0.25 : 0);
  parts.data = Math.min(1, hasPrice / 1.25) * (c.severity === "error" ? 0.5 : 1);
  if (n.cap_rate && c.record_kind === "sale" && !c.cap_rate_pct) parts.data *= 0.4;

  // weights
  const w = { ...BASE_WEIGHTS };
  for (const p of parsed.priorities || []) if (w[p] != null) w[p] *= 1.7;
  let num = 0, den = 0;
  for (const [k, v] of Object.entries(parts)) {
    if (v == null) continue;
    num += w[k] * v;
    den += w[k];
  }
  const score = den ? num / den : 0;
  return { score, parts, notes, sf };
}

function passesHard(c, parsed, loc, sinceYear) {
  const strict = new Set(parsed.strict || []);
  const types = parsed.comp_types && parsed.comp_types.length ? parsed.comp_types : ["sale", "lease"];
  if (!types.includes(c.record_kind) && !(c.record_kind === "land_sale" && types.includes("sale") && (parsed.property_subtype || []).includes("ios"))) return false;
  if (c.status !== "active") return false;
  if (sinceYear != null && c.txn_year != null && c.txn_year < sinceYear) return false;
  if (strict.has("since") && sinceYear != null && c.txn_year == null) return false;
  const sf = c.record_kind === "lease" ? c.leased_sf ?? c.building_sf : saleSf(c);
  const sz = parsed.size_sf || {};
  if ((strict.has("size_sf") || (sz.min != null && sz.max != null)) && sf != null) {
    if (sz.min != null && sf < sz.min * 0.9) return false;
    if (sz.max != null && sf > sz.max * 1.1) return false;
  }
  const la = parsed.land_acres || {};
  if (la.min != null && la.max != null && c.land_acres != null && (c.land_acres < la.min * 0.85 || c.land_acres > la.max * 1.15)) return false;
  if (parsed.product_gate && c._tagState === "conflict") return false;
  if (strict.has("property_subtype") && parsed.property_subtype?.length && c.property_subtype && !parsed.property_subtype.includes(c.property_subtype)) return false;
  if (strict.has("building_class") && parsed.building_class?.length && c.building_class && !parsed.building_class.includes(c.building_class)) return false;
  if (parsed.needs?.cap_rate && strict.has("cap_rate") && !c.cap_rate_pct) return false;
  if (loc && c._dist != null && c._dist > loc.radius * (loc.kind === "market" ? 1.0 : 2.6)) return false;
  if (loc && loc.kind === "market" && c.market && parsed.subject?.market && c.market !== parsed.subject.market) return false;
  if (strict.has("clear_min") && parsed.clear_height_ft?.min && c.clear_height_ft != null && c.clear_height_ft < parsed.clear_height_ft.min) return false;
  const ybf = parsed.year_built || {};
  if (strict.has("year_built") && c.year_built != null && ((ybf.min && c.year_built < ybf.min) || (ybf.max && c.year_built > ybf.max))) return false;
  for (const f of ["loading", "construction"]) if (strict.has(f) && parsed[f]?.length && c[f] && !parsed[f].includes(c[f])) return false;
  if (strict.has("source") && parsed.source && c.source_kind !== parsed.source) return false;
  const pr = parsed.price_psf || {};
  if (c.price_psf != null && ((pr.min != null && c.price_psf < pr.min * 0.9) || (pr.max != null && c.price_psf > pr.max * 1.1))) return false;
  const rr = parsed.rent_psf_yr || {};
  if (c.rent_psf_yr != null && ((rr.min != null && c.rent_psf_yr < rr.min * 0.9) || (rr.max != null && c.rent_psf_yr > rr.max * 1.1))) return false;
  return true;
}

function median(a) {
  const v = a.filter((x) => x != null && Number.isFinite(x)).sort((x, y) => x - y);
  if (!v.length) return null;
  const m = Math.floor(v.length / 2);
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

export function summarize(rows) {
  const sales = rows.filter((r) => r.record_kind === "sale" || r.record_kind === "land_sale");
  const leases = rows.filter((r) => r.record_kind === "lease");
  const psf = sales.filter((r) => r.sale_type !== "portfolio").map((r) => r.price_psf);
  const caps = sales.map((r) => r.cap_rate_pct);
  const rents = leases.map((r) => r.rent_psf_yr);
  let wNum = 0, wDen = 0;
  for (const r of leases) if (r.rent_psf_yr && r.leased_sf) { wNum += r.rent_psf_yr * r.leased_sf; wDen += r.leased_sf; }
  const dates = rows.map((r) => r.txn_date).filter(Boolean).sort();
  return {
    count: rows.length, sales: sales.length, leases: leases.length,
    median_price_psf: median(psf), median_cap_rate: median(caps), cap_rate_n: caps.filter(Boolean).length,
    median_rent_psf_yr: median(rents), sf_weighted_rent_psf_yr: wDen ? wNum / wDen : null,
    median_term_months: median(leases.map((r) => r.term_months)), median_escalation: median(leases.map((r) => r.escalation_pct)),
    date_range: dates.length ? [dates[0], dates[dates.length - 1]] : null,
  };
}

export async function findComps(env, db, llm, { query = "", location = null, filters = {}, limit = 30, llms = null, parsed: given = null, skipSubject = false, perProperty = 3 }) {
  const t0 = Date.now();
  const parsed = given ? JSON.parse(JSON.stringify(given)) : query.trim() ? await parseQuery(llm, query, llms) : { comp_types: filters.comp_types || ["sale", "lease"], subject: {}, strict: [], priorities: [] };
  if (!given && !(parsed.tags_want || []).length) parsed.tags_want = queryTags(parsed, query);
  if (!given && parsed.product_gate == null) parsed.product_gate = true;
  // Explicit UI filters override the parse and are hard constraints.
  parsed.strict = parsed.strict || [];
  for (const [k, v] of Object.entries(filters || {})) {
    if (v == null || v === "" || (Array.isArray(v) && !v.length)) continue;
    if (typeof v === "object" && !Array.isArray(v) && !Object.values(v).some((x) => x != null && x !== "" && x !== false)) continue;
    if (k === "radius_mi") continue;
    if (k === "needs") { parsed.needs = { ...(parsed.needs || {}), ...v }; if (v.cap_rate) parsed.strict.push("cap_rate"); continue; }
    if (k === "market") { parsed.subject = { ...(parsed.subject || {}), market: v }; continue; }
    parsed[k] = v;
    if (["size_sf", "property_subtype", "building_class", "since", "comp_types", "year_built", "loading", "construction", "source"].includes(k)) parsed.strict.push(k);
    if (k === "clear_height_ft" && v.min) parsed.strict.push("clear_min");
  }
  const loc = await resolveLocation(env, db, parsed, location);
  if (loc && filters?.radius_mi) loc.radius = Number(filters.radius_mi);
  let subject = null;
  try {
    if (!skipSubject) subject = await describeSubject(db, loc, parsed);
  } catch {}
  if (subject) {
    const auto = [];
    const sz = parsed.size_sf || {};
    if (sz.min == null && sz.max == null && sz.target == null && subject.building_sf) { parsed.size_sf = { target: subject.building_sf }; auto.push("size"); }
    const yb = parsed.year_built || {};
    if (yb.min == null && yb.max == null && yb.target == null && subject.year_built) { parsed.year_built = { target: subject.year_built }; auto.push("vintage"); }
    if (!(parsed.clear_height_ft?.target || parsed.clear_height_ft?.min) && subject.clear_height_ft) { parsed.clear_height_ft = { target: subject.clear_height_ft }; auto.push("clear height"); }
    if (!(parsed.property_subtype || []).length && subject.property_subtype && subject.property_subtype !== "warehouse") { parsed.property_subtype = [subject.property_subtype]; auto.push("product"); }
    subject.auto_filled = auto;
  }
  const nowYear = new Date().getFullYear() + new Date().getMonth() / 12;
  const sinceYear = parsed.since ? dateToNumber(parsed.since) : null;

  let sql = `SELECT c.*, p.address, p.property_name, p.city, p.zip, p.county, p.market, p.submarket, p.latitude, p.longitude, p.geocode_quality,
    p.building_sf, p.land_acres, p.year_built, p.clear_height_ft, p.dock_doors, p.drive_in_doors, p.loading, p.construction, p.office_pct,
    p.sprinkler, p.rail_served, p.yard_acres, p.building_class, p.property_subtype, p.tenancy, p.power, p.cranes, p.features_json, p.product_tags, p.id AS pid
    FROM v2_comps c JOIN v2_properties p ON p.id = c.property_id WHERE c.status = 'active'`;
  const binds = [];
  if (loc) {
    const b = bbox(loc.lat, loc.lng, Math.max(loc.radius * 2.6, 3));
    sql += " AND p.latitude BETWEEN ? AND ? AND p.longitude BETWEEN ? AND ?";
    binds.push(b.minLat, b.maxLat, b.minLng, b.maxLng);
  } else if (parsed.subject?.market) {
    sql += " AND p.market = ?";
    binds.push(parsed.subject.market);
  }
  const types = parsed.comp_types && parsed.comp_types.length ? parsed.comp_types : ["sale", "lease"];
  const kinds = new Set(types);
  if (kinds.has("sale")) kinds.add("land_sale");
  sql += ` AND c.record_kind IN (${[...kinds].map(() => "?").join(",")})`;
  binds.push(...kinds);
  sql += " LIMIT 6000";
  const { results } = await db.prepare(sql).bind(...binds).all();

  let scored = [];
  const misses = [];
  const relaxedParsed = { ...parsed, strict: [], size_sf: { target: parsed.size_sf?.target ?? (parsed.size_sf?.min && parsed.size_sf?.max ? Math.sqrt(parsed.size_sf.min * parsed.size_sf.max) : parsed.size_sf?.min ?? parsed.size_sf?.max ?? null) }, price_psf: {}, rent_psf_yr: {} };
  for (const c of results) {
    const s = scoreComp(c, parsed, loc, nowYear);
    if (passesHard(c, parsed, loc, sinceYear)) scored.push({ ...c, _score: s.score, _parts: s.parts, _notes: s.notes, _sf: s.sf });
    else if (passesHard(c, relaxedParsed, loc ? { ...loc, radius: loc.radius * 1.6 } : loc, sinceYear ? sinceYear - 1 : null)) misses.push({ ...c, _score: s.score * 0.8, _parts: s.parts, _notes: [...s.notes, "outside your constraints"], _sf: s.sf, _relaxed: true });
  }
  scored.sort((a, b) => b._score - a._score);
  // Too few exact matches: show the closest near-misses, clearly labelled.
  if (scored.length < Math.min(8, limit)) {
    misses.sort((a, b) => b._score - a._score);
    scored = scored.concat(misses.slice(0, Math.min(10, limit) - scored.length));
  }
  // Diversity: avoid one park flooding the list - at most 3 comps per property.
  const perProp = new Map();
  const top = [];
  for (const r of scored) {
    const n = perProp.get(r.pid) || 0;
    if (n >= perProperty) continue;
    perProp.set(r.pid, n + 1);
    top.push(r);
    if (top.length >= limit) break;
  }
  if (parsed.sort === "newest") top.sort((a, b) => (b.txn_year || 0) - (a.txn_year || 0));
  // Deals AT the subject building are history, not comps: list them separately.
  let atSubject = [];
  if (loc && loc.kind === "address") {
    atSubject = scored.filter((r) => r._dist != null && r._dist < 0.03).slice(0, 10);
    const ids = new Set(atSubject.map((r) => r.id));
    for (let i = top.length - 1; i >= 0; i--) if (ids.has(top[i].id)) top.splice(i, 1);
    // refill
    for (const r of scored) {
      if (top.length >= limit) break;
      if (ids.has(r.id) || top.includes(r)) continue;
      const n = perProp.get(r.pid) || 0;
      if (n >= 3) continue;
      perProp.set(r.pid, n + 1);
      top.push(r);
    }
    top.sort((a, b) => b._score - a._score);
  }
  return { parsed, location: loc, subject, candidates: results.length, matched: scored.filter((x) => !x._relaxed).length, near_misses: top.filter((x) => x._relaxed).length, results: top.map(publicRow), subject_history: atSubject.map(publicRow), stats: summarize(top), ms: Date.now() - t0 };
}

function publicRow(r) {
  const out = {};
  for (const [k, v] of Object.entries(r)) if (!k.startsWith("_") && k !== "snapshot_json" && k !== "flags_json" && k !== "features_json" && k !== "sources_json") out[k] = v;
  out.score = Math.round(r._score * 100);
  out.parts = Object.fromEntries(Object.entries(r._parts).filter(([, v]) => v != null).map(([k, v]) => [k, Math.round(v * 100)]));
  out.distance_mi = r._dist != null ? +r._dist.toFixed(2) : null;
  out.size_sf = r._sf ?? null;
  out.relaxed = !!r._relaxed;
  out.tag_state = r._tagState || null;
  out.approx_location = ["city", "postal", "submarket"].includes(r.geocode_quality);
  out.flags = r.flags_json ? JSON.parse(r.flags_json).filter((f) => f.severity !== "info").map((f) => f.msg) : [];
  return out;
}

// Optional analyst notes for the top comps.
export async function explainComps(llm, query, parsed, rows) {
  if (!llm || !rows.length) return {};
  const brief = rows.slice(0, 12).map((r) => ({
    id: r.id, kind: r.record_kind, addr: r.address || r.property_name, city: r.city, submarket: r.submarket, dist_mi: r.distance_mi, date: r.txn_date,
    sf: r.size_sf, bldg_sf: r.building_sf, yb: r.year_built, clear: r.clear_height_ft, loading: r.loading, constr: r.construction, type: r.property_subtype, cls: r.building_class,
    price_psf: r.price_psf, cap: r.cap_rate_pct, sale_type: r.sale_type, rent_yr: r.rent_psf_yr, structure: r.lease_structure, term: r.term_months, tenant: r.tenant, score: r.score,
  }));
  const out = await llm
    .chat({
      tier: "fast",
      reasoning: "off",
      maxTokens: 2500,
      timeoutMs: 40000,
      system: "You are a Harbor Capital acquisitions analyst writing comp notes. Terse, specific, no fluff. JSON only.",
      user: `Subject / request: """${query}"""\nParsed: ${JSON.stringify({ size: parsed.size_sf, clear: parsed.clear_height_ft, yb: parsed.year_built, type: parsed.property_subtype, loading: parsed.loading })}\nComps: ${JSON.stringify(brief)}\nReturn {"summary": "2 sentences: what this comp set says (pricing/rent range, how good the set is, biggest gap)", "notes": {"<id>": "max 18 words: why it is (or is not) a good comp and the adjustment direction, e.g. 'Same corridor, 4 ft taller and 2019 vintage - adjust down; strongest size match'"}}`,
    })
    .catch(() => null);
  return out || {};
}
