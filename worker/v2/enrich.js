// Building-feature sourcing for a property, given only what a comp sheet has
// (an address). Waterfall, each source recorded as a feature observation with
// provenance and confidence:
//
//   1. broker sheets   - features stated in the comp files themselves (written at commit)
//   2. CoStar index    - ~7,100 TX industrial buildings Harbor exported from CoStar
//                        (clear height, docks, drive-ins, class, year built, land, type)
//   3. county appraisal- statewide StratMap parcel (owner, acres, account) then the
//                        county CAD for building SF / year built / class:
//                        Harris (HCAD 2026 CAMA table in D1), Dallas (DCAD), Collin (CCAD),
//                        Travis/Tarrant/Montgomery/Denton/Webb/Ellis/Johnson/Rockwall
//                        (True Prodigy CAD API)
//   4. web listings    - LLM with web search reads LoopNet/Crexi/broker flyers (OpenAI only)
//   5. aerial imagery  - satellite tile + vision model: loading config, yard, rail, trailer parking
//
// Conflicts are kept (every observation is stored); the property row holds the
// highest-confidence value per field.

import { addrSimilarity, haversineMi, bbox, parseAddress } from "./geo.js";

export const SOURCE_CONF = { manual: 1.0, broker_sheet: 0.85, om: 0.84, cad: 0.8, costar: 0.8, web: 0.6, aerial: 0.5, parcel: 0.75 };
// Per-field overrides: CAD is authoritative for land and vintage, weaker for building SF on multi-building parcels.
const FIELD_CONF = {
  cad: { land_acres: 0.92, year_built: 0.85, building_sf: 0.7 },
  costar: { clear_height_ft: 0.8, dock_doors: 0.75, drive_in_doors: 0.75 },
  aerial: { loading: 0.55, rail_served: 0.5, yard_acres: 0.45, trailer_parking: 0.4 },
};
export function confFor(source, field) {
  return FIELD_CONF[source]?.[field] ?? SOURCE_CONF[source] ?? 0.5;
}

const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

async function fetchJson(url, init = {}, timeoutMs = 12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { ...init, signal: ctrl.signal, headers: { "User-Agent": UA, ...(init.headers || {}) } });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}
async function fetchText(url, timeoutMs = 12000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers: { "User-Agent": UA } });
    return r.ok ? await r.text() : null;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

// ---------------- CoStar index ----------------
export async function matchCostar(db, prop) {
  if (prop.latitude == null) return null;
  const b = bbox(prop.latitude, prop.longitude, 0.4);
  const { results } = await db
    .prepare("SELECT * FROM v2_costar_props WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ? LIMIT 200")
    .bind(b.minLat, b.maxLat, b.minLng, b.maxLng)
    .all();
  let best = null;
  for (const c of results) {
    const d = haversineMi(prop.latitude, prop.longitude, c.latitude, c.longitude);
    const a = prop.address && c.address ? addrSimilarity(prop.address, c.address) : 0;
    let s = 0;
    if (a >= 0.95) s = 0.9 - d * 0.5;
    else if (a >= 0.75 && d < 0.15) s = 0.75;
    else if (d < 0.025 && a >= 0.5) s = 0.7;
    if (prop.building_sf && c.building_sf) {
      const r = Math.abs(prop.building_sf - c.building_sf) / Math.max(prop.building_sf, c.building_sf);
      if (r < 0.05) s += 0.1;
      else if (r > 0.5) s -= 0.25;
    }
    if (s > 0.6 && (!best || s > best.score)) best = { score: s, row: c, distanceMi: d, addrSim: a };
  }
  return best;
}

export function costarObservations(row) {
  const obs = [];
  const add = (field, value) => value != null && value !== "" && obs.push({ field, value, source: "costar", source_ref: `CoStar property ${row.prop_id}` });
  add("building_sf", row.building_sf);
  add("land_acres", row.land_acres);
  add("year_built", row.year_built || null);
  add("year_renovated", row.year_renovated || null);
  add("clear_height_ft", row.clear_height_ft);
  add("dock_doors", row.dock_doors);
  add("drive_in_doors", row.drive_in_doors);
  add("building_class", row.building_class && /^[ABC]$/.test(row.building_class) ? row.building_class : null);
  const sub = { Warehouse: "warehouse", Distribution: "bulk_distribution", Manufacturing: "manufacturing", Service: "service", Showroom: "flex", "R&D": "flex", "Food Processing": "manufacturing", "Refrigeration/Cold Storage": "cold_storage", "Truck Terminal": "ios", "Data Hosting": "data_center" }[row.sec_type];
  add("property_subtype", sub || null);
  if (row.rail && /yes|y|served/i.test(row.rail)) add("rail_served", true);
  if (row.power && row.power !== "No") add("power", row.power);
  if (row.crane && /yes/i.test(row.crane)) add("cranes", "yes");
  add("zoning", row.zoning);
  return obs;
}

// ---------------- Parcels / CAD ----------------
const STRATMAP = "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap_land_parcels_48_most_recent/MapServer/identify";
const TP_OFFICES = { Travis: "Travis", Tarrant: "Tarrant", Montgomery: "Montgomery", Denton: "Denton", Webb: "Webb", Ellis: "Ellis", Johnson: "Johnson", Rockwall: "Rockwall" };
const FIPS_COUNTY = { 48201: "Harris", 48113: "Dallas", 48439: "Tarrant", 48029: "Bexar", 48157: "Fort Bend", 48339: "Montgomery", 48085: "Collin", 48121: "Denton", 48453: "Travis", 48491: "Williamson", 48479: "Webb", 48039: "Brazoria", 48139: "Ellis", 48257: "Kaufman", 48251: "Johnson", 48091: "Comal", 48187: "Guadalupe", 48209: "Hays", 48167: "Galveston", 48473: "Waller", 48397: "Rockwall" };

const titleCase = (s) => (s ? String(s).toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase()) : null);

export async function stratmapParcel(lat, lng) {
  for (const tol of [0, 4]) {
    const url = `${STRATMAP}?geometry=${lng},${lat}&geometryType=esriGeometryPoint&sr=4326&layers=all&tolerance=${tol}&mapExtent=${lng - 0.005},${lat - 0.005},${lng + 0.005},${lat + 0.005}&imageDisplay=800,800,96&returnGeometry=false&f=json`;
    const d = await fetchJson(url);
    const a = d?.results?.[0]?.attributes;
    if (!a) continue;
    const num = (v) => (v == null || v === "Null" || v === "" ? null : Number(v));
    let acres = num(a.LEGAL_AREA) || num(a.GIS_AREA);
    if (!acres) {
      const m2 = num(a["st_area(shape)"] || a["Shape.STArea()"] || a.SHAPE_Area);
      if (m2) acres = (m2 * Math.cos((lat * Math.PI) / 180) ** 2) / 4046.86;
    }
    const fips = String(a.FIPS || a.CNTY_FIPS || "").replace(/\D/g, "");
    return {
      county: titleCase(a.COUNTY) || FIPS_COUNTY[fips] || FIPS_COUNTY["48" + fips.slice(-3)] || null,
      prop_id: a.PROP_ID || null,
      owner: a.OWNER_NAME || null,
      situs: [a.SITUS_NUM, a.SITUS_STREET_PREFIX, a.SITUS_STREET, a.SITUS_STREET_SUFFIX].filter((x) => x && x !== "Null").join(" ") || a.SITUS_ADDR || null,
      acres: acres ? +acres.toFixed(3) : null,
      year_built: num(a.YEAR_BUILT) > 1800 ? num(a.YEAR_BUILT) : null,
      market_value: num(a.MKT_VALUE) || null,
    };
  }
  return null;
}

async function hcad(db, lat, lng, fallbackAcct) {
  const url = `https://arcweb.hcad.org/server/rest/services/public/public_query/MapServer/0/query?geometry=${lng},${lat}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=false&f=json`;
  let d = await fetchJson(url);
  if (!d?.features?.length) d = await fetchJson(url + "&distance=25&units=esriSRUnit_Meter");
  const a = d?.features?.[0]?.attributes || {};
  const acct = String(a.HCAD_NUM || fallbackAcct || "").trim();
  if (!acct) return null;
  const row = await db.prepare("SELECT * FROM hcad_buildings WHERE acct = ?").bind(acct).first().catch(() => null);
  return { acct, owner: a.owner, state_class: a.state_class, row };
}

let tpTokens = {};
async function trueProdigy(office, propId) {
  const pid = Number(String(propId).replace(/\D/g, ""));
  if (!pid) return null;
  const now = Date.now();
  if (!tpTokens[office] || tpTokens[office].exp < now) {
    const t = await fetchJson("https://prod-container.trueprodigyapi.com/trueprodigy/cadpublic/auth/token", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ office }) });
    const token = t?.user?.token || t?.token;
    if (!token) return null;
    tpTokens[office] = { token, exp: now + 4 * 60 * 1000 };
  }
  const token = tpTokens[office].token;
  const year = String(new Date().getFullYear() >= 2027 ? new Date().getFullYear() - 1 : new Date().getFullYear());
  const search = await fetchJson("https://prod-container.trueprodigyapi.com/public/property/search?page=1&pageSize=5", {
    method: "POST",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify({ pid: { operator: "in", value: [pid] }, pYear: { operator: "=", value: year } }),
  });
  const res = search?.results?.[0];
  if (!res) return null;
  const imp = await fetchJson(`https://prod-container.trueprodigyapi.com/public/propertyaccount/${res.pAccountID}/improvement`, { headers: { Authorization: token } });
  let gba = 0, yb = null, cls = null, desc = null;
  for (const i of imp?.results || []) {
    // livingArea is the building's main area; GBA also counts paving, ramps and canopies.
    gba += Number(i.livingArea || 0) || Number(i.grossBuildingArea || 0);
    desc = desc || i.imprvDescription || null;
    for (const dt of (i.details || []).filter((x) => !/^(CON|ASP|OP|RAM|CNP|PAV|FEN|LT)$/i.test(x.imprvDetailType || ""))) {
      const y = Number(dt.actualYearBuilt);
      if (y > 1800 && (!yb || (dt.area || 0) > (yb.area || 0))) yb = { y, area: dt.area || 0 };
      cls = cls || dt.class || null;
    }
  }
  return { owner: res.name, acres: Number(res.legalAcreage) || null, market_value: res.marketValue || null, use: res.useCd || null, building_sf: gba || null, year_built: yb?.y || null, cad_class: cls, description: desc };
}

async function collin(lat, lng) {
  const base = `https://services2.arcgis.com/uXyoacYrZTPTKD3R/arcgis/rest/services/CCAD_Parcel_Feature_Set/FeatureServer/4/query?geometry=${lng},${lat}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=false&f=json`;
  let d = await fetchJson(base);
  if (!d?.features?.length) d = await fetchJson(base + "&distance=25&units=esriSRUnit_Meter");
  const a = d?.features?.[0]?.attributes;
  if (!a) return null;
  return { owner: a.ownerName, acres: a.landSizeAcres || null, building_sf: a.ecoGroupGBA || a.imprvMainArea || null, year_built: a.imprvYearBuilt > 1800 ? a.imprvYearBuilt : null, cad_class: a.imprvClassCd || null, use: a.propCategoryCode || null };
}

async function dcad(lat, lng) {
  const url = `https://maps.dcad.org/prdwa/rest/services/Property/ParcelQuery/MapServer/4/query?geometry=${lng},${lat}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&outFields=*&returnGeometry=false&f=json`;
  let d = await fetchJson(url);
  if (!d?.features?.length) d = await fetchJson(url + "&distance=25&units=esriSRUnit_Meter");
  const feats = (d?.features || []).map((f) => f.attributes).filter((a) => a.USEDSCRP !== "BPP");
  const a = feats[0];
  if (!a) return null;
  const id = a.LOWPARCELID || a.PARCELID;
  const html = id ? await fetchText(`https://www.dallascad.org/AcctDetailCom.aspx?ID=${encodeURIComponent(id)}`) : null;
  const txt = html ? html.replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ") : "";
  const area = /Total Area:\s*([\d,]+)\s*sq/i.exec(txt) || /Gross Bldg Area:\s*([\d,]+)/i.exec(txt);
  const yb = /Year Built:\s*(\d{4})/i.exec(txt);
  const qual = /Quality:\s*([A-Z ]+?)\s{1,}(?:Condition|Construction|Desc)/i.exec(txt);
  const cons = /Construction:\s*([A-Z ,\-]+?)\s{1,}(?:Foundation|Heating|Year|Desc|Quality)/i.exec(txt);
  const landSf = /Area\s*([\d,]+(?:\.\d+)?)\s*SQUARE FEET/i.exec(txt);
  const acresM = /ACS\s*([\d.]+)/i.exec(txt);
  return {
    owner: a.OWNERNME1 || null,
    building_sf: area ? Number(area[1].replace(/,/g, "")) : null,
    year_built: yb ? Number(yb[1]) : null,
    cad_class: qual ? qual[1].trim() : null,
    construction_text: cons ? cons[1].trim() : null,
    acres: landSf ? +(Number(landSf[1].replace(/,/g, "")) / 43560).toFixed(3) : acresM ? Number(acresM[1]) : null,
    account: id,
  };
}

function constructionFromText(t) {
  if (!t) return null;
  if (/tilt/i.test(t)) return "tilt_wall";
  if (/metal|steel/i.test(t)) return "metal";
  if (/block|cmu/i.test(t)) return "concrete_block";
  if (/masonry|brick/i.test(t)) return "masonry";
  return null;
}

// County building-type text -> Harbor product type / construction (low confidence).
export function cadUseToFeatures(text) {
  const t = String(text || "").toLowerCase();
  const out = {};
  if (/manufactur|heavy industrial|plant|fabricat/.test(t)) out.property_subtype = "manufacturing";
  else if (/cold storage|refriger|freezer/.test(t)) out.property_subtype = "cold_storage";
  else if (/distribution/.test(t)) out.property_subtype = "warehouse"; // CAD "distribution warehouse" covers every size; bulk is inferred from SF
  else if (/office[\s-]*warehouse|flex|showroom|r&d/.test(t)) out.property_subtype = "flex";
  else if (/truck terminal|terminal/.test(t)) out.property_subtype = "ios";
  else if (/warehouse|storage|industrial/.test(t) && !/mini/.test(t)) out.property_subtype = "warehouse";
  if (/metal/.test(t)) out.construction = "metal";
  else if (/tilt/.test(t)) out.construction = "tilt_wall";
  return out;
}

export async function parcelObservations(db, prop) {
  if (prop.latitude == null) return { obs: [], meta: null };
  const lat = prop.latitude, lng = prop.longitude;
  const sm = await stratmapParcel(lat, lng);
  const obs = [];
  const meta = { county: sm?.county || prop.county || null, parcel_id: sm?.prop_id || null, owner: sm?.owner || null };
  const add = (field, value, ref, source = "cad") => value != null && value !== "" && value !== 0 && obs.push({ field, value, source, source_ref: ref });
  if (sm) {
    add("land_acres", sm.acres, `StratMap parcel ${sm.prop_id || ""} (${sm.county || ""})`, "parcel");
    if (sm.year_built) add("year_built", sm.year_built, `StratMap parcel ${sm.prop_id}`, "parcel");
  }
  const county = meta.county;
  try {
    if (county === "Harris" && db) {
      const h = await hcad(db, lat, lng, sm?.prop_id && /^\d{13}$/.test(String(sm.prop_id)) ? String(sm.prop_id) : null);
      if (h) {
        meta.parcel_id = h.acct;
        meta.owner = h.owner || meta.owner;
        const r = h.row;
        if (r) {
          const ref = `HCAD ${h.acct} (${r.bldg_type || r.state_class})`;
          add("building_sf", r.bldg_sf, ref);
          add("year_built", r.year_built, ref);
          add("land_acres", r.land_acres, ref);
          if (r.num_bldgs > 1) add("notes_cad", `${r.num_bldgs} buildings on HCAD account`, ref);
          if (r.quality) add("cad_quality", r.quality, ref);
          if (r.bldg_type) {
            add("cad_use", r.bldg_type, ref);
            for (const [f, v] of Object.entries(cadUseToFeatures(r.bldg_type))) obs.push({ field: f, value: v, source: "cad", source_ref: ref, confidence: 0.5 });
          }
        }
      }
    } else if (county === "Dallas") {
      const d = await dcad(lat, lng);
      if (d) {
        const ref = `DCAD ${d.account}`;
        meta.parcel_id = d.account;
        add("building_sf", d.building_sf, ref);
        add("year_built", d.year_built, ref);
        add("land_acres", d.acres, ref);
        add("construction", constructionFromText(d.construction_text), ref);
        if (d.cad_class) add("cad_quality", d.cad_class, ref);
      }
    } else if (county === "Collin") {
      const c = await collin(lat, lng);
      if (c) {
        const ref = "CCAD parcel";
        add("building_sf", c.building_sf, ref);
        add("year_built", c.year_built, ref);
        add("land_acres", c.acres, ref);
      }
    } else if (TP_OFFICES[county] && sm?.prop_id) {
      const t = await trueProdigy(TP_OFFICES[county], sm.prop_id);
      if (t) {
        const ref = `${county} CAD ${sm.prop_id}`;
        add("building_sf", t.building_sf, ref);
        add("year_built", t.year_built, ref);
        add("land_acres", t.acres, ref);
        if (t.cad_class) add("cad_quality", t.cad_class, ref);
        if (t.description) add("cad_use", t.description, ref);
        for (const [f, v] of Object.entries(cadUseToFeatures(`${t.description || ""} ${t.cad_class || ""}`))) obs.push({ field: f, value: v, source: "cad", source_ref: ref, confidence: 0.45 });
      }
    }
  } catch (e) {
    meta.error = String(e.message || e);
  }
  return { obs, meta };
}

// ---------------- Aerial imagery (vision) ----------------
export async function aerialObservations(env, llm, prop, debug = null) {
  if (prop.latitude == null || !llm) return [];
  // Google Static Maps if enabled on the key, else Esri World Imagery export (keyless).
  let r = null;
  if (env.GOOGLE_API_KEY) {
    r = await fetch(`https://maps.googleapis.com/maps/api/staticmap?center=${prop.latitude},${prop.longitude}&zoom=18&size=640x640&scale=1&maptype=satellite&key=${env.GOOGLE_API_KEY}`).catch(() => null);
    if (r && (!r.ok || !(r.headers.get("content-type") || "").startsWith("image"))) r = null;
  }
  if (!r) {
    const dLat = 0.0017, dLng = 0.0017 / Math.cos((prop.latitude * Math.PI) / 180);
    const bb = [prop.longitude - dLng, prop.latitude - dLat, prop.longitude + dLng, prop.latitude + dLat].join(",");
    r = await fetch(`https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export?bbox=${bb}&bboxSR=4326&imageSR=3857&size=640,640&format=jpg&f=image`).catch(() => null);
  }
  if (debug) debug.image = r ? `${r.status} ${r.headers.get("content-type")}` : "none";
  if (!r || !r.ok) return [];
  const buf = new Uint8Array(await r.arrayBuffer());
  let bin = "";
  for (let i = 0; i < buf.length; i += 8192) bin += String.fromCharCode(...buf.subarray(i, i + 8192));
  const dataUrl = `data:${r.headers.get("content-type") || "image/jpeg"};base64,${btoa(bin)}`;
  const out = await llm
    .chat({
      tier: "vision",
      reasoning: "off",
      maxTokens: 800,
      system: "You are an industrial real estate analyst reading a satellite image (zoom 18, ~0.6 m/pixel, 640x640 px, north up) centered on a property. Be conservative: answer null when unsure. Return JSON only.",
      user: `The building at the CENTER of the image is ${prop.address || prop.property_name || "the subject"}${prop.building_sf ? ` (~${Math.round(prop.building_sf).toLocaleString()} SF)` : ""}.
Return {"loading": "rear|front|cross_dock|side|null" (where the truck court / dock doors are relative to the street; cross_dock = dock doors on two opposite sides), "dock_doors_visible": integer or null (count only clearly visible dock doors/trailers backed in), "trailer_parking": "none|some|large" or null, "outdoor_storage": true/false/null (paved/gravel yard with stored equipment, containers, trailers beyond normal parking), "yard_share": "fraction of the parcel that is open yard, 0-1" or null, "rail_spur": true/false/null, "multi_tenant": true/false/null (many small doors / storefronts), "confidence": 0-1, "notes": "one short sentence"}`,
      images: [dataUrl],
    })
    .catch((e) => { if (debug) debug.llm_error = String(e.message || e).slice(0, 300); return null; });
  if (debug) debug.vision = out;
  if (!out || (out.confidence ?? 0) < 0.45) return [];
  const obs = [];
  const ref = `satellite ${prop.latitude.toFixed(5)},${prop.longitude.toFixed(5)} (conf ${out.confidence})`;
  if (["rear", "front", "cross_dock", "side"].includes(out.loading)) obs.push({ field: "loading", value: out.loading, source: "aerial", source_ref: ref });
  if (typeof out.rail_spur === "boolean") obs.push({ field: "rail_served", value: out.rail_spur, source: "aerial", source_ref: ref });
  if (out.trailer_parking && out.trailer_parking !== "none") obs.push({ field: "trailer_parking_text", value: out.trailer_parking, source: "aerial", source_ref: ref });
  if (out.outdoor_storage === true) obs.push({ field: "outdoor_storage", value: true, source: "aerial", source_ref: ref });
  if (typeof out.multi_tenant === "boolean") obs.push({ field: "tenancy", value: out.multi_tenant ? "multi" : "single", source: "aerial", source_ref: ref });
  if (out.notes) obs.push({ field: "aerial_notes", value: out.notes, source: "aerial", source_ref: ref });
  return obs;
}

// ---------------- Web listings (LLM + web search) ----------------
export async function webObservations(env, prop) {
  if (!env.OPENAI_API_KEY) return [];
  const addr = [prop.address, prop.city, "TX", prop.zip].filter(Boolean).join(", ");
  if (!addr || !/\d/.test(prop.address || "")) return [];
  const body = {
    model: env.WEB_MODEL || "gpt-4.1-mini",
    tools: [{ type: "web_search_preview", search_context_size: "low" }],
    input: `Find the industrial building at ${addr}${prop.property_name ? ` (${prop.property_name})` : ""} on listing sites (LoopNet, Crexi, CommercialCafe, broker flyers, county records). Report ONLY specs you actually found for THIS address. Reply with JSON only: {"clear_height_ft": number|null, "dock_doors": int|null, "drive_in_doors": int|null, "building_sf": number|null, "land_acres": number|null, "year_built": int|null, "construction": "tilt_wall|metal|masonry|concrete_block|null", "loading": "rear|front|cross_dock|side|null", "sprinkler": "ESFR|wet|none|null", "rail_served": bool|null, "office_sf": number|null, "sources": ["url", ...]}`,
  };
  const r = await fetch("https://api.openai.com/v1/responses", { method: "POST", headers: { Authorization: `Bearer ${env.OPENAI_API_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify(body) }).catch(() => null);
  if (!r || !r.ok) return [];
  const d = await r.json();
  const text = (d.output || []).flatMap((o) => o.content || []).map((c) => c.text || "").join("");
  let j;
  try {
    j = JSON.parse(text.replace(/^```(json)?|```$/g, "").trim().replace(/^[^{]*/, "").replace(/[^}]*$/, ""));
  } catch {
    return [];
  }
  const src = (j.sources || []).filter((u) => /^https?:/.test(u)).slice(0, 3);
  if (!src.length) return [];
  const ref = src.join(" ");
  const obs = [];
  for (const f of ["clear_height_ft", "dock_doors", "drive_in_doors", "building_sf", "land_acres", "year_built", "construction", "loading", "sprinkler", "rail_served", "office_sf"]) {
    if (j[f] != null && j[f] !== "null") obs.push({ field: f, value: j[f], source: "web", source_ref: ref });
  }
  return obs;
}

// Choose the winning value per field from all observations.
export function resolveFeatures(observations) {
  const best = {};
  const conflicts = {};
  // Physically impossible observations are dropped before voting: land that is
  // smaller than the building footprint (parcel lookups that hit a sliver /
  // neighbouring lot), clear heights or vintages outside reality.
  const sfObs = observations.filter((o) => o.field === "building_sf" && Number(o.value) > 0).sort((a, b) => (b.confidence ?? confFor(b.source, b.field)) - (a.confidence ?? confFor(a.source, a.field)));
  const bldg = sfObs.length ? Number(sfObs[0].value) : null;
  observations = observations.filter((o) => {
    const v = Number(o.value);
    if (o.field === "land_acres" && bldg && v > 0 && bldg / (v * 43560) > 1.05) return false;
    if (o.field === "clear_height_ft" && (v < 8 || v > 60)) return false;
    if (o.field === "year_built" && (v < 1850 || v > new Date().getFullYear() + 3)) return false;
    return true;
  });
  for (const o of observations) {
    const c = o.confidence ?? confFor(o.source, o.field);
    const cur = best[o.field];
    if (!cur || c > cur.confidence || (c === cur.confidence && (o.observed_at || "") > (cur.at || ""))) {
      best[o.field] = { value: o.value, source: o.source, ref: o.source_ref, confidence: c, at: o.observed_at || null };
    }
  }
  // flag material disagreements
  for (const o of observations) {
    const b = best[o.field];
    if (!b || o.source === b.source) continue;
    const x = Number(o.value), y = Number(b.value);
    let disagree = false;
    if (Number.isFinite(x) && Number.isFinite(y)) {
      const tol = { clear_height_ft: 2, year_built: 3, dock_doors: 2, drive_in_doors: 1 }[o.field];
      disagree = tol != null ? Math.abs(x - y) > tol : Math.abs(x - y) / Math.max(Math.abs(x), Math.abs(y), 1) > 0.15;
    } else disagree = String(o.value).toLowerCase() !== String(b.value).toLowerCase();
    if (disagree) (conflicts[o.field] = conflicts[o.field] || []).push({ value: o.value, source: o.source, ref: o.source_ref });
  }
  return { best, conflicts };
}
