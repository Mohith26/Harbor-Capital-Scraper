// Address normalization, distance math and geocoding.

const SUFFIX = {
  street: "st", st: "st", str: "st", road: "rd", rd: "rd", drive: "dr", dr: "dr", drv: "dr", boulevard: "blvd", blvd: "blvd", blv: "blvd",
  parkway: "pkwy", pkwy: "pkwy", pky: "pkwy", pkway: "pkwy", freeway: "fwy", fwy: "fwy", frwy: "fwy", highway: "hwy", hwy: "hwy", hiway: "hwy",
  lane: "ln", ln: "ln", avenue: "ave", ave: "ave", av: "ave", court: "ct", ct: "ct", circle: "cir", cir: "cir", place: "pl", pl: "pl",
  trail: "trl", trl: "trl", way: "way", loop: "loop", lp: "loop", expressway: "expy", expy: "expy", terrace: "ter", plaza: "plz", square: "sq",
  crossing: "xing", xing: "xing", run: "run", row: "row", pass: "pass", pike: "pike", path: "path", point: "pt", bend: "bnd", cove: "cv",
};
const DIRS = { north: "n", south: "s", east: "e", west: "w", northeast: "ne", northwest: "nw", southeast: "se", southwest: "sw", n: "n", s: "s", e: "e", w: "w", ne: "ne", nw: "nw", se: "se", sw: "sw" };

function preClean(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/\bfarm[\s-]*to[\s-]*market(?:\s+road)?\b|\bf\.?m\.?\s*(?:road|rd)?\s*(?=\d)/g, "fm ")
    .replace(/\branch[\s-]*to[\s-]*market(?:\s+road)?\b|\br\.?m\.?\s*(?=\d)/g, "rm ")
    .replace(/\b(?:interstate(?:\s+highway)?|ih|i)[\s-]*(\d+)\s*([ew])?\b/g, (m, n, d) => `i${n}${d || ""}`)
    .replace(/\bu\.?s\.?\s*(?:highway|hwy|route)?\s*(\d+)/g, "us$1")
    .replace(/\b(?:state\s+highway|state\s+hwy|sh|tx)[\s-]*(\d+)/g, "sh$1")
    .replace(/\b(?:county\s+road|cr)[\s-]*(\d+)/g, "cr$1")
    .replace(/\bbeltway\s*8\b|\bbw\s*8\b|\bsam\s+houston\s+(?:pkwy|pky|parkway|tollway|tlwy)\b\.?/g, "bw8")
    .replace(/\bloop\s*(\d+)/g, "loop$1")
    .replace(/\bspur\s*(\d+)/g, "spur$1")
    .replace(/\b(fm|rm|cr|sh|us|loop|spur|hwy|highway)[\s-]+(\d+)/g, "$1 $2")
    .replace(/#/g, " unit ")
    .replace(/[.,]/g, " ");
}

// Split an address into components and a stable identity key.
export function parseAddress(raw) {
  const out = { raw: raw || "", numbers: [], street: "", unit: null, building: null, key: "", intersection: false };
  if (!raw) return out;
  // Building / suite designators anywhere in the string (", Building 4", "Bldg B", "Ste 160").
  const full = preClean(raw);
  const bmAll = /\b(?:bldg|building|bld)\.?\s*#?\s*([a-z0-9-]+)\b/.exec(full);
  const umAll = /\b(?:suite|ste|unit|space|spc)\.?\s*#?\s*([a-z0-9-]+)\b/.exec(full);
  // Keep only the street part: "1400 Schlipf Road, Katy, TX 77493" -> "1400 Schlipf Road".
  let rawStreet = String(raw);
  const segs = rawStreet.split(",").map((x) => x.trim()).filter(Boolean);
  if (segs.length > 1) {
    const withNum = segs.find((x) => /^\d/.test(x));
    rawStreet = withNum || segs[0];
  }
  // "2400 FM 156 and Mobility Way" -> first street of a corner description
  if (/^\s*\d/.test(rawStreet)) rawStreet = rawStreet.replace(/\s+(?:and|&|at|@)\s+(?![\d])[a-z].*$/i, "");
  let s = preClean(rawStreet);
  // building / suite designators
  const bm = /\b(?:bldg|building|bld)\s*([a-z0-9-]+)\b/.exec(s);
  if (bm) {
    out.building = bm[1];
    s = s.replace(bm[0], " ");
  }
  const um = /\b(?:suite|ste|unit|space|spc)\s*([a-z0-9-]+)\b/.exec(s);
  if (um) {
    out.unit = um[1];
    s = s.replace(um[0], " ");
  }
  // drop trailing city/state/zip after a comma-free street (best effort)
  s = s.replace(/\b(tx|texas)\b\s*\d{5}(-\d{4})?\s*$/, " ").replace(/\b\d{5}(-\d{4})?\s*$/, " ");
  if (/\s(&|and|at|@)\s|\//.test(s) && !/^\s*\d/.test(s)) out.intersection = true;
  // leading house numbers incl ranges / lists: "5851-5861", "625 & 633", "200,201,206"
  const nm = /^\s*((?:\d+[a-z]?\s*(?:[-&/]|and|,)\s*)*\d+[a-z]?)\b\s*/.exec(s);
  if (nm) {
    const nums = nm[1].match(/\d+/g).map(Number);
    if (nums.length === 2 && /-/.test(nm[1]) && nums[1] > nums[0] && nums[1] - nums[0] < 2000) out.numbers = [nums[0], nums[1]];
    else out.numbers = nums;
    s = s.slice(nm[0].length);
  }
  const words = s
    .replace(/[^a-z0-9\s-]/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => DIRS[w] || SUFFIX[w] || w);
  out.street = words.join(" ").trim();
  // core street name without suffix / direction for fuzzy comparison
  const core = words.filter((w) => !Object.values(DIRS).includes(w) && !Object.values(SUFFIX).includes(w));
  out.core = core.join(" ") || words.join(" ");
  if (!out.building && bmAll) out.building = bmAll[1];
  if (!out.unit && umAll) out.unit = umAll[1];
  out.key = `${out.numbers.length ? out.numbers[0] : ""}|${out.core}${out.building ? "|b" + out.building : ""}`;
  return out;
}

export function addrSimilarity(a, b) {
  const A = typeof a === "string" ? parseAddress(a) : a;
  const B = typeof b === "string" ? parseAddress(b) : b;
  if (!A.core || !B.core) return 0;
  let numScore = 0;
  if (A.numbers.length && B.numbers.length) {
    const ra = [Math.min(...A.numbers), Math.max(...A.numbers)], rb = [Math.min(...B.numbers), Math.max(...B.numbers)];
    if (A.numbers.some((n) => B.numbers.includes(n))) numScore = 1;
    else if (ra[0] <= rb[1] && rb[0] <= ra[1]) numScore = 0.9; // overlapping ranges
    else numScore = 0;
  } else numScore = 0.5;
  const ta = new Set(A.core.split(" ")), tb = new Set(B.core.split(" "));
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  const streetScore = inter / Math.max(ta.size, tb.size);
  const bldg = A.building && B.building && A.building !== B.building ? 0.5 : 1;
  return numScore * 0.5 * bldg + streetScore * 0.5 * bldg;
}

export function haversineMi(lat1, lon1, lat2, lon2) {
  const R = 3958.8;
  const toR = Math.PI / 180;
  const dLat = (lat2 - lat1) * toR, dLon = (lon2 - lon1) * toR;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * toR) * Math.cos(lat2 * toR) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

export function bbox(lat, lng, miles) {
  const dLat = miles / 69.0;
  const dLng = miles / (69.17 * Math.cos((lat * Math.PI) / 180));
  return { minLat: lat - dLat, maxLat: lat + dLat, minLng: lng - dLng, maxLng: lng + dLng };
}

export const TX_BOUNDS = { minLat: 25.8, maxLat: 36.6, minLng: -106.7, maxLng: -93.5 };
export function inTexas(lat, lng) {
  return lat >= TX_BOUNDS.minLat && lat <= TX_BOUNDS.maxLat && lng >= TX_BOUNDS.minLng && lng <= TX_BOUNDS.maxLng;
}

export const MARKETS = {
  Houston: { lat: 29.76, lng: -95.37, r: 60, counties: ["Harris", "Fort Bend", "Montgomery", "Brazoria", "Galveston", "Waller", "Chambers", "Liberty"] },
  DFW: { lat: 32.82, lng: -96.98, r: 60, counties: ["Dallas", "Tarrant", "Collin", "Denton", "Ellis", "Kaufman", "Rockwall", "Johnson", "Parker", "Wise", "Hunt"] },
  "San Antonio": { lat: 29.45, lng: -98.5, r: 45, counties: ["Bexar", "Comal", "Guadalupe", "Medina", "Wilson", "Atascosa"] },
  Austin: { lat: 30.3, lng: -97.74, r: 45, counties: ["Travis", "Williamson", "Hays", "Bastrop", "Caldwell"] },
  Laredo: { lat: 27.55, lng: -99.48, r: 30, counties: ["Webb"] },
  "El Paso": { lat: 31.78, lng: -106.4, r: 30, counties: ["El Paso"] },
};

export function marketFor(lat, lng) {
  if (lat == null || lng == null) return null;
  let best = null;
  for (const [name, m] of Object.entries(MARKETS)) {
    const d = haversineMi(lat, lng, m.lat, m.lng);
    if (d <= m.r && (!best || d < best.d)) best = { name, d };
  }
  return best ? best.name : "Other TX";
}

// Quality labels, best to worst.
export const GEO_QUALITY = ["source", "rooftop", "parcel", "interpolated", "intersection", "street", "postal", "city", "failed"];

// Geocode with Google (if key) then Esri World (keyless). Never overwrites the
// address the broker wrote: returns the formatted address separately.
export async function geocode(env, { address, city, state = "TX", zip, propertyName }, { cache } = {}) {
  const parts = [address, city, state, zip].filter(Boolean);
  if (!address && propertyName) parts.unshift(propertyName);
  const q = parts.join(", ");
  if (!q.trim()) return { quality: "failed", reason: "empty" };
  if (cache) {
    const hit = await cache.get(q);
    if (hit) return hit;
  }
  let res = null;
  if (env.GOOGLE_API_KEY) res = await googleGeocode(env.GOOGLE_API_KEY, q, { zip, city }).catch(() => null);
  if (!res || res.quality === "failed" || res.quality === "city" || res.quality === "postal") {
    const esri = await esriGeocode(q).catch(() => null);
    if (esri && (!res || GEO_QUALITY.indexOf(esri.quality) < GEO_QUALITY.indexOf(res.quality))) res = esri;
  }
  res = res || { quality: "failed" };
  if (res.lat != null && !inTexas(res.lat, res.lng)) res = { quality: "failed", reason: "outside Texas", candidate: res.formatted };
  if (cache && res.quality !== "failed") await cache.put(q, res);
  return { ...res, query: q };
}

async function googleGeocode(key, q, { zip }) {
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(q)}&components=country:US|administrative_area:TX&key=${key}`;
  const r = await fetch(url);
  const d = await r.json();
  if (d.status !== "OK" || !d.results?.length) return { quality: "failed", reason: d.status };
  const g = d.results[0];
  const types = g.types || [];
  const lt = g.geometry.location_type;
  let quality = "street";
  if (types.includes("street_address") || types.includes("premise") || types.includes("subpremise")) quality = lt === "ROOFTOP" ? "rooftop" : "interpolated";
  else if (types.includes("intersection")) quality = "intersection";
  else if (types.includes("route")) quality = "street";
  else if (types.includes("postal_code")) quality = "postal";
  else if (types.includes("locality") || types.includes("administrative_area_level_2") || types.includes("administrative_area_level_1")) quality = "city";
  else if (types.includes("establishment") || types.includes("point_of_interest")) quality = lt === "ROOFTOP" ? "rooftop" : "interpolated";
  if (g.partial_match && quality === "rooftop") quality = "interpolated";
  const comp = (t) => g.address_components.find((c) => c.types.includes(t));
  const outZip = comp("postal_code")?.short_name || null;
  // A partial match landing in a different zip than the broker gave is suspicious.
  if (zip && outZip && zip !== outZip && quality !== "rooftop") quality = quality === "interpolated" ? "street" : quality;
  return {
    lat: g.geometry.location.lat,
    lng: g.geometry.location.lng,
    quality,
    formatted: g.formatted_address,
    city: comp("locality")?.long_name || comp("sublocality")?.long_name || null,
    county: (comp("administrative_area_level_2")?.long_name || "").replace(/ County$/, "") || null,
    zip: outZip,
    source: "google",
  };
}

async function esriGeocode(q) {
  const url = `https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?SingleLine=${encodeURIComponent(q)}&countryCode=USA&outFields=Addr_type,City,Subregion,Postal,Score&maxLocations=1&f=json`;
  const r = await fetch(url);
  const d = await r.json();
  const c = d.candidates?.[0];
  if (!c || c.score < 75) return { quality: "failed", reason: "no esri match" };
  const t = c.attributes?.Addr_type;
  const quality = { PointAddress: "rooftop", Subaddress: "rooftop", Parcel: "parcel", StreetAddress: "interpolated", StreetInt: "intersection", StreetName: "street", Postal: "postal", PostalExt: "postal", Locality: "city", POI: "interpolated" }[t] || "city";
  return { lat: c.location.y, lng: c.location.x, quality, formatted: c.address, city: c.attributes.City || null, county: (c.attributes.Subregion || "").replace(/ County$/, "") || null, zip: c.attributes.Postal || null, source: "esri", score: c.score };
}
