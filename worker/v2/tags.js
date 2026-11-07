// Product tags: the analyst vocabulary partners use to pick comps
// ("crane-served", "IOS", "shallow bay", "cross-dock", "user sale"...).
// Derived from resolved building features plus the text that came with the
// comps (notes, property name, the broker file's own title). Stored on
// v2_properties.product_tags as '|tag|tag|' so SQL can filter with LIKE.

export const TAGS = {
  crane: "Crane-served",
  heavy_power: "Heavy power",
  manufacturing: "Manufacturing",
  ios: "IOS / yard",
  yard: "Yard / laydown",
  truck_terminal: "Truck terminal",
  rail: "Rail",
  shallow_bay: "Shallow bay",
  multi_tenant: "Multi-tenant",
  flex: "Flex",
  bulk: "Bulk / big box",
  cross_dock: "Cross-dock",
  rear_load: "Rear-load",
  front_load: "Front-load",
  cold_storage: "Cold storage",
  service: "Service center",
};

const NO = /^(no|none|n\/a|0|false)$/i;

export function productTags(p, text = "") {
  const t = new Set();
  const s = `${p.property_name || ""} ${text || ""}`.toLowerCase();
  const sub = p.property_subtype;
  const sf = Number(p.building_sf) || null;
  const ac = Number(p.land_acres) || null;
  // cranes
  if ((p.cranes && !NO.test(String(p.cranes).trim())) || /\bcranes?\b|crane[- ](served|ready)|\d+\s*-?\s*ton\b/.test(s.replace(/no cranes?/g, ""))) t.add("crane");
  // power
  if ((p.power && /\d{3,5}\s*(a\b|amps?)|kva|3[- ]?phase|three[- ]phase|heavy|480/i.test(String(p.power))) || /heavy power|\b\d{3,5}\s*amps?\b|\bkva\b|3[- ]phase|three[- ]phase/.test(s)) t.add("heavy_power");
  if (sub === "manufacturing" || /manufactur|fabricat|machine shop|food processing/.test(s)) t.add("manufacturing");
  // outdoor storage
  const coverage = sf && ac ? sf / (ac * 43560) : null;
  if (sub === "ios" || /\bios\b|outdoor storage|truck (yard|parking)|trailer (storage|parking lot)|equipment yard/.test(s) || (coverage != null && coverage < 0.12 && ac >= 1.5 && sf < 50000)) t.add("ios");
  if ((p.yard_acres && Number(p.yard_acres) > 0.4) || /\byard\b|laydown|lay-down|stabilized|outside storage/.test(s)) t.add("yard");
  if (/truck terminal|cross[- ]dock terminal|\bterminal\b/.test(s) && !/airport/.test(s)) t.add("truck_terminal");
  if (p.rail_served === 1 || p.rail_served === true || /rail[- ]served|\brail spur\b|\bspur\b/.test(s)) t.add("rail");
  // building shape
  if (["shallow_bay", "multi_tenant", "flex", "service"].includes(sub) || /shallow[- ]bay|small[- ]bay|service center|business park|tech center/.test(s)) t.add("shallow_bay");
  if (sub === "multi_tenant" || p.tenancy === "multi" || /multi[- ]tenant/.test(s)) t.add("multi_tenant");
  if (sub === "flex" || /\bflex\b|showroom|r&d/.test(s)) t.add("flex");
  if (sub === "service" || /service center/.test(s)) t.add("service");
  if (sub === "bulk_distribution" || (sf && sf >= 250000) || /big[- ]box|\bbulk\b|distribution center/.test(s)) t.add("bulk");
  if (p.loading === "cross_dock" || /cross[- ]dock/.test(s)) t.add("cross_dock");
  if (p.loading === "rear" || /rear[- ]load/.test(s)) t.add("rear_load");
  if (p.loading === "front" || /front[- ]load/.test(s)) t.add("front_load");
  if (sub === "cold_storage" || /cold storage|freezer|refrigerat/.test(s)) t.add("cold_storage");
  return t.size ? `|${[...t].join("|")}|` : null;
}

export function tagList(s) {
  return String(s || "").split("|").filter(Boolean);
}

// Subject tags from a write-up (same vocabulary).
export function subjectTags(subject, text = "") {
  return tagList(productTags({ ...subject }, text));
}
