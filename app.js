import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";
import anime from "https://cdn.jsdelivr.net/npm/animejs@3.2.1/+esm";
const RM = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
// only run reveal animations when motion is allowed AND the tab is visible — otherwise
// requestAnimationFrame is paused and opacity:0 starts would leave content stuck blank.
const canAnimate = () => !RM && document.visibilityState === "visible";

/* ---------- vocab: life_form → Hebrew label / icon / placeholder palette ---------- */
const LIFE = {
  tree:      {he:"עץ",        ic:"🌳", hue:128, t:"tree"},
  shrub:     {he:"שיח",       ic:"🪴", hue:110, t:"shrub"},
  perennial: {he:"רב-שנתי",   ic:"🌿", hue:96,  t:"flower"},
  annual:    {he:"חד-שנתי",   ic:"🌼", hue:74,  t:"flower"},
  bulb:      {he:"גיאופיט",   ic:"🌷", hue:280, t:"bulb"},
  climber:   {he:"מטפס",      ic:"🍃", hue:140, t:"vine"},
  succulent: {he:"בשרני",     ic:"🌵", hue:165, t:"succulent"},
  fern:      {he:"שרך",       ic:"🌿", hue:150, t:"fern"},
  grass:     {he:"דגני",      ic:"🌾", hue:54,  t:"flower"},
  palm:      {he:"דקל",       ic:"🌴", hue:120, t:"tree"},
  aquatic:   {he:"מים",       ic:"💧", hue:195, t:"flower"},
};
const lifeHe = v => (LIFE[v]?.he) || "—";
const INVAS = {listed:{he:"פולש",cls:"listed"}, potential:{he:"פולש פוטנציאלי",cls:"potential"}, not_listed:null};
const MONTHS = ["ינואר","פברואר","מרץ","אפריל","מאי","יוני","יולי","אוגוסט","ספטמבר","אוקטובר","נובמבר","דצמבר"];

/* ---------- botanical placeholder SVG (no photos in the dataset) ---------- */
function placeholder(lifeForm, key){
  const L = LIFE[lifeForm] || {hue:100, t:"leaf"};
  let h=0; for(const c of (key||"")) h=(h*31+c.charCodeAt(0))%360;
  const hue = L.hue + (h%18) - 9;
  const bg1=`hsl(${hue} 30% 90%)`, bg2=`hsl(${hue} 26% 80%)`, fg=`hsl(${hue} 38% 38%)`, fg2=`hsl(${hue} 34% 52%)`;
  const motif = {
    tree:`<circle cx="100" cy="48" r="30" fill="${fg2}"/><circle cx="78" cy="60" r="22" fill="${fg}"/><circle cx="122" cy="60" r="22" fill="${fg}"/><rect x="96" y="64" width="8" height="34" fill="${fg}"/>`,
    shrub:`<circle cx="80" cy="74" r="20" fill="${fg2}"/><circle cx="118" cy="74" r="20" fill="${fg2}"/><circle cx="100" cy="58" r="24" fill="${fg}"/>`,
    flower:`<path d="M100 96 V58" stroke="${fg}" stroke-width="4"/><g fill="${fg2}"><circle cx="100" cy="46" r="11"/><circle cx="84" cy="54" r="11"/><circle cx="116" cy="54" r="11"/><circle cx="90" cy="40" r="11"/><circle cx="110" cy="40" r="11"/></g><circle cx="100" cy="48" r="7" fill="${fg}"/>`,
    bulb:`<path d="M100 98 V52" stroke="${fg}" stroke-width="4"/><path d="M100 50 C86 50 84 30 100 26 C116 30 114 50 100 50Z" fill="${fg2}"/><path d="M100 50 C92 50 90 36 100 32 C110 36 108 50 100 50Z" fill="${fg}"/>`,
    vine:`<path d="M60 30 C120 40 80 70 140 86" stroke="${fg}" stroke-width="3" fill="none"/><g fill="${fg2}"><ellipse cx="84" cy="44" rx="9" ry="5" transform="rotate(30 84 44)"/><ellipse cx="104" cy="58" rx="9" ry="5" transform="rotate(40 104 58)"/><ellipse cx="124" cy="74" rx="9" ry="5" transform="rotate(35 124 74)"/></g>`,
    succulent:`<g fill="${fg2}"><ellipse cx="100" cy="64" rx="9" ry="26"/><ellipse cx="100" cy="64" rx="26" ry="9"/><ellipse cx="100" cy="64" rx="20" ry="20" transform="rotate(45 100 64)" fill="${fg}" opacity=".5"/></g><circle cx="100" cy="64" r="7" fill="${fg}"/>`,
    fern:`<path d="M100 100 C100 60 100 36 100 28" stroke="${fg}" stroke-width="3"/><g stroke="${fg2}" stroke-width="2" fill="none"><path d="M100 40 q-16 -4 -22 -14"/><path d="M100 40 q16 -4 22 -14"/><path d="M100 56 q-18 -4 -26 -14"/><path d="M100 56 q18 -4 26 -14"/><path d="M100 72 q-18 -2 -26 -12"/><path d="M100 72 q18 -2 26 -12"/></g>`,
    leaf:`<path d="M100 28 C70 50 70 86 100 100 C130 86 130 50 100 28Z" fill="${fg2}"/><path d="M100 32 V96" stroke="${fg}" stroke-width="3"/>`,
  }[L.t] || "";
  return `<svg viewBox="0 0 200 128" preserveAspectRatio="xMidYMid slice" xmlns="http://www.w3.org/2000/svg">
    <defs><linearGradient id="g${hue}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${bg1}"/><stop offset="1" stop-color="${bg2}"/></linearGradient></defs>
    <rect width="200" height="128" fill="url(#g${hue})"/>${motif}</svg>`;
}

/* ---------- DuckDB-WASM bootstrap ---------- */
const bootMsg = document.getElementById("boot-msg");
let conn;
async function initDB(){
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerURL = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], {type:"text/javascript"}));
  const worker = new Worker(workerURL);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerURL);
  bootMsg.textContent = "טוען את מאגר הצמחים (canonical.parquet)…";
  const buf = new Uint8Array(await (await fetch("public/canonical.parquet")).arrayBuffer());
  await db.registerFileBuffer("canonical.parquet", buf);
  conn = await db.connect();
  await conn.query(`CREATE VIEW plants AS SELECT * FROM 'canonical.parquet'`);
}
async function q(sql){ return (await conn.query(sql)).toArray().map(r => ({...r})); }
const esc = s => String(s).replace(/'/g, "''");

/* ---------- filter state ---------- */
const state = {search:"", life_form:"", invasive_status:"", family:"", month:"", habitat:"", forSale:false, sort:"name_he", limit:48};
let facets = {families:[], habitats:[]};

function whereSQL(){
  const w = [];
  if(state.search){ const s=esc(state.search); w.push(`(name_he ILIKE '%${s}%' OR scientific_name ILIKE '%${s}%' OR family ILIKE '%${s}%' OR habitat_he ILIKE '%${s}%')`); }
  if(state.life_form) w.push(`life_form = '${esc(state.life_form)}'`);
  if(state.invasive_status) w.push(`invasive_status = '${esc(state.invasive_status)}'`);
  if(state.family) w.push(`family = '${esc(state.family)}'`);
  if(state.month) w.push(`bloom_months_he LIKE '%${esc(state.month)}%'`);
  if(state.habitat) w.push(`habitat_he = '${esc(state.habitat)}'`);
  if(state.forSale) w.push(`sold_by IS NOT NULL`);
  return w.length ? "WHERE "+w.join(" AND ") : "";
}
const SELECT = `SELECT CAST(canonical_taxon_id AS VARCHAR) id, name_he, scientific_name, family, genus,
  life_form, native_status, invasive_status, bloom_months_he, habitat_he, distribution_il,
  array_to_string(common_names_en, ', ') common_en, array_to_string(synonyms_latin, ', ') synonyms,
  array_to_string(sold_by, ', ') sold_by, array_to_string(pot_sizes, ', ') pot_sizes,
  price_ils, price_band, availability, array_to_string(source_urls, ' ') source_urls, source_url,
  image_thumb, image_source, image_license, image_credit, image_page
  FROM plants`;

let matched = [];
async function runQuery(){
  const where = whereSQL();
  const orderBy = state.sort==="price" ? "price_ils DESC NULLS LAST, name_he" : `${state.sort} NULLS LAST`;
  matched = await q(`${SELECT} ${where} ORDER BY ${orderBy}`);
  state.limit = 48;
  renderGrid();
}

/* ---------- rendering ---------- */
const grid = document.getElementById("grid");
const countEl = document.getElementById("count");

function bloomShort(b){ if(!b) return "—"; const p=b.split(/[,\s]+/).filter(Boolean); return p.length>3 ? p[0]+"–"+p[p.length-1] : p.join(", "); }

function card(p){
  const inv = INVAS[p.invasive_status];
  const badges = `<span class="badge badge--native">מקומי</span>${inv?`<span class="badge badge--${inv.cls}">${inv.he}</span>`:""}`;
  const foot = p.sold_by
    ? `<span class="price">₪${p.price_ils}<small> · ${p.pot_sizes||""}</small></span><span class="stock stock--${p.availability==='in_stock'?'in':'out'}">${p.availability==='in_stock'?'במלאי':'אזל'}</span>`
    : `<span class="attr__lbl" style="font-size:12.5px">${p.family||""}</span>`;
  const photo = p.image_thumb ? `<img class="card__photo" src="${p.image_thumb}" loading="lazy" alt="${p.name_he||''}" onerror="this.remove()">` : "";
  return `<article class="card" data-id="${p.id}">
    <div class="card__img">${placeholder(p.life_form, p.name_he)}${photo}
      <div class="card__badges">${badges}</div>
      <button class="card__fav" title="הוספה למועדפים">♡</button>
    </div>
    <div class="card__body">
      <div><div class="card__name">${p.name_he||"—"}</div><div class="card__sci">${p.scientific_name||""}</div></div>
      <div class="attrs">
        <div class="attr"><span class="attr__ic">${LIFE[p.life_form]?.ic||"🌱"}</span><span>${lifeHe(p.life_form)}</span></div>
        <div class="attr"><span class="attr__ic">🗓️</span><span class="attr__lbl">פריחה:</span><span>${bloomShort(p.bloom_months_he)}</span></div>
        <div class="attr"><span class="attr__ic">📍</span><span class="attr__lbl">בית גידול:</span><span>${p.habitat_he||"—"}</span></div>
      </div>
      <div class="card__foot">${foot}</div>
    </div>
  </article>`;
}

function renderGrid(){
  countEl.innerHTML = `נמצאו <b>${matched.length.toLocaleString("he")}</b> צמחים`;
  if(!matched.length){ grid.innerHTML = `<div class="empty">לא נמצאו צמחים התואמים את הסינון. נסו לרכך את הסינון.</div>`; document.getElementById("loadmore-wrap").hidden=true; return; }
  grid.innerHTML = matched.slice(0, state.limit).map(card).join("");
  document.getElementById("loadmore-wrap").hidden = matched.length <= state.limit;
  if (canAnimate()) anime({targets: "#grid .card", opacity: [0, 1], translateY: [16, 0], scale: [0.985, 1],
                  duration: 460, delay: anime.stagger(18, {grid: [4, 100], from: "first"}), easing: "easeOutCubic"});
}

/* ---------- filter chips ---------- */
function sel(id, label, ic, options){
  const opts = `<option value="">${label}</option>` + options.map(o=>`<option value="${o.v}">${o.t}</option>`).join("");
  return `<label class="chip chip--sel"><span class="chip__ic">${ic}</span><select data-f="${id}">${opts}</select></label>`;
}
function renderFilters(){
  const fl = document.getElementById("filters");
  const lifeOpts = Object.entries(LIFE).map(([v,o])=>({v, t:o.he}));
  fl.innerHTML = [
    sel("life_form","צורת חיים","🌿", lifeOpts),
    sel("month","עונת פריחה","🗓️", MONTHS.map(m=>({v:m,t:m}))),
    sel("habitat","בית גידול","📍", facets.habitats.map(h=>({v:h.habitat_he, t:`${h.habitat_he} (${h.c})`}))),
    sel("family","משפחה","🧬", facets.families.map(f=>({v:f.family, t:`${f.family} (${f.c})`}))),
    sel("invasive_status","סטטוס פלישה","⚠️", [{v:"listed",t:"פולש"},{v:"potential",t:"פולש פוטנציאלי"},{v:"not_listed",t:"לא פולש"}]),
    `<label class="chip" id="chip-sale"><span class="chip__ic">🛒</span>למכירה</label>`,
  ].join("");
  fl.querySelectorAll("select").forEach(s=>s.addEventListener("change", e=>{
    state[e.target.dataset.f] = e.target.value;
    e.target.closest(".chip").classList.toggle("is-on", !!e.target.value);
    runQuery();
  }));
  document.getElementById("chip-sale").addEventListener("click", e=>{
    state.forSale = !state.forSale; e.currentTarget.classList.toggle("is-on", state.forSale); runQuery();
  });
}

/* ---------- stats sidebar ---------- */
async function renderStats(){
  const [s] = await q(`SELECT count(*) total, count(DISTINCT family) fams,
    count(*) FILTER(WHERE invasive_status<>'not_listed') invas,
    count(*) FILTER(WHERE sold_by IS NOT NULL) sale,
    count(*) FILTER(WHERE life_form IS NOT NULL) lifed FROM plants`);
  const cards = [
    {ic:"🌿", v:Number(s.total), l:"מינים במאגר", cls:""},
    {ic:"🧬", v:Number(s.fams), l:"משפחות בוטניות", cls:""},
    {ic:"⚠️", v:Number(s.invas), l:"מסומנים כפולשים", cls:"stat--terra"},
    {ic:"🛒", v:Number(s.sale), l:"זמינים לרכישה", cls:"stat--gold"},
  ];
  document.getElementById("stat-cards").innerHTML = cards.map(c=>`
    <div class="stat ${c.cls}"><div class="stat__ic">${c.ic}</div><div>
      <div class="stat__num" data-v="${c.v}">0</div>
      <div class="stat__lbl">${c.l}</div></div></div>`).join("");
  document.querySelectorAll(".stat__num").forEach((el, i) => {
    const v = +el.dataset.v;
    if (!canAnimate()) { el.textContent = v.toLocaleString("he"); return; }
    const o = {n: 0};
    anime({targets: o, n: v, round: 1, duration: 1300, delay: 200 + i*90, easing: "easeOutExpo",
           update: () => el.textContent = Math.round(o.n).toLocaleString("he")});
  });
  const pct = Math.round(Number(s.lifed)/Number(s.total)*100);
  const circ = 2*Math.PI*50;
  const dv = document.getElementById("donut-val"), pctEl = document.getElementById("donut-pct");
  if (!canAnimate()) { dv.style.strokeDasharray = `${circ*pct/100} ${circ}`; pctEl.textContent = pct+"%"; }
  else { const o = {p: 0}; anime({targets: o, p: pct, round: 1, duration: 1400, delay: 300, easing: "easeInOutCubic",
           update: () => { dv.style.strokeDasharray = `${circ*o.p/100} ${circ}`; pctEl.textContent = Math.round(o.p)+"%"; }}); }
  document.getElementById("donut-note").textContent = `לכל המינים יש זהות (GBIF) וסטטוס פלישה; ל-${pct}% גם צורת חיים מסווגת.`;
}

/* ---------- detail drawer ---------- */
const overlay = document.getElementById("overlay"), detail = document.getElementById("detail");
function openDetail(p){
  const inv = INVAS[p.invasive_status];
  const regions = (p.distribution_il||"").split(/[,،]\s*/).filter(Boolean).map(r=>`<span class="region">${r}</span>`).join("");
  const sellerSec = p.sold_by ? `<h3 class="detail__sec">זמינות מסחרית</h3>
    <div class="seller-row"><span>נמכר ב־<b>${p.sold_by}</b> · עציץ ${p.pot_sizes||"—"}</span><span class="price">₪${p.price_ils} <small>(${p.price_band})</small></span></div>` : "";
  const dphoto = p.image_thumb ? `<img class="detail__photo" src="${p.image_thumb}" alt="${p.name_he||''}" onerror="this.remove()">` : "";
  const credit = p.image_thumb ? `<div class="detail__credit">צילום: ${p.image_credit||p.image_source||""}${p.image_license?" · "+p.image_license:""}${p.image_page?` · <a href="${p.image_page}" target="_blank" rel="noopener">מקור ↗</a>`:""}</div>` : "";
  detail.innerHTML = `
    <div class="detail__hero">${placeholder(p.life_form, p.name_he)}${dphoto}<button class="detail__close" aria-label="סגירה">✕</button></div>
    ${credit}
    <div class="detail__body">
      <h2 class="detail__name">${p.name_he||"—"}</h2>
      <div class="detail__sci">${p.scientific_name||""}</div>
      <div class="detail__chips">
        <span class="dchip">מקומי</span>${inv?`<span class="dchip">${inv.he}</span>`:""}
        <span class="dchip">${LIFE[p.life_form]?.ic||"🌱"} ${lifeHe(p.life_form)}</span>
        ${p.family?`<span class="dchip">🧬 ${p.family}</span>`:""}
      </div>
      <div class="detail__grid">
        <div class="dfield"><div class="dfield__lbl">סוג / Genus</div><div class="dfield__val">${p.genus||"—"}</div></div>
        <div class="dfield"><div class="dfield__lbl">עונת פריחה</div><div class="dfield__val">${p.bloom_months_he||"—"}</div></div>
        <div class="dfield dfield--wide"><div class="dfield__lbl">בית גידול</div><div class="dfield__val">${p.habitat_he||"—"}</div></div>
        ${p.common_en?`<div class="dfield dfield--wide"><div class="dfield__lbl">שם נפוץ (אנגלית)</div><div class="dfield__val">${p.common_en}</div></div>`:""}
        ${p.synonyms?`<div class="dfield dfield--wide"><div class="dfield__lbl">שמות נרדפים (לטינית)</div><div class="dfield__val" style="font-style:italic">${p.synonyms}</div></div>`:""}
      </div>
      ${regions?`<h3 class="detail__sec">תפוצה בארץ</h3><div class="regions">${regions}</div>`:""}
      ${sellerSec}
      ${p.source_url?`<a class="src-link" href="${p.source_url}" target="_blank" rel="noopener">מקור: צמח השדה (קק״ל) ↗</a>`:""}
      <p class="prov-note">זהות וטקסונומיה: GBIF · בית גידול/פריחה/תפוצה: צמח השדה · סטטוס פלישה: GRIIS + רשימת פולשים מהספרות · מחיר/זמינות: משתלת אזור. כל ערך נשמר עם מקורו בטבלת ה-provenance.</p>
    </div>`;
  detail.querySelector(".detail__close").addEventListener("click", closeDetail);
  overlay.hidden = false; detail.hidden = false;
  if (RM) { detail.style.transform = "none"; overlay.style.opacity = 1; return; }
  anime({targets: overlay, opacity: [0, 1], duration: 240, easing: "easeOutQuad"});
  anime({targets: detail, translateX: ["100%", "0%"], duration: 440, easing: "easeOutCubic"});
  anime({targets: detail.querySelectorAll(".detail__body > *, .detail__credit"),
         opacity: [0, 1], translateY: [12, 0], delay: anime.stagger(32, {start: 140}),
         duration: 420, easing: "easeOutCubic"});
}
function closeDetail(){
  if (RM || (overlay.hidden && detail.hidden)) { overlay.hidden = true; detail.hidden = true; return; }
  anime({targets: overlay, opacity: [1, 0], duration: 260, easing: "easeInQuad", complete: () => overlay.hidden = true});
  anime({targets: detail, translateX: ["0%", "100%"], duration: 320, easing: "easeInCubic",
         complete: () => { detail.hidden = true; detail.style.transform = ""; }});
}
overlay.addEventListener("click", closeDetail);
document.addEventListener("keydown", e=>{ if(e.key==="Escape") closeDetail(); });
grid.addEventListener("click", e=>{
  const c = e.target.closest(".card"); if(!c) return;
  if(e.target.classList.contains("card__fav")){ e.target.textContent = e.target.textContent==="♡"?"♥":"♡"; return; }
  const p = matched.find(x=>x.id===c.dataset.id); if(p) openDetail(p);
});

/* ---------- controls ---------- */
let t;
document.getElementById("q").addEventListener("input", e=>{ clearTimeout(t); t=setTimeout(()=>{ state.search=e.target.value.trim(); runQuery(); },220); });
document.getElementById("sort").addEventListener("change", e=>{ state.sort=e.target.value; runQuery(); });
document.getElementById("loadmore").addEventListener("click", ()=>{ state.limit+=48; renderGrid(); });
document.getElementById("view-grid").addEventListener("click", ()=>{ grid.classList.remove("is-list"); toggleView("grid"); });
document.getElementById("view-list").addEventListener("click", ()=>{ grid.classList.add("is-list"); toggleView("list"); });
function toggleView(v){ document.getElementById("view-grid").classList.toggle("is-active",v==="grid"); document.getElementById("view-list").classList.toggle("is-active",v==="list"); }

/* ---------- boot ---------- */
(async function(){
  try{
    await initDB();
    facets.families = await q(`SELECT family, count(*) c FROM plants WHERE family IS NOT NULL GROUP BY 1 ORDER BY c DESC`);
    facets.habitats = await q(`SELECT habitat_he, count(*) c FROM plants WHERE habitat_he IS NOT NULL GROUP BY 1 ORDER BY c DESC`);
    renderFilters();
    await renderStats();
    await runQuery();
    document.getElementById("boot").hidden = true;
    if (canAnimate())  // barely-there breeze on the gypsophila
      anime({targets: ".deco--gypso", rotate: [0, 1.6], duration: 7800, direction: "alternate", loop: true, easing: "easeInOutSine"});
  }catch(err){
    console.error(err);
    const b = document.getElementById("boot"); b.classList.add("err");
    bootMsg.innerHTML = `אירעה שגיאה בטעינת מנוע הנתונים.<br><small style="color:#a85a36">${err}</small>`;
  }
})();
