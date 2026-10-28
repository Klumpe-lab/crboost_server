// The STA kitchen's one element: the count line, the filter strip, the slab panel and the gallery,
// drawn from one in-browser store of the pool table (services/kitchen/payload.py).
//
// The store functions at the top touch no DOM: `node` runs them against the server's reading of the
// same recipe (services/kitchen/pool.py `select`, roadmap 40 §4.5). The component below them draws.

// ── Store ───────────────────────────────────────────────────────────────────────────────────────

const TYPED = { "<f8": Float64Array, "<f4": Float32Array, "<i4": Int32Array };
export const KEEP = 1;
export const DROP = 2;

export function parsePayload(buf) {
  const head = new Uint8Array(buf, 0, 4);
  if (String.fromCharCode(...head) !== "CBK1") throw new Error("not a kitchen table");
  const hlen = new DataView(buf).getUint32(4, true);
  const header = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 8, hlen)));
  const base = 8 + hlen;
  const store = { header, n: header.n, num: {}, cat: {}, cols: {}, frames: header.frames, uid: null };
  for (const a of header.arrays) {
    const arr = new TYPED[a.dtype](buf, base + a.offset, a.length);
    if (a.name === "uid") store.uid = arr;
    else if (a.name.startsWith("num:")) store.num[a.name.slice(4)] = arr;
    else store.cat[a.name.slice(4)] = arr;
  }
  for (const c of header.columns) store.cols[c.name] = c;
  store.verdict = new Int8Array(store.n);
  store.tags = new Int32Array(store.n);
  store.hex = null;
  store.rowOf = null;
  return store;
}

export function uidHex(store, row) {
  if (!store.hex) store.hex = new Array(store.n);
  let h = store.hex[row];
  if (h === undefined) h = store.hex[row] = store.uid[row].toString(16).padStart(12, "0");
  return h;
}

export function rowOfUid(store, hex) {
  if (!store.rowOf) {
    store.rowOf = new Map();
    for (let i = 0; i < store.n; i++) store.rowOf.set(uidHex(store, i), i);
  }
  return store.rowOf.get(hex);
}

export function applyVerdicts(store, map) {
  for (const [hex, v] of Object.entries(map)) {
    const row = rowOfUid(store, hex);
    if (row !== undefined) store.verdict[row] = v;
  }
}

// One filter's mask (1 = passes). Bounds are compared as float32, as the server compares them.
export function filterMask(store, f, out) {
  const n = store.n;
  out = out || new Uint8Array(n);
  if (f.kind === "range") {
    const v = store.num[f.col];
    if (!v) return out.fill(1);
    const lo = f.lo == null ? -Infinity : Math.fround(f.lo);
    const hi = f.hi == null ? Infinity : Math.fround(f.hi);
    const keep = f.keep_missing === false ? 0 : 1;
    for (let i = 0; i < n; i++) {
      const x = v[i];
      out[i] = x !== x ? keep : x >= lo && x <= hi ? 1 : 0;
    }
    return out;
  }
  if (f.kind === "category") {
    const codes = store.cat[f.col];
    const names = (store.cols[f.col] || {}).categories || [];
    if (!codes) return out.fill(1);
    const allowed = new Uint8Array(names.length + 1);
    const want = new Set(f.allowed || []);
    names.forEach((name, i) => { if (want.has(name)) allowed[i] = 1; });
    for (let i = 0; i < n; i++) out[i] = codes[i] >= 0 ? allowed[codes[i]] : 0;
    return out;
  }
  return out.fill(1);
}

// Rows of a conflicted acquisition outside its chosen frame group are set aside (0).
export function choiceMask(store, choice, out) {
  const n = store.n;
  out = out || new Uint8Array(n);
  out.fill(1);
  const conflicts = store.header.conflicts || {};
  const frame = store.cat.frame;
  for (const [acq, groups] of Object.entries(conflicts)) {
    const chosen = (choice || {})[acq];
    if (!chosen) continue;
    const keep = groups.find((g) => store.frames[g[0]].key === chosen);
    if (!keep) continue;
    const off = new Set();
    for (const g of groups) if (g !== keep) for (const f of g) off.add(f);
    for (let i = 0; i < n; i++) if (off.has(frame[i])) out[i] = 0;
  }
  return out;
}

// Filters, the frame choice, removals per filter (rows passing every other filter and failing it).
export function evaluate(store, draft, choice) {
  const n = store.n;
  const filters = (draft && draft.filters) || [];
  const masks = filters.map((f) => filterMask(store, f));
  const base = choiceMask(store, choice);
  const passes = Uint8Array.from(base);
  for (const m of masks) for (let i = 0; i < n; i++) passes[i] &= m[i];
  // failing[i] = how many filters row i fails; a row failing exactly one is "removed" by that one.
  const failing = new Uint8Array(n);
  for (const m of masks) for (let i = 0; i < n; i++) failing[i] += m[i] ? 0 : 1;
  const removes = masks.map((m) => {
    let c = 0;
    for (let i = 0; i < n; i++) if (base[i] && failing[i] === 1 && !m[i]) c++;
    return c;
  });
  const coverage = filters.map((f) => {
    if (f.kind !== "range" || !store.num[f.col]) return [n, n];
    const v = store.num[f.col];
    let c = 0;
    for (let i = 0; i < n; i++) if (v[i] === v[i]) c++;
    return [c, n];
  });
  return { masks, base, passes, removes, coverage, failing };
}

function postGroup(store, i) {
  const f = store.cat.frame[i];
  return f >= 0 ? f : -1 - (store.cat.member[i] * 100000 + store.cat.tomo[i]);
}

// Post-ops per frame on the rows that pass: best σ first (no σ last, then table order), NN thinning
// at nn_a Å (a row is dropped within nn_a of an earlier kept one), then the first top_n.
export function applyPostOps(store, passes, post) {
  const n = store.n;
  const nn = post && post.nn_a ? +post.nn_a : 0;
  const top = post && post.top_n ? Math.floor(+post.top_n) : 0;
  if (!nn && !top) return Uint8Array.from(passes);
  const out = new Uint8Array(n);
  const rows = [];
  for (let i = 0; i < n; i++) if (passes[i]) rows.push(i);
  const sig = store.num.sigma;
  const key = (i) => (sig[i] === sig[i] ? -sig[i] : Infinity);
  rows.sort((a, b) => postGroup(store, a) - postGroup(store, b) || key(a) - key(b) || a - b);
  const x = store.num.x_a, y = store.num.y_a, z = store.num.z_a;
  const r2 = nn * nn;
  let start = 0;
  while (start < rows.length) {
    const g = postGroup(store, rows[start]);
    let end = start;
    while (end < rows.length && postGroup(store, rows[end]) === g) end++;
    const kept = [];
    for (let k = start; k < end; k++) {
      const i = rows[k];
      let ok = true;
      if (nn) {
        for (const j of kept) {
          const dx = x[i] - x[j], dy = y[i] - y[j], dz = z[i] - z[j];
          if (dx * dx + dy * dy + dz * dz <= r2) { ok = false; break; }
        }
      }
      if (ok) kept.push(i);
    }
    const n_keep = top ? Math.min(top, kept.length) : kept.length;
    for (let k = 0; k < n_keep; k++) out[kept[k]] = 1;
    start = end;
  }
  return out;
}

// kept = (passes the recipe and not dropped by hand) or kept by hand.
export function finalMask(store, after) {
  const n = store.n;
  const out = new Uint8Array(n);
  const v = store.verdict;
  for (let i = 0; i < n; i++) out[i] = (after[i] && v[i] !== DROP) || v[i] === KEEP ? 1 : 0;
  return out;
}

export function select(store, draft, choice) {
  const ev = evaluate(store, draft, choice);
  const after = applyPostOps(store, ev.passes, draft && draft.post);
  const final = finalMask(store, after);
  return { ...ev, after, final };
}

export function countRows(store, mask) {
  const n = store.n;
  const frames = new Set();
  const perProject = new Map();
  let c = 0;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    c++;
    frames.add(store.cat.frame[i]);
    const p = store.cat.project[i];
    perProject.set(p, (perProject.get(p) || 0) + 1);
  }
  return { n: c, nFrames: frames.size, perProject };
}

// The rows of `mask` in display order.
export function ordered(store, mask, sort, cut, seed) {
  const rows = [];
  for (let i = 0; i < store.n; i++) if (mask[i]) rows.push(i);
  const sig = store.num.sigma;
  const asc = (col) => {
    const v = store.num[col];
    return (a, b) => (v[a] === v[a] ? v[a] : Infinity) - (v[b] === v[b] ? v[b] : Infinity) || a - b;
  };
  const bySigma = (a, b) => (sig[b] === sig[b] ? sig[b] : -Infinity) - (sig[a] === sig[a] ? sig[a] : -Infinity) || a - b;
  let cmp = bySigma;
  if (sort === "depth") cmp = asc("depth_a");
  else if (sort === "nn") cmp = asc("nn_a");
  else if (sort === "tomogram") cmp = (a, b) => store.cat.frame[a] - store.cat.frame[b] || bySigma(a, b);
  else if (sort === "random") {
    const h = (i) => { let s = (store.uid[i] % 2147483647) ^ (seed || 1); s = Math.imul(s ^ (s >>> 15), 2246822507); s = Math.imul(s ^ (s >>> 13), 3266489909); return (s ^ (s >>> 16)) >>> 0; };
    cmp = (a, b) => h(a) - h(b) || a - b;
  } else if (sort === "cut" && cut != null) {
    const d = (i) => (sig[i] === sig[i] ? Math.abs(sig[i] - cut) : Infinity);
    cmp = (a, b) => d(a) - d(b) || a - b;
  }
  return Int32Array.from(rows.sort(cmp));
}

// Group keys per row for the gallery's group headers.
export function groupOf(store, group, row) {
  if (group === "tomogram") return store.cat.frame[row];
  if (group === "project") return store.cat.project[row];
  if (group === "sigma") { const s = store.num.sigma[row]; return s === s ? Math.floor(s) : -1e9; }
  if (group === "tag") return store.tags[row];
  return 0;
}

// Counts per bin over [lo, hi): of every row with a value, and of rows in `mask`.
export function histogram(values, mask, bins, lo, hi) {
  const all = new Float64Array(bins), sel = new Float64Array(bins);
  const w = (hi - lo) / bins;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v !== v) continue;
    let b = Math.floor((v - lo) / w);
    if (b < 0) b = 0; else if (b >= bins) b = bins - 1;
    all[b]++;
    if (mask[i]) sel[b]++;
  }
  return { all, sel };
}

export function quantiles(values, qs) {
  const v = [];
  for (let i = 0; i < values.length; i++) if (values[i] === values[i]) v.push(values[i]);
  if (!v.length) return qs.map(() => NaN);
  v.sort((a, b) => a - b);
  return qs.map((q) => v[Math.min(v.length - 1, Math.max(0, Math.round(q * (v.length - 1))))]);
}

// The by-tomogram stream: one entry per frame holding a shown row, its shown rows ordered by their
// dot's Y on the slab (top first) then X, so leaders rarely cross; frames with nothing shown apart.
export function streamFrames(store, shown, rowOrder) {
  const by = new Map(), total = new Map();
  const frame = store.cat.frame;
  for (let i = 0; i < store.n; i++) {
    const f = frame[i];
    total.set(f, (total.get(f) || 0) + 1);
    if (!shown[i]) continue;
    if (!by.has(f)) by.set(f, []);
    by.get(f).push(i);
  }
  const y = store.num.y_px, x = store.num.x_px;
  const fact = (f, col) => { for (let i = 0; i < store.n; i++) if (frame[i] === f) return store.num[col][i]; return NaN; };
  const out = [];
  for (const [f, rows] of by) {
    rows.sort((a, b) => (y[b] === y[b] ? y[b] : -Infinity) - (y[a] === y[a] ? y[a] : -Infinity) || x[a] - x[b] || a - b);
    out.push({ code: f, rows: Int32Array.from(rows), total: total.get(f), ctf: NaN, tilts: NaN });
  }
  const firstRow = new Map();
  for (let i = 0; i < store.n; i++) if (!firstRow.has(frame[i])) firstRow.set(frame[i], i);
  for (const e of out) { const r = firstRow.get(e.code); e.ctf = store.num.ctf_res[r]; e.tilts = store.num.n_tilts[r]; }
  const name = (e) => ((store.frames[e.code] || {}).tomo || "");
  const proj = (e) => ((store.frames[e.code] || {}).project || "");
  const cmp = {
    passing: (a, b) => b.rows.length - a.rows.length || a.code - b.code,
    ctf: (a, b) => (b.ctf === b.ctf ? b.ctf : -Infinity) - (a.ctf === a.ctf ? a.ctf : -Infinity) || a.code - b.code,
    project: (a, b) => proj(a).localeCompare(proj(b)) || name(a).localeCompare(name(b), undefined, { numeric: true }),
    name: (a, b) => name(a).localeCompare(name(b), undefined, { numeric: true }) || proj(a).localeCompare(proj(b)),
  }[rowOrder] || ((a, b) => b.rows.length - a.rows.length || a.code - b.code);
  out.sort(cmp);
  const empty = [...total.keys()].filter((f) => !by.has(f) && f >= 0).map((f) => ({ code: f, rows: new Int32Array(0), total: total.get(f), ctf: store.num.ctf_res[firstRow.get(f)], tilts: store.num.n_tilts[firstRow.get(f)] }));
  empty.sort(cmp);
  return { frames: out, empty };
}

// ── Component ───────────────────────────────────────────────────────────────────────────────────

const SIZES = { S: 96, M: 160, L: 256 };
const GAP = 4;
const HEADER_H = 20;
const GLYPH = { tm: "", hand: "✋", imported: "⇣", merged: "∪" };

function el(tag, style, cls) {
  const e = document.createElement(tag);
  if (style) e.style.cssText = style;
  if (cls) e.className = cls;
  return e;
}

function fmt(v, d = 1) {
  return v === v && v != null ? (+v).toFixed(d) : "–";
}

export default {
  template: `
    <div ref="root" class="cbk" style="display:flex;flex-direction:column;width:100%;height:100%;min-height:0;position:relative;container-type:inline-size;">
      <div ref="countline" class="cbk-countline" style="display:flex;align-items:center;gap:10px;min-height:18px;padding:0 2px 6px 2px;font-size:10px;color:#334155;"></div>
      <div ref="strip" class="cbk-strip" style="display:flex;flex-direction:column;gap:4px;"></div>
      <div ref="main" class="cbk-main" style="flex:1 1 0%;min-height:0;display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:10px;">
        <div ref="left" class="cbk-left" style="min-height:0;overflow-y:auto;overflow-x:hidden;display:flex;flex-direction:column;gap:4px;"></div>
        <div ref="grid" class="cbk-grid" style="min-height:0;overflow-y:auto;overflow-x:hidden;position:relative;outline:none;" tabindex="0">
          <div ref="pin" class="cbk-pin" style="position:sticky;top:0;z-index:3;height:0;"></div>
          <div ref="sizer" style="position:relative;width:100%;"></div>
        </div>
      </div>
      <div ref="stream" class="cbk-stream" style="display:none;flex:1 1 0%;min-height:0;overflow-y:auto;overflow-x:hidden;position:relative;">
        <div ref="streamSizer" style="position:relative;width:100%;"></div>
      </div>
      <div ref="status" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;font-size:11px;color:#64748b;"></div>
    </div>`,
  props: {
    table_url: String,
    tile_url: String,
    slab_url: String,
    draft: Object,
    frame_choice: Object,
    mode: String,
    size: String,
    sort: String,
    group: String,
    species_color: String,
    tag_defs: Array,
    curating: Boolean,
    row_order: String,
    averages: Object,
  },
  mounted() {
    this.s = null; // the store; non-reactive on purpose
    this.sel = null;
    this.order = new Int32Array(0);
    this.layout = null;
    this.tiles = new Map();
    this.focusFrame = -1;
    this.activeRow = -1;
    this.cursor = 0;
    this.localDraft = this.draft ? JSON.parse(JSON.stringify(this.draft)) : { filters: [], post: {} };
    this.seed = Math.floor(Math.random() * 1e9);
    this.inverted = -1;
    this.drag = null;
    this.lasso = null;
    this.streamNodes = new Map();
    this.streamLayout = null;
    this.showEmpty = false;
    this.$refs.stream.addEventListener("scroll", () => {
      if (this.streamRaf) return;
      this.streamRaf = requestAnimationFrame(() => { this.streamRaf = 0; this.renderStream(); });
    }, { passive: true });
    this.applyMode();
    this.onScroll = () => this.scheduleWindow();
    this.$refs.grid.addEventListener("scroll", this.onScroll, { passive: true });
    this.ro = new ResizeObserver(() => { this.relayout(); this.drawDots(); });
    this.ro.observe(this.$refs.grid);
    this.ro.observe(this.$refs.left);
    this.$refs.grid.addEventListener("mouseover", (e) => this.onTileOver(e));
    this.$refs.grid.addEventListener("click", (e) => this.onTileClick(e));
    this.$refs.grid.addEventListener("keydown", (e) => this.onKey(e));
    this.$refs.grid.addEventListener("pointerdown", (e) => this.onGridDown(e));
    window.addEventListener("pointermove", (this.onWinMove = (e) => this.onGridMove(e)));
    window.addEventListener("pointerup", (this.onWinUp = (e) => this.onGridUp(e)));
    if (this.table_url) this.load(this.table_url);
    else this.status("Opening the pool…");
  },
  unmounted() {
    this.ro && this.ro.disconnect();
    window.removeEventListener("pointermove", this.onWinMove);
    window.removeEventListener("pointerup", this.onWinUp);
  },
  watch: {
    table_url(url) { if (url) this.load(url); },
    size() { this.relayout(); },
    sort() { this.reorder(); },
    group() { this.reorder(); },
    mode() { this.applyMode(); this.keepStreamTarget = true; this.reorder(); },
    row_order() { this.reorder(); },
    averages() { this.paintAverages(); },
    draft(d) { this.localDraft = d ? JSON.parse(JSON.stringify(d)) : { filters: [], post: {} }; this.recompute(true); },
    frame_choice() { this.recompute(true); },
  },
  methods: {
    url(u) { return (u.startsWith("/") ? window.path_prefix || "" : "") + u; },
    status(text) { this.$refs.status.textContent = text || ""; },
    async load(url) {
      this.status("Loading the pool table…");
      try {
        const res = await fetch(this.url(url));
        if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
        const keep = this.s ? { verdict: this.s.verdict, tags: this.s.tags, hex: this.s.hex } : null;
        this.s = parsePayload(await res.arrayBuffer());
        this.override = null;
        this.status(this.s.n ? "" : "No picks in the pool yet: add sources.");
        this.$emit("loaded", { n: this.s.n, version: this.s.header.version });
        if (keep && keep.hex) {
          // A rebuilt table keeps the verdicts it already showed until the server's arrive.
          for (let i = 0; i < keep.hex.length; i++) if (keep.hex[i] !== undefined && keep.verdict[i]) {
            const r = rowOfUid(this.s, keep.hex[i]);
            if (r !== undefined) this.s.verdict[r] = keep.verdict[i];
          }
        }
        this.recompute(true);
      } catch (e) {
        this.status(`The pool table could not be loaded: ${e.message}`);
      }
    },
    // Python → JS
    apply_verdicts(map) {
      if (!this.s) return;
      this.s.verdict.fill(0);
      applyVerdicts(this.s, map || {});
      this.recompute(true);
    },
    apply_tags(map) {
      if (!this.s) return;
      this.s.tags.fill(0);
      for (const [hex, bits] of Object.entries(map || {})) {
        const r = rowOfUid(this.s, hex);
        if (r !== undefined) this.s.tags[r] = bits;
      }
      this.renderWindow(true);
    },
    focus_frame(code) { this.setFocus(code, -1); },
    // Only these particles, group by group, until "back" (the Ledger's comparison of two sets).
    show_uids(groups) {
      if (!this.s) return;
      const rows = [], gi = new Map(), labels = [];
      (groups || []).forEach((g, k) => {
        labels.push(`${g.label} · ${g.uids.length.toLocaleString()}`);
        for (const u of g.uids) {
          const r = rowOfUid(this.s, u);
          if (r !== undefined && !gi.has(r)) { rows.push(r); gi.set(r, k); }
        }
      });
      this.override = { rows: Int32Array.from(rows), gi, labels };
      if (this.streamMode()) this.$emit("mode", { mode: "pool" });
      this.renderCounts();
      this.reorder();
    },
    // The recipe
    recompute(full) {
      if (!this.s) return;
      this.sel = select(this.s, this.localDraft, this.frame_choice);
      this.renderCounts();
      this.buildStrip();
      if (full) this.reorder();
      else this.drawDots();
    },
    shownMask() {
      const k = this.inverted;
      if (k == null || k < 0 || !this.sel.masks || !this.sel.masks[k]) return this.sel.final;
      // What one filter removes: rows passing every other filter and failing this one.
      const m = this.sel.masks[k], out = new Uint8Array(this.s.n);
      for (let i = 0; i < this.s.n; i++) out[i] = this.sel.base[i] && this.sel.failing[i] === 1 && !m[i] ? 1 : 0;
      return out;
    },
    reorder() {
      if (!this.s || !this.sel) return;
      const cut = this.sigmaCut();
      this.order = this.override ? this.override.rows : ordered(this.s, this.shownMask(), this.sort || "sigma", cut, this.seed);
      if (this.focusFrame < 0 && this.order.length) this.setFocus(this.s.cat.frame[this.order[0]], -1);
      if (this.streamMode()) {
        this.buildStream();
        return;
      }
      this.relayout();
      this.drawDots();
    },
    sigmaCut() {
      const f = (this.localDraft.filters || []).find((x) => x.kind === "range" && x.col === "sigma");
      return f && f.lo != null ? f.lo : null;
    },
    // Count line and composition bar
    renderCounts() {
      const box = this.$refs.countline;
      box.innerHTML = "";
      const s = this.s, sel = this.sel;
      const fin = countRows(s, sel.final);
      const allFrames = new Set(s.cat.frame).size;
      const projects = (s.cols.project || {}).categories || [];
      const line = el("span", "font-family:'IBM Plex Mono',monospace;font-size:10px;white-space:nowrap;");
      line.textContent = `${fin.n.toLocaleString()} of ${s.n.toLocaleString()} · ${fin.nFrames}/${allFrames} tomograms · ${fin.perProject.size}/${projects.length} projects`;
      box.appendChild(line);
      const hand = [];
      let kept = 0, dropped = 0;
      for (let i = 0; i < s.n; i++) {
        if (s.verdict[i] === KEEP && !sel.after[i]) kept++;
        else if (s.verdict[i] === DROP && sel.after[i]) dropped++;
      }
      if (kept) hand.push(`+${kept} kept by hand`);
      if (dropped) hand.push(`−${dropped} dropped by hand`);
      if (hand.length) {
        const h = el("span", "font-size:9px;color:#475569;");
        h.textContent = hand.join(" · ");
        box.appendChild(h);
      }
      const bar = el("span", "display:inline-flex;height:6px;width:140px;border-radius:2px;overflow:hidden;outline:1px solid #cbd5e1;flex:0 0 auto;");
      const total = Math.max(1, fin.n);
      const shades = ["#334155", "#64748b", "#94a3b8", "#cbd5e1", "#475569", "#1e293b", "#7c8ea3", "#a8b5c4"];
      const tip = [];
      [...fin.perProject.entries()].sort((a, b) => b[1] - a[1]).forEach(([p, c], k) => {
        const seg = el("span", `width:${(100 * c) / total}%;background:${shades[k % shades.length]};`);
        bar.appendChild(seg);
        tip.push(`${projects[p]}: ${c.toLocaleString()}`);
      });
      bar.title = tip.join("\n") || "nothing selected";
      box.appendChild(bar);
      if (this.override) {
        const back = el("span", "font-size:9px;color:#2c4a6b;cursor:pointer;padding:0 4px;border-radius:3px;outline:1px solid #cbd5e1;", "cbk-chip");
        back.textContent = "comparing two sets · back to the pool";
        back.onclick = () => { this.override = null; this.renderCounts(); this.reorder(); };
        box.appendChild(back);
      }
      if (this.$refs.countline.dataset) this.$refs.countline.dataset.n = fin.n;
    },
    // Gallery
    tilePx() { return SIZES[this.size || "M"] || SIZES.M; },
    relayout() {
      if (!this.s) return;
      const grid = this.$refs.grid;
      const px = this.tilePx();
      const cols = Math.max(1, Math.floor((grid.clientWidth - 2 + GAP) / (px + GAP)));
      const rows = []; // {y, h, header?, start, end}
      let y = 0;
      const order = this.order;
      const ov = this.override;
      const group = ov ? "override" : this.group || "none";
      const gOf = (row) => (ov ? ov.gi.get(row) : groupOf(this.s, group, row));
      let i = 0;
      while (i < order.length) {
        let j = i;
        if (group !== "none") {
          const g = gOf(order[i]);
          while (j < order.length && gOf(order[j]) === g) j++;
          rows.push({ y, h: HEADER_H, header: ov ? ov.labels[g] : this.groupLabel(group, order[i], j - i) });
          y += HEADER_H;
        } else j = order.length;
        for (let k = i; k < j; k += cols) {
          rows.push({ y, h: px + GAP, start: k, end: Math.min(j, k + cols) });
          y += px + GAP;
        }
        i = j;
      }
      this.layout = { cols, px, rows, height: y };
      this.$refs.sizer.style.height = `${y}px`;
      this.renderWindow(true);
    },
    groupLabel(group, row, count) {
      const s = this.s;
      if (group === "tomogram") { const f = s.frames[s.cat.frame[row]] || {}; return `${f.tomo || "?"} · ${f.project || ""} · ${count}`; }
      if (group === "project") return `${s.cols.project.categories[s.cat.project[row]]} · ${count}`;
      if (group === "sigma") { const v = s.num.sigma[row]; return v === v ? `${Math.floor(v)}–${Math.floor(v) + 1} σ · ${count}` : `no σ · ${count}`; }
      if (group === "tag") { const bits = s.tags[row]; const names = (this.tag_defs || []).filter((t) => bits & (1 << t.id)).map((t) => t.name); return `${names.join(", ") || "no tag"} · ${count}`; }
      return "";
    },
    scheduleWindow() {
      if (this.raf) return;
      this.raf = requestAnimationFrame(() => { this.raf = 0; this.renderWindow(false); });
    },
    renderWindow(force) {
      if (!this.layout) return;
      const grid = this.$refs.grid, sizer = this.$refs.sizer;
      const top = grid.scrollTop, h = grid.clientHeight;
      const lo = top - h, hi = top + 2 * h;
      const { rows, px } = this.layout;
      const want = new Map();
      let pin = "";
      for (const r of rows) {
        if (r.header && r.y <= top) pin = r.header;
        if (r.y + r.h < lo || r.y > hi) continue;
        if (r.header) { want.set(`h${r.y}`, r); continue; }
        for (let k = r.start; k < r.end; k++) want.set(k, { r, k });
      }
      this.$refs.pin.textContent = "";
      if (pin && (this.override || (this.group && this.group !== "none"))) {
        const p = el("div", "font-size:9px;color:#334155;background:rgba(255,255,255,0.92);padding:2px 4px;", "cbk-ghead");
        p.textContent = pin;
        this.$refs.pin.appendChild(p);
      }
      for (const [key, node] of this.tiles) {
        if (!want.has(key) || force) { node.remove(); this.tiles.delete(key); }
      }
      const need = [];
      for (const [key, w] of want) {
        if (this.tiles.has(key)) continue;
        let node;
        if (typeof key === "string") {
          node = el("div", `position:absolute;left:0;right:0;top:${w.y}px;height:${HEADER_H}px;display:flex;align-items:flex-end;font-size:9px;color:#475569;`, "cbk-ghead");
          node.textContent = w.header;
        } else {
          const row = this.order[w.k];
          const col = w.k - w.r.start;
          node = this.makeTile(row, w.k, col * (px + GAP), w.r.y, px);
          need.push(uidHex(this.s, row));
        }
        sizer.appendChild(node);
        this.tiles.set(key, node);
      }
      if (need.length) this.requestTiles();
    },
    requestTiles() {
      clearTimeout(this.needTimer);
      this.needTimer = setTimeout(() => {
        const uids = [];
        for (const [key] of this.tiles) if (typeof key === "number") uids.push(uidHex(this.s, this.order[key]));
        this.$emit("need_tiles", { uids });
      }, 60);
    },
    makeTile(row, k, x, y, px) {
      const s = this.s;
      const v = s.verdict[row];
      const cls = "cbk-tile" + (v === DROP ? " cbk-drop" : v === KEEP ? " cbk-keep" : "") + (k === this.cursor ? " cbk-cursor" : "") + (row === this.activeRow ? " cbk-active" : "");
      const t = el("div", `position:absolute;left:${x}px;top:${y}px;width:${px}px;height:${px}px;background:#1e293b;border-radius:2px;overflow:hidden;cursor:pointer;`, cls);
      t.dataset.row = row;
      t.dataset.k = k;
      const fr = s.frames[s.cat.frame[row]];
      if (!fr || !(s.num.x_px[row] === s.num.x_px[row])) {
        t.classList.add("cbk-none");
        t.dataset.reason = "no voxel position: the tomogram has no geometry";
      } else {
        const img = el("img", "width:100%;height:100%;display:block;");
        img.decoding = "async";
        img.draggable = false;
        img.onerror = () => { t.classList.add("cbk-none"); t.dataset.reason = "no cutout (outside the volume, or the volume is unreadable)"; img.remove(); };
        img.src = this.url(this.tile_url + uidHex(s, row));
        t.appendChild(img);
      }
      const src = (s.cols.source.categories || [])[s.cat.source[row]];
      if (GLYPH[src]) {
        const g = el("span", "position:absolute;left:2px;top:1px;font-size:9px;color:#fff;text-shadow:0 0 2px #000;");
        g.textContent = GLYPH[src];
        t.appendChild(g);
      }
      const bits = s.tags[row];
      if (bits) {
        const dots = el("span", "position:absolute;right:2px;bottom:2px;display:flex;gap:2px;");
        for (const td of this.tag_defs || []) if (bits & (1 << td.id)) {
          const d = el("span", "width:5px;height:5px;border-radius:50%;background:#e2e8f0;box-shadow:0 0 0 1px #0f172a;");
          d.title = td.name;
          dots.appendChild(d);
        }
        t.appendChild(dots);
      }
      return t;
    },
    tileText(row) {
      const s = this.s, fr = s.frames[s.cat.frame[row]] || {};
      const src = (s.cols.source.categories || [])[s.cat.source[row]];
      const v = s.verdict[row] === KEEP ? " · kept by hand" : s.verdict[row] === DROP ? " · dropped by hand" : "";
      return `${fr.project || "?"} · ${fr.tomo || "?"} · ${src}\nσ ${fmt(s.num.sigma[row], 2)} · depth ${fmt(s.num.depth_a[row], 0)} Å · NN ${fmt(s.num.nn_a[row], 0)} Å${v}`;
    },
    onTileOver(e) {
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!t) return;
      const row = +t.dataset.row;
      t.title = this.tileText(row) + (t.dataset.reason ? `\n${t.dataset.reason}` : "");
      if (this.activeRow !== row) {
        this.activeRow = row;
        this.drawDots();
      }
    },
    onTileClick(e) {
      if (this.suppressClick) { this.suppressClick = false; return; }
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!t) return;
      const row = +t.dataset.row;
      this.cursor = +t.dataset.k;
      this.$refs.grid.focus({ preventScroll: true });
      if (this.curating) this.clickVerdict(row);
      else this.setFocus(this.s.cat.frame[row], row);
    },
    // Slab panel
    setFocus(frame, row) {
      if (!this.s) return;
      this.activeRow = row;
      if (frame !== this.focusFrame) {
        this.focusFrame = frame;
        this.buildLeft();
        this.$emit("focus", { frame });
      }
      this.drawDots();
    },
    buildLeft() {
      const left = this.$refs.left;
      left.innerHTML = "";
      const s = this.s, fr = s.frames[this.focusFrame];
      if (!fr) {
        const e = el("div", "font-size:10px;color:#64748b;padding:8px;");
        e.textContent = "Click a cutout to show its tomogram here.";
        left.appendChild(e);
        this.slab = null;
        return;
      }
      const title = el("div", "display:flex;align-items:baseline;gap:8px;font-size:10px;color:#334155;flex-wrap:wrap;", "cbk-ttl");
      left.appendChild(title);
      this.slab = { title, panes: [] };
      const dims = fr.dims || [1, 1, 1];
      for (const which of ["xy", "xz"]) {
        const w = dims[0], h = which === "xy" ? dims[1] : dims[2];
        const pane = el("div", `position:relative;width:100%;aspect-ratio:${w}/${h};background:#0f172a;border-radius:2px;overflow:hidden;flex:0 0 auto;`, "cbk-slab");
        const img = el("img", "position:absolute;inset:0;width:100%;height:100%;display:block;opacity:0;transition:opacity 120ms;");
        img.draggable = false;
        const note = el("div", "position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:9px;color:#94a3b8;");
        note.textContent = fr.recon_exists ? "rendering the slab…" : "no reconstruction on disk";
        img.onload = () => { img.style.opacity = 1; note.textContent = ""; };
        img.onerror = () => { note.textContent = "the slab could not be rendered (see the server log)"; };
        if (fr.recon_exists) img.src = this.url(this.slab_url.replace("{frame}", this.focusFrame).replace("{which}", which));
        const canvas = el("canvas", "position:absolute;inset:0;width:100%;height:100%;");
        canvas.addEventListener("mousemove", (e) => this.onDotHover(e, which));
        canvas.addEventListener("click", (e) => this.onDotClick(e, which));
        pane.append(img, note, canvas);
        left.appendChild(pane);
        this.slab.panes.push({ which, pane, canvas, w, h });
      }
      this.renderTitle();
    },
    renderTitle() {
      if (!this.slab) return;
      const s = this.s, fr = s.frames[this.focusFrame], t = this.slab.title;
      let total = 0, pass = 0, ctf = NaN, tilts = NaN;
      for (let i = 0; i < s.n; i++) if (s.cat.frame[i] === this.focusFrame) {
        total++;
        if (this.sel.final[i]) pass++;
        ctf = s.num.ctf_res[i]; tilts = s.num.n_tilts[i];
      }
      const other = (s.header.conflicts || {})[fr.acq] ? " · ⧉ in another frame" : "";
      t.textContent = `${fr.tomo} · ${fr.project}${other} · ${pass}/${total} · CTF ${fmt(ctf, 1)} Å · ${fmt(tilts, 0)} tilts`;
      if (!fr.apix) t.classList.add("cbk-red");
    },
    frameRows() {
      const s = this.s, out = [];
      for (let i = 0; i < s.n; i++) if (s.cat.frame[i] === this.focusFrame) out.push(i);
      return out;
    },
    drawDots() {
      if (!this.slab || !this.s) return;
      this.renderTitle();
      const s = this.s, rows = this.frameRows();
      const color = this.species_color || "#3b82f6";
      const dpr = window.devicePixelRatio || 1;
      for (const p of this.slab.panes) {
        const cw = p.pane.clientWidth, ch = p.pane.clientHeight;
        if (!cw || !ch) continue;
        p.canvas.width = Math.round(cw * dpr);
        p.canvas.height = Math.round(ch * dpr);
        const g = p.canvas.getContext("2d");
        g.setTransform(dpr, 0, 0, dpr, 0, 0);
        g.clearRect(0, 0, cw, ch);
        const sx = cw / p.w, sy = ch / p.h;
        const vcol = p.which === "xy" ? s.num.y_px : s.num.z_px;
        p.pts = [];
        const r = Math.max(2.5, Math.min(5, cw / 160));
        const draw = (i, active) => {
          const x = s.num.x_px[i] * sx, y = ch - vcol[i] * sy;
          if (!(x === x && y === y)) return;
          const shown = this.sel.final[i];
          const hand = (s.cols.source.categories || [])[s.cat.source[i]] === "hand";
          g.beginPath();
          if (hand) { g.moveTo(x, y - r - 1); g.lineTo(x + r + 1, y); g.lineTo(x, y + r + 1); g.lineTo(x - r - 1, y); g.closePath(); }
          else g.arc(x, y, r, 0, 2 * Math.PI);
          if (shown) {
            g.fillStyle = color; g.fill();
            g.lineWidth = 1.5; g.strokeStyle = "#ffffff"; g.stroke();
            g.beginPath(); g.arc(x, y, r + 1.5, 0, 2 * Math.PI); g.lineWidth = 1; g.strokeStyle = "rgba(0,0,0,0.9)"; g.stroke();
          } else {
            g.lineWidth = 1.2; g.strokeStyle = "rgba(226,232,240,0.85)"; g.stroke();
          }
          if (active) {
            g.beginPath(); g.arc(x, y, r + 5, 0, 2 * Math.PI); g.lineWidth = 2; g.strokeStyle = "#ffffff"; g.stroke();
            g.beginPath(); g.arc(x, y, r + 6.5, 0, 2 * Math.PI); g.lineWidth = 1; g.strokeStyle = "rgba(15,23,42,0.85)"; g.stroke();
          }
          p.pts.push([x, y, i]);
        };
        for (const i of rows) if (!this.sel.final[i]) draw(i, false);
        for (const i of rows) if (this.sel.final[i] && i !== this.activeRow) draw(i, false);
        if (this.activeRow >= 0 && s.cat.frame[this.activeRow] === this.focusFrame) draw(this.activeRow, true);
      }
      this.markActiveTile();
    },
    nearestDot(e, which) {
      const p = this.slab && this.slab.panes.find((q) => q.which === which);
      if (!p || !p.pts) return -1;
      const b = p.canvas.getBoundingClientRect();
      const mx = e.clientX - b.left, my = e.clientY - b.top;
      let best = -1, bd = 64;
      for (const [x, y, i] of p.pts) { const d = (x - mx) ** 2 + (y - my) ** 2; if (d < bd) { bd = d; best = i; } }
      return best;
    },
    onDotHover(e, which) {
      const i = this.nearestDot(e, which);
      if (i !== this.activeRow) { this.activeRow = i; this.drawDots(); }
      const p = this.slab.panes.find((q) => q.which === which);
      p.canvas.title = i >= 0 ? this.tileText(i) : "";
    },
    onDotClick(e, which) {
      const i = this.nearestDot(e, which);
      if (i < 0) return;
      const k = this.order.indexOf(i);
      if (k >= 0) this.scrollToK(k);
    },
    scrollToK(k) {
      if (!this.layout) return;
      const r = this.layout.rows.find((q) => !q.header && k >= q.start && k < q.end);
      if (!r) return;
      const grid = this.$refs.grid;
      if (r.y < grid.scrollTop || r.y + r.h > grid.scrollTop + grid.clientHeight) grid.scrollTop = Math.max(0, r.y - grid.clientHeight / 3);
      this.cursor = k;
      this.renderWindow(false);
      this.refreshTileClasses();
    },
    // ── Filter strip (S3) ──────────────────────────────────────────────────────────────────────
    binsFor(col) {
      const v = this.s.num[col];
      let [lo, hi] = quantiles(v, [0.005, 0.995]);
      if (!(lo === lo)) return null;
      if (col === "sigma") {
        lo = Math.floor(lo * 4) / 4; hi = Math.ceil(hi * 4) / 4;
        return { lo, hi: Math.max(hi, lo + 0.25), bins: Math.max(1, Math.round((Math.max(hi, lo + 0.25) - lo) / 0.25)) };
      }
      if (hi <= lo) hi = lo + 1;
      return { lo, hi, bins: 40 };
    },
    snap(col, x) {
      if (col === "sigma") return Math.round(x * 4) / 4;
      const b = this.strip && this.strip.scales[col];
      if (!b) return x;
      const step = (b.hi - b.lo) / b.bins;
      const mag = Math.pow(10, Math.floor(Math.log10(Math.abs(step) || 1)));
      return Math.round(x / mag) * mag;
    },
    buildStrip() {
      const box = this.$refs.strip;
      box.innerHTML = "";
      if (!this.s) return;
      const s = this.s;
      this.strip = { cards: [], scales: {} };
      const row = el("div", "display:flex;flex-wrap:wrap;gap:6px;align-items:stretch;");
      box.appendChild(row);
      (this.localDraft.filters || []).forEach((f, k) => {
        const col = s.cols[f.col] || { label: f.col, unit: "" };
        const card = el("div", "position:relative;display:flex;flex-direction:column;gap:2px;padding:4px 6px;background:var(--cb-panel,#fff);border-radius:4px;outline:1px solid #dbe3ec;outline-offset:-1px;min-width:190px;max-width:320px;", "cbk-fcard");
        const head = el("div", "display:flex;align-items:center;gap:6px;font-size:9px;color:#475569;");
        const name = el("span", "font-weight:600;color:#1e293b;");
        name.textContent = col.label + (col.unit ? ` (${col.unit})` : "");
        name.title = `${f.col}: double-click the histogram to clear the range`;
        const range = el("span", "font-family:'IBM Plex Mono',monospace;color:#334155;");
        const x = el("span", "margin-left:auto;cursor:pointer;color:#94a3b8;", "cbk-chip");
        x.textContent = "✕";
        x.title = "Remove this filter";
        x.onclick = () => { this.localDraft.filters.splice(k, 1); this.release(); };
        head.append(name, range, x);
        card.appendChild(head);
        const entry = { f, k, card, range };
        if (f.kind === "range") {
          const sc = this.binsFor(f.col);
          if (sc) {
            this.strip.scales[f.col] = sc;
            const cv = el("canvas", "width:100%;height:36px;display:block;cursor:crosshair;touch-action:none;");
            card.appendChild(cv);
            entry.canvas = cv;
            entry.scale = sc;
            this.wireHistogram(entry);
          }
          const cov = el("div", "font-size:9px;color:#64748b;");
          entry.cov = cov;
          card.appendChild(cov);
        } else if (f.kind === "category") {
          const chips = el("div", "display:flex;flex-wrap:wrap;gap:3px;max-height:64px;overflow-y:auto;");
          entry.chips = chips;
          card.appendChild(chips);
        }
        const rem = el("div", "font-size:9px;color:#475569;cursor:pointer;align-self:flex-start;padding:0 3px;border-radius:3px;", "cbk-chip");
        rem.onclick = () => { this.inverted = this.inverted === k ? -1 : k; this.reorder(); this.paintStrip(); };
        entry.rem = rem;
        card.appendChild(rem);
        this.strip.cards.push(entry);
        row.appendChild(card);
      });
      const add = el("div", "align-self:center;font-size:10px;color:#2c4a6b;cursor:pointer;padding:2px 6px;border-radius:3px;outline:1px solid #cbd5e1;", "cbk-chip");
      add.textContent = "+ filter";
      add.onclick = (e) => this.openAddMenu(e, add);
      row.appendChild(add);
      box.appendChild(this.buildPostOps());
      const lower = el("div", "display:flex;align-items:center;gap:14px;flex-wrap:wrap;");
      const edge = el("div", "display:flex;align-items:center;gap:3px;min-height:0;", "cbk-edge");
      const avg = el("div", "display:flex;align-items:center;gap:6px;", "cbk-avg");
      this.strip.edge = edge;
      this.strip.avg = avg;
      lower.append(edge, avg);
      box.appendChild(lower);
      this.paintAverages();
      this.paintStrip();
    },
    buildPostOps() {
      const line = el("div", "display:flex;align-items:center;gap:6px;font-size:9px;color:#475569;");
      const post = this.localDraft.post || (this.localDraft.post = {});
      const field = (label, key, unit, step) => {
        const wrap = el("label", "display:inline-flex;align-items:center;gap:3px;");
        const t = el("span");
        t.textContent = label;
        const inp = el("input", "width:56px;height:16px;font-size:10px;font-family:'IBM Plex Mono',monospace;border:1px solid #cbd5e1;border-radius:4px;padding:0 4px;background:#fff;color:#0f172a;", "cbk-num");
        inp.type = "number"; inp.min = "0"; inp.step = step;
        inp.value = post[key] == null ? "" : post[key];
        inp.onchange = () => {
          const v = inp.value === "" ? null : +inp.value;
          post[key] = v && v > 0 ? (key === "top_n" ? Math.floor(v) : v) : null;
          this.release();
        };
        const u = el("span");
        u.textContent = unit;
        wrap.append(t, inp, u);
        return wrap;
      };
      const then = el("span", "color:#64748b;");
      then.textContent = "then, per tomogram:";
      line.append(then, field("no two closer than", "nn_a", "Å", "10"), field("· at most", "top_n", "best", "1"));
      return line;
    },
    openAddMenu(e, anchor) {
      const old = document.querySelector(".cbk-menu");
      if (old) { old.remove(); return; }
      const s = this.s;
      const menu = el("div", "position:fixed;z-index:9000;max-height:320px;overflow-y:auto;padding:4px;font-size:10px;min-width:260px;", "cbk-menu cb-glass");
      const r = anchor.getBoundingClientRect();
      menu.style.left = `${r.left}px`;
      menu.style.top = `${r.bottom + 2}px`;
      const used = new Set((this.localDraft.filters || []).map((f) => f.col));
      let group = "";
      for (const c of s.header.columns) {
        if (used.has(c.name) || c.name === "frame") continue;
        if (c.group !== group) {
          group = c.group;
          const g = el("div", "font-size:9px;color:#64748b;padding:4px 4px 1px 4px;");
          g.textContent = group;
          menu.appendChild(g);
        }
        const item = el("div", "display:flex;gap:8px;padding:2px 6px;cursor:pointer;border-radius:3px;", "cbk-chip");
        const a = el("span", "color:#0f172a;");
        a.textContent = c.label + (c.unit ? ` (${c.unit})` : "");
        const b = el("span", "margin-left:auto;color:#64748b;font-family:'IBM Plex Mono',monospace;");
        b.textContent = c.coverage < s.n ? `${c.coverage.toLocaleString()} of ${s.n.toLocaleString()}` : "all rows";
        item.append(a, b);
        item.onclick = () => {
          menu.remove();
          const f = c.kind === "num"
            ? { kind: "range", col: c.name, lo: null, hi: null, keep_missing: c.keep_missing !== false }
            : { kind: "category", col: c.name, allowed: [...(c.categories || [])] };
          this.localDraft.filters.push(f);
          this.release();
        };
        menu.appendChild(item);
      }
      document.body.appendChild(menu);
      setTimeout(() => document.addEventListener("pointerdown", function off(ev) {
        if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener("pointerdown", off); }
      }), 0);
    },
    wireHistogram(entry) {
      const cv = entry.canvas, f = entry.f, sc = entry.scale;
      const xOf = (v) => ((v - sc.lo) / (sc.hi - sc.lo)) * cv.clientWidth;
      const vOf = (x) => sc.lo + (x / cv.clientWidth) * (sc.hi - sc.lo);
      cv.addEventListener("pointerdown", (e) => {
        const b = cv.getBoundingClientRect(), x = e.clientX - b.left;
        const lx = f.lo == null ? -1e9 : xOf(f.lo), hx = f.hi == null ? 1e9 : xOf(f.hi);
        let which = Math.abs(x - lx) <= 6 ? "lo" : Math.abs(x - hx) <= 6 ? "hi" : "new";
        this.drag = { entry, which, x0: x };
        cv.setPointerCapture(e.pointerId);
      });
      cv.addEventListener("pointermove", (e) => {
        if (!this.drag || this.drag.entry !== entry) return;
        const b = cv.getBoundingClientRect(), x = Math.min(Math.max(e.clientX - b.left, 0), cv.clientWidth);
        const v = this.snap(f.col, vOf(x));
        if (this.drag.which === "new") {
          if (Math.abs(x - this.drag.x0) < 3) return;
          const v0 = this.snap(f.col, vOf(this.drag.x0));
          f.lo = Math.min(v0, v); f.hi = Math.max(v0, v);
        } else {
          f[this.drag.which] = v;
          if (f.lo != null && f.hi != null && f.lo > f.hi) [f.lo, f.hi] = [f.hi, f.lo];
        }
        // An end dragged past the histogram's edge is open there.
        if (x <= 0.5) f.lo = null;
        if (x >= cv.clientWidth - 0.5) f.hi = null;
        this.liveUpdate();
      });
      cv.addEventListener("pointerup", () => {
        if (!this.drag) return;
        this.drag = null;
        this.release();
      });
      cv.addEventListener("dblclick", () => { f.lo = null; f.hi = null; this.release(); });
    },
    liveUpdate() {
      if (this.liveRaf) return;
      this.liveRaf = requestAnimationFrame(() => {
        this.liveRaf = 0;
        const ev = evaluate(this.s, this.localDraft, this.frame_choice);
        this.sel = { ...ev, after: ev.passes, final: finalMask(this.s, ev.passes), live: true };
        this.renderCounts();
        const now = performance.now();
        const slow = !this.lastLiveGrid || now - this.lastLiveGrid > 120;
        this.paintStrip(!slow);
        this.drawDots();
        if (slow) {
          this.lastLiveGrid = now;
          this.reorder();
        }
      });
    },
    release() {
      this.inverted = this.inverted >= (this.localDraft.filters || []).length ? -1 : this.inverted;
      this.recompute(true);
      clearTimeout(this.draftTimer);
      this.draftTimer = setTimeout(() => {
        let n = 0;
        for (let i = 0; i < this.s.n; i++) n += this.sel.final[i];
        this.$emit("draft", { draft: this.localDraft, n });
      }, 300);
    },
    paintStrip(skipEdge) {
      if (!this.strip || !this.sel) return;
      const s = this.s, sel = this.sel;
      for (const e of this.strip.cards) {
        const f = e.f;
        // Rows passing every other filter: failing none, or failing only this one.
        const others = new Uint8Array(s.n);
        const m = sel.masks && sel.masks[e.k];
        for (let i = 0; i < s.n; i++) others[i] = sel.base[i] && (sel.failing[i] === 0 || (sel.failing[i] === 1 && m && !m[i])) ? 1 : 0;
        if (f.kind === "range") {
          e.range.textContent = `${f.lo == null ? "…" : fmt(f.lo, 2)} – ${f.hi == null ? "…" : fmt(f.hi, 2)}`;
          if (e.canvas) this.paintHistogram(e, others);
          const [c, n] = (sel.coverage && sel.coverage[e.k]) || [s.n, s.n];
          e.cov.innerHTML = "";
          if (c < n) {
            const t = el("span");
            t.textContent = `applies to ${c.toLocaleString()} of ${n.toLocaleString()} · ${(n - c).toLocaleString()} have no value: `;
            const k = el("span", `cursor:pointer;${f.keep_missing !== false ? "font-weight:600;color:#0f172a;" : ""}`, "cbk-chip");
            k.textContent = "keep";
            k.onclick = () => { f.keep_missing = true; this.release(); };
            const d = el("span", `cursor:pointer;${f.keep_missing === false ? "font-weight:600;color:#b91c1c;" : ""}`, "cbk-chip");
            d.textContent = "drop";
            d.onclick = () => { f.keep_missing = false; this.release(); };
            e.cov.append(t, k, document.createTextNode(" | "), d);
          }
        } else if (f.kind === "category" && e.chips) {
          this.paintChips(e, others);
        }
        const r = (sel.removes || [])[e.k] || 0;
        e.rem.textContent = this.inverted === e.k ? `showing the ${r.toLocaleString()} it removes · back` : `removes ${r.toLocaleString()}`;
        e.rem.style.fontWeight = this.inverted === e.k ? "600" : "";
      }
      if (!skipEdge) this.paintEdge();
    },
    paintHistogram(e, others) {
      const cv = e.canvas, sc = e.scale, f = e.f;
      const w = cv.clientWidth, h = cv.clientHeight, dpr = window.devicePixelRatio || 1;
      if (!w) return;
      cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr);
      const g = cv.getContext("2d");
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, w, h);
      const { all, sel } = histogram(this.s.num[f.col], others, sc.bins, sc.lo, sc.hi);
      let mx = 1;
      for (const c of all) mx = Math.max(mx, c);
      const bw = w / sc.bins, scale = (h - 2) / Math.sqrt(mx);
      for (let b = 0; b < sc.bins; b++) {
        const ha = Math.sqrt(all[b]) * scale, hs = Math.sqrt(sel[b]) * scale;
        g.fillStyle = "#dbe3ec"; g.fillRect(b * bw + 0.5, h - ha, bw - 1, ha);
        g.fillStyle = "#475569"; g.fillRect(b * bw + 0.5, h - hs, bw - 1, hs);
      }
      const xOf = (v) => ((v - sc.lo) / (sc.hi - sc.lo)) * w;
      g.fillStyle = "rgba(255,255,255,0.65)";
      if (f.lo != null) g.fillRect(0, 0, Math.max(0, xOf(f.lo)), h);
      if (f.hi != null) g.fillRect(Math.min(w, xOf(f.hi)), 0, w, h);
      g.fillStyle = "#2c4a6b";
      for (const v of [f.lo, f.hi]) if (v != null) g.fillRect(Math.min(w - 2, Math.max(0, xOf(v) - 1)), 0, 2, h);
    },
    paintChips(e, others) {
      const s = this.s, f = e.f, codes = s.cat[f.col], names = (s.cols[f.col] || {}).categories || [];
      const tot = new Map(), pass = new Map();
      for (let i = 0; i < s.n; i++) { const c = codes[i]; tot.set(c, (tot.get(c) || 0) + 1); if (others[i]) pass.set(c, (pass.get(c) || 0) + 1); }
      const allowed = new Set(f.allowed || []);
      e.chips.innerHTML = "";
      const list = [...tot.entries()].sort((a, b) => b[1] - a[1]);
      const shown = e.expanded ? list : list.slice(0, 12);
      for (const [c, n] of shown) {
        const name = names[c] === undefined ? "?" : names[c];
        const on = allowed.has(name);
        const chip = el("span", `font-size:9px;padding:0 4px;border-radius:3px;cursor:pointer;outline:1px solid ${on ? "#2c4a6b" : "#cbd5e1"};color:${on ? "#0f172a" : "#94a3b8"};`, "cbk-chip");
        chip.textContent = `${name.length > 28 ? name.slice(0, 12) + "…" + name.slice(-14) : name} ${(pass.get(c) || 0).toLocaleString()}/${n.toLocaleString()}`;
        chip.title = name;
        chip.onclick = () => { if (on) allowed.delete(name); else allowed.add(name); f.allowed = [...allowed]; this.release(); };
        e.chips.appendChild(chip);
      }
      if (list.length > 12) {
        const more = el("span", "font-size:9px;color:#2c4a6b;cursor:pointer;", "cbk-chip");
        more.textContent = e.expanded ? "fewer" : `+${list.length - 12} more`;
        more.onclick = () => { e.expanded = !e.expanded; this.paintChips(e, others); };
        e.chips.appendChild(more);
      }
      const all = el("span", "font-size:9px;color:#2c4a6b;cursor:pointer;", "cbk-chip");
      all.textContent = allowed.size ? "none" : "all";
      all.onclick = () => { f.allowed = allowed.size ? [] : names.slice(); this.release(); };
      e.chips.appendChild(all);
    },
    // The six cutouts just above and the six just below the σ filter's low edge, among the rows that
    // pass every other filter.
    paintEdge() {
      const box = this.strip && this.strip.edge;
      if (!box) return;
      const k = (this.localDraft.filters || []).findIndex((f) => f.kind === "range" && f.col === "sigma" && f.lo != null);
      box.innerHTML = "";
      if (k < 0) return;
      const s = this.s, sel = this.sel, cut = Math.fround(this.localDraft.filters[k].lo), sig = s.num.sigma;
      const m = sel.masks[k];
      // One pass keeping the six nearest on each side: no sort of the whole pool per drag frame.
      const above = [], below = [];
      const keep6 = (list, i, better) => {
        if (list.length < 6) { list.push(i); list.sort(better); return; }
        if (better(i, list[5]) < 0) { list[5] = i; list.sort(better); }
      };
      const up = (a, b) => sig[a] - sig[b] || a - b, down = (a, b) => sig[b] - sig[a] || a - b;
      for (let i = 0; i < s.n; i++) {
        if (!sel.base[i] || !(sel.failing[i] === 0 || (sel.failing[i] === 1 && !m[i]))) continue;
        const v = sig[i];
        if (v !== v) continue;
        if (v >= cut) keep6(above, i, up); else keep6(below, i, down);
      }
      const label = el("span", "font-size:9px;color:#64748b;margin-right:4px;");
      label.textContent = `edge of the cut at ${fmt(cut, 2)} σ:`;
      box.appendChild(label);
      const tile = (i, side) => {
        const t = el("div", `position:relative;width:44px;height:44px;border-radius:2px;overflow:hidden;background:#1e293b;cursor:pointer;opacity:${side ? 1 : 0.6};`, "cbk-tile");
        t.dataset.row = i;
        if (s.num.x_px[i] === s.num.x_px[i]) {
          const img = el("img", "width:100%;height:100%;display:block;");
          img.src = this.url(this.tile_url + uidHex(s, i));
          t.appendChild(img);
        }
        t.title = this.tileText(i);
        t.onclick = () => this.setFocus(s.cat.frame[i], i);
        return t;
      };
      for (const i of below.slice(0, 6).reverse()) box.appendChild(tile(i, false));
      const bar = el("span", "width:2px;height:40px;background:#2c4a6b;margin:0 3px;");
      box.appendChild(bar);
      for (const i of above.slice(0, 6)) box.appendChild(tile(i, true));
    },
    // Unaligned means of a sample of the kept and of the removed cutouts, made by the server on release.
    paintAverages() {
      const box = this.strip && this.strip.avg;
      if (!box) return;
      box.innerHTML = "";
      const a = this.averages || {};
      for (const which of ["kept", "removed"]) {
        const e = a[which];
        if (!e || !e.url) continue;
        const cell = el("div", "display:flex;flex-direction:column;align-items:center;gap:1px;");
        const img = el("img", "width:56px;height:56px;border-radius:2px;display:block;background:#1e293b;");
        img.src = this.url(e.url);
        img.title = `unaligned mean of ${e.n} of the ${e.of.toLocaleString()} ${which} cutouts (a fixed random sample)`;
        const t = el("span", "font-size:8px;color:#64748b;");
        t.textContent = `${which} mean`;
        cell.append(img, t);
        box.appendChild(cell);
      }
    },
    // ── Curation (S3) ──────────────────────────────────────────────────────────────────────────
    setVerdict(rows, value) {
      const s = this.s;
      const code = value === "keep" ? KEEP : value === "drop" ? DROP : 0;
      for (const r of rows) s.verdict[r] = code;
      const uids = rows.map((r) => uidHex(s, r));
      const q = this.pendingV || (this.pendingV = new Map());
      q.set(value, [...(q.get(value) || []), ...uids]);
      clearTimeout(this.vTimer);
      this.vTimer = setTimeout(() => {
        for (const [v, list] of this.pendingV) this.$emit("verdict", { uids: list, value: v });
        this.pendingV = new Map();
      }, 250);
      this.sel = { ...this.sel, final: finalMask(s, this.sel.after) };
      this.renderCounts();
      this.drawDots();
      this.refreshTileClasses();
    },
    refreshTileClasses() {
      for (const [key, node] of this.tiles) {
        if (typeof key !== "number") continue;
        const v = this.s.verdict[+node.dataset.row];
        node.classList.toggle("cbk-drop", v === DROP);
        node.classList.toggle("cbk-keep", v === KEEP);
        node.classList.toggle("cbk-cursor", key === this.cursor);
      }
    },
    toggleTag(rows, tag) {
      const s = this.s, bit = 1 << tag;
      const on = rows.some((r) => !(s.tags[r] & bit));
      for (const r of rows) s.tags[r] = on ? s.tags[r] | bit : s.tags[r] & ~bit;
      this.$emit("tag", { uids: rows.map((r) => uidHex(s, r)), tag, on });
      this.renderWindow(true);
    },
    clickVerdict(row) {
      const v = this.s.verdict[row];
      if (this.sel.after[row]) this.setVerdict([row], v === DROP ? "clear" : "drop");
      else this.setVerdict([row], v === KEEP ? "clear" : "keep");
    },
    onKey(e) {
      if (!this.s || !this.order.length || !this.layout) return;
      const cols = this.layout.cols;
      const k = Math.min(Math.max(this.cursor, 0), this.order.length - 1);
      const moves = { ArrowRight: 1, ArrowLeft: -1, ArrowDown: cols, ArrowUp: -cols };
      if (e.key in moves) {
        e.preventDefault();
        const next = Math.min(Math.max(k + moves[e.key], 0), this.order.length - 1);
        this.scrollToK(next);
        this.activeRow = this.order[next];
        this.drawDots();
        return;
      }
      const row = this.order[k];
      if (e.key === "k") this.setVerdict([row], "keep");
      else if (e.key === "x") this.setVerdict([row], "drop");
      else if (e.key === "u") this.setVerdict([row], "clear");
      else if (e.key === "Enter") this.setFocus(this.s.cat.frame[row], row);
      else if (/^[1-9]$/.test(e.key)) {
        const t = (this.tag_defs || []).find((d) => d.key === +e.key);
        if (t) this.toggleTag([row], t.id);
      } else return;
      e.preventDefault();
    },
    // A drag over tiles while curating: one direction for the whole box, by majority.
    onGridDown(e) {
      if (!this.curating || e.button !== 0) return;
      const grid = this.$refs.grid, b = grid.getBoundingClientRect();
      this.lasso = { x0: e.clientX - b.left, y0: e.clientY - b.top + grid.scrollTop, moved: false, box: null };
    },
    onGridMove(e) {
      const l = this.lasso;
      if (!l) return;
      const grid = this.$refs.grid, b = grid.getBoundingClientRect();
      const x = e.clientX - b.left, y = e.clientY - b.top + grid.scrollTop;
      if (!l.moved && Math.hypot(x - l.x0, y - l.y0) < 6) return;
      l.moved = true;
      if (!l.box) {
        l.box = el("div", "position:absolute;border:1px solid #4e79a7;background:rgba(78,121,167,0.12);pointer-events:none;z-index:4;");
        this.$refs.sizer.appendChild(l.box);
      }
      Object.assign(l.box.style, { left: `${Math.min(x, l.x0)}px`, top: `${Math.min(y, l.y0)}px`, width: `${Math.abs(x - l.x0)}px`, height: `${Math.abs(y - l.y0)}px` });
      l.x1 = x; l.y1 = y;
    },
    onGridUp() {
      const l = this.lasso;
      this.lasso = null;
      if (!l || !l.moved) return;
      if (l.box) l.box.remove();
      const [x0, x1] = [Math.min(l.x0, l.x1), Math.max(l.x0, l.x1)], [y0, y1] = [Math.min(l.y0, l.y1), Math.max(l.y0, l.y1)];
      const rows = [];
      for (const [key, node] of this.tiles) {
        if (typeof key !== "number") continue;
        const nx = parseFloat(node.style.left), ny = parseFloat(node.style.top), w = node.offsetWidth;
        if (nx + w >= x0 && nx <= x1 && ny + w >= y0 && ny <= y1) rows.push(+node.dataset.row);
      }
      if (!rows.length) return;
      this.suppressClick = true;
      const dropped = rows.filter((r) => this.s.verdict[r] === DROP).length;
      this.setVerdict(rows, dropped * 2 > rows.length ? "clear" : "drop");
    },
    // ── By-tomogram stream (S4) ────────────────────────────────────────────────────────────────
    streamMode() { return this.mode === "tomo"; },
    applyMode() {
      const tomo = this.streamMode();
      this.$refs.main.style.display = tomo ? "none" : "grid";
      this.$refs.stream.style.display = tomo ? "block" : "none";
    },
    buildStream() {
      if (!this.s || !this.sel) return;
      const st = streamFrames(this.s, this.shownMask(), this.row_order || "passing");
      const cw = SIZES[this.size || "M"] >= 256 ? 340 : 280;
      const rows = [];
      let y = 0;
      const add = (e, folded) => {
        const fr = this.s.frames[e.code] || {};
        const dims = fr.dims || [1, 1, 1];
        const hxy = Math.round((cw * dims[1]) / dims[0]), hxz = Math.max(24, Math.round((cw * dims[2]) / dims[0] / 2));
        const h = 18 + hxy + 4 + hxz + 14;
        rows.push({ e, y, h, hxy, hxz, cw, folded });
        y += h;
      };
      for (const e of st.frames) add(e, false);
      if (st.empty.length) {
        rows.push({ fold: true, y, h: 26, n: st.empty.length });
        y += 26;
        if (this.showEmpty) for (const e of st.empty) add(e, true);
      }
      this.streamLayout = { rows, height: y };
      const sizer = this.$refs.streamSizer;
      sizer.style.height = `${y}px`;
      for (const node of this.streamNodes.values()) node.remove();
      this.streamNodes.clear();
      const target = rows.findIndex((r) => r.e && r.e.code === this.focusFrame);
      if (target >= 0 && this.keepStreamTarget) this.$refs.stream.scrollTop = rows[target].y;
      this.keepStreamTarget = false;
      this.renderStream();
    },
    renderStream() {
      const L = this.streamLayout;
      if (!L) return;
      const sc = this.$refs.stream, top = sc.scrollTop, h = sc.clientHeight;
      const want = new Set();
      L.rows.forEach((r, k) => { if (r.y + r.h >= top - h && r.y <= top + 2 * h) want.add(k); });
      for (const [k, node] of this.streamNodes) if (!want.has(k)) { node.remove(); this.streamNodes.delete(k); }
      for (const k of want) {
        if (this.streamNodes.has(k)) continue;
        const r = L.rows[k];
        const node = r.fold ? this.makeFold(r) : this.makeStreamRow(r);
        this.$refs.streamSizer.appendChild(node);
        this.streamNodes.set(k, node);
      }
    },
    makeFold(r) {
      const node = el("div", `position:absolute;left:0;right:0;top:${r.y}px;height:${r.h}px;display:flex;align-items:center;font-size:10px;color:#475569;cursor:pointer;`, "cbk-chip");
      node.textContent = `${r.n} tomogram${r.n === 1 ? "" : "s"} with nothing passing ${this.showEmpty ? "▾" : "▸"}`;
      node.onclick = () => { this.showEmpty = !this.showEmpty; this.buildStream(); };
      return node;
    },
    makeStreamRow(r) {
      const s = this.s, e = r.e, fr = s.frames[e.code] || {};
      const node = el("div", `position:absolute;left:0;right:0;top:${r.y}px;height:${r.h}px;`);
      const title = el("div", "height:16px;font-size:10px;color:#334155;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;", "cbk-ttl");
      const other = (s.header.conflicts || {})[fr.acq] ? " · ⧉ in another frame" : "";
      title.textContent = `${fr.tomo} · ${fr.project}${other} · ${e.rows.length}/${e.total} · CTF ${fmt(e.ctf, 1)} Å · ${fmt(e.tilts, 0)} tilts`;
      if (!fr.apix) title.classList.add("cbk-red");
      node.appendChild(title);
      const body = el("div", "position:absolute;left:0;right:0;top:18px;bottom:14px;");
      node.appendChild(body);
      const card = el("div", `position:absolute;left:0;top:0;width:${r.cw}px;display:flex;flex-direction:column;gap:4px;`);
      const panes = [];
      for (const [which, ph] of [["xy", r.hxy], ["xz", r.hxz]]) {
        const pane = el("div", `position:relative;width:${r.cw}px;height:${ph}px;background:#0f172a;border-radius:2px;overflow:hidden;`);
        if (fr.recon_exists) {
          const img = el("img", "position:absolute;inset:0;width:100%;height:100%;display:block;opacity:0;transition:opacity 120ms;");
          img.draggable = false;
          img.onload = () => { img.style.opacity = 1; };
          img.src = this.url(this.slab_url.replace("{frame}", e.code).replace("{which}", which));
          pane.appendChild(img);
        }
        const cv = el("canvas", "position:absolute;inset:0;width:100%;height:100%;");
        pane.appendChild(cv);
        card.appendChild(pane);
        panes.push({ which, cv, w: (fr.dims || [1])[0], h: which === "xy" ? (fr.dims || [1, 1])[1] : (fr.dims || [1, 1, 1])[2], pw: r.cw, ph });
      }
      body.appendChild(card);
      const gap = 56;
      const box = el("div", `position:absolute;left:${r.cw + gap}px;right:0;top:0;height:${r.hxy + 4 + r.hxz}px;overflow-y:auto;overflow-x:hidden;`, "cbk-sbox");
      const inner = el("div", "position:relative;width:100%;");
      box.appendChild(inner);
      body.appendChild(box);
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      svg.setAttribute("style", "position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible;");
      body.appendChild(svg);
      const R = { r, e, fr, panes, box, inner, svg, gap, tiles: new Map(), hover: -1 };
      box.addEventListener("scroll", () => this.renderBox(R), { passive: true });
      box.addEventListener("mouseover", (ev) => {
        const t = ev.target.closest && ev.target.closest(".cbk-tile");
        const row = t ? +t.dataset.row : -1;
        if (t) t.title = this.tileText(row);
        if (row !== R.hover) { R.hover = row; this.drawStreamRow(R); }
      });
      box.addEventListener("mouseleave", () => { R.hover = -1; this.drawStreamRow(R); });
      box.addEventListener("click", (ev) => {
        const t = ev.target.closest && ev.target.closest(".cbk-tile");
        if (!t) return;
        const row = +t.dataset.row;
        if (this.curating) { this.clickVerdict(row); this.drawStreamRow(R); this.refreshStreamClasses(R); }
        else this.focusInPool(row);
      });
      panes[0].cv.addEventListener("mousemove", (ev) => {
        const b = panes[0].cv.getBoundingClientRect();
        let best = -1, bd = 64;
        for (const [x, y, i] of R.pts || []) { const d = (x - (ev.clientX - b.left)) ** 2 + (y - (ev.clientY - b.top)) ** 2; if (d < bd) { bd = d; best = i; } }
        if (best !== R.hover) { R.hover = best; this.drawStreamRow(R); }
      });
      node._row = R;
      requestAnimationFrame(() => this.renderBox(R));
      return node;
    },
    renderBox(R) {
      const px = Math.min(this.tilePx(), 160);
      const bw = R.box.clientWidth || 1;
      const cols = Math.max(1, Math.floor((bw + GAP) / (px + GAP)));
      const n = R.e.rows.length;
      R.cols = cols; R.px = px;
      R.inner.style.height = `${Math.ceil(n / cols) * (px + GAP)}px`;
      const top = R.box.scrollTop, h = R.box.clientHeight;
      const k0 = Math.max(0, Math.floor((top - h) / (px + GAP)) * cols), k1 = Math.min(n, Math.ceil((top + 2 * h) / (px + GAP)) * cols);
      for (const [k, node] of R.tiles) if (k < k0 || k >= k1) { node.remove(); R.tiles.delete(k); }
      const need = [];
      for (let k = k0; k < k1; k++) {
        if (R.tiles.has(k)) continue;
        const row = R.e.rows[k];
        const t = this.makeTile(row, -1, (k % cols) * (px + GAP), Math.floor(k / cols) * (px + GAP), px);
        R.inner.appendChild(t);
        R.tiles.set(k, t);
        need.push(uidHex(this.s, row));
      }
      if (need.length) this.$emit("need_tiles", { uids: need });
      this.drawStreamRow(R);
    },
    refreshStreamClasses(R) {
      for (const node of R.tiles.values()) {
        const v = this.s.verdict[+node.dataset.row];
        node.classList.toggle("cbk-drop", v === DROP);
        node.classList.toggle("cbk-keep", v === KEEP);
      }
    },
    // Dots on the row's slabs and a leader from each dot to its tile, for the tiles in view: faint in
    // the species colour, the hovered pair solid.
    drawStreamRow(R) {
      const s = this.s, color = this.species_color || "#3b82f6", dpr = window.devicePixelRatio || 1;
      const frameRows = [];
      for (let i = 0; i < s.n; i++) if (s.cat.frame[i] === R.e.code) frameRows.push(i);
      for (const p of R.panes) {
        p.cv.width = Math.round(p.pw * dpr); p.cv.height = Math.round(p.ph * dpr);
        const g = p.cv.getContext("2d");
        g.setTransform(dpr, 0, 0, dpr, 0, 0);
        g.clearRect(0, 0, p.pw, p.ph);
        const sx = p.pw / p.w, sy = p.ph / p.h, vcol = p.which === "xy" ? s.num.y_px : s.num.z_px;
        if (p.which === "xy") R.pts = [];
        for (const i of frameRows) {
          const x = s.num.x_px[i] * sx, y = p.ph - vcol[i] * sy;
          if (!(x === x && y === y)) continue;
          const on = this.sel.final[i];
          g.beginPath(); g.arc(x, y, i === R.hover ? 4.5 : 2.5, 0, 2 * Math.PI);
          if (on) { g.fillStyle = color; g.fill(); g.lineWidth = 1; g.strokeStyle = "#fff"; g.stroke(); }
          else { g.lineWidth = 1; g.strokeStyle = "rgba(226,232,240,0.8)"; g.stroke(); }
          if (p.which === "xy") R.pts.push([x, y, i]);
        }
      }
      while (R.svg.firstChild) R.svg.removeChild(R.svg.firstChild);
      const p0 = R.panes[0], sx = p0.pw / p0.w, sy = p0.ph / p0.h;
      const top = R.box.scrollTop, boxH = R.box.clientHeight;
      for (const [k, node] of R.tiles) {
        const row = R.e.rows[k];
        const ty = Math.floor(k / R.cols) * (R.px + GAP) - top + R.px / 2;
        if (ty < 0 || ty > boxH) continue;
        const x1 = s.num.x_px[row] * sx, y1 = p0.ph - s.num.y_px[row] * sy;
        if (!(x1 === x1 && y1 === y1)) continue;
        const x2 = R.r.cw + R.gap + (k % R.cols) * (R.px + GAP), y2 = ty;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const mx = R.r.cw + R.gap / 2;
        path.setAttribute("d", `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`);
        const hot = row === R.hover;
        path.setAttribute("stroke", hot ? "#ffffff" : color);
        path.setAttribute("stroke-opacity", hot ? "1" : "0.28");
        path.setAttribute("stroke-width", hot ? "1.6" : "1");
        path.setAttribute("fill", "none");
        if (hot) {
          const under = path.cloneNode();
          under.setAttribute("stroke", "#0f172a");
          under.setAttribute("stroke-width", "3");
          R.svg.appendChild(under);
        }
        R.svg.appendChild(path);
        node.classList.toggle("cbk-active", hot);
      }
    },
    // A tile clicked in the stream opens its tomogram in pool mode.
    focusInPool(row) {
      this.setFocus(this.s.cat.frame[row], row);
      this.$emit("mode", { mode: "pool" });
    },
    markActiveTile() {
      for (const [key, node] of this.tiles) {
        if (typeof key !== "number") continue;
        node.classList.toggle("cbk-active", +node.dataset.row === this.activeRow);
      }
    },
  },
};
