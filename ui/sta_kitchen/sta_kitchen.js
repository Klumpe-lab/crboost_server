// The STA kitchen's one element: the filter strip, the cutout gallery (pool mode) and the by-tomogram
// stream, drawn from one in-browser store of the pool table (services/sta_kitchen/payload.py).
//
// The store functions at the top touch no DOM: `node` runs them against the server's reading of the
// same recipe (services/sta_kitchen/pool.py `select`, roadmap 40 §4.5). The component below them draws.
// Cutouts come a screen at a time from /api/kitchen/tiles and stay in memory for the session: a re-sort,
// a filter or a mode switch moves the tiles on screen, it never fetches or decodes them again.

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
  store.hex = null;
  store.rowOf = null;
  return store;
}

// A set's rows as the server sends them (services/sta_kitchen/setmatch.py `mask_payload`): base64 of bits, the
// first row in the lowest bit of the first byte.
export function decodeMask(b64, n) {
  const bin = atob(b64), out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (bin.charCodeAt(i >> 3) >> (i & 7)) & 1;
  return out;
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
  if (f.kind === "set") {
    // The rows the set holds (`store.setMasks`, from the server's match); none yet: it passes everything.
    const m = store.setMasks && store.setMasks[f.set_id];
    if (!m) return out.fill(1);
    const flip = f.mode === "out" ? 1 : 0;
    for (let i = 0; i < n; i++) out[i] = m[i] ^ flip;
    return out;
  }
  if (f.kind === "tomograms") {
    const frame = store.cat.frame;
    const names = (store.cols.frame || {}).categories || [];
    const off = new Uint8Array(names.length + 1);
    const ex = new Set(f.excluded || []);
    names.forEach((key, i) => { if (ex.has(key)) off[i] = 1; });
    for (let i = 0; i < n; i++) out[i] = frame[i] >= 0 && off[frame[i]] ? 0 : 1;
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

// Per row, its member's index (the header's member ranges: a member's rows are contiguous).
export function memberIndex(store) {
  if (!store.memberIdx) {
    const out = new Int32Array(store.n).fill(-1);
    (store.header.members || []).forEach((m, i) => out.fill(i, m.start, m.stop));
    store.memberIdx = out;
  }
  return store.memberIdx;
}

// The acquisition a frame shows, as sources are chosen per acquisition: its label, else the frame itself.
export function acqKey(fr) {
  return fr.acq && fr.acq !== "unknown" ? fr.acq : fr.key;
}

// Rows of an acquisition fed by several sources whose member is not the chosen one are set aside (0).
export function sourceMask(store, choice, out) {
  const n = store.n;
  out = out || new Uint8Array(n);
  out.fill(1);
  const conf = store.header.source_conflicts || {};
  if (!Object.keys(conf).length) return out;
  const index = new Map((store.header.members || []).map((m, i) => [m.key, i]));
  const allowed = new Int32Array(store.frames.length).fill(-1);
  store.frames.forEach((fr, f) => {
    const a = acqKey(fr), c = (choice || {})[a];
    if (c && (conf[a] || []).includes(c)) allowed[f] = index.get(c);
  });
  const frame = store.cat.frame, mi = memberIndex(store);
  for (let i = 0; i < n; i++) {
    const f = frame[i];
    if (f >= 0 && allowed[f] >= 0 && allowed[f] !== mi[i]) out[i] = 0;
  }
  return out;
}

// In one of the scope's included subsets (when any of them has rows here) and in none of its excluded ones.
export function scopeMask(store, scope, out) {
  const n = store.n, masks = store.subMasks || {};
  out = out || new Uint8Array(n);
  const inc = ((scope && scope.include) || []).filter((id) => masks[id]).map((id) => masks[id]);
  if (inc.length) {
    out.fill(0);
    for (const m of inc) for (let i = 0; i < n; i++) out[i] |= m[i];
  } else out.fill(1);
  for (const id of (scope && scope.exclude) || []) {
    const m = masks[id];
    if (m) for (let i = 0; i < n; i++) if (m[i]) out[i] = 0;
  }
  return out;
}

// Filters, the frame and source choices and the subset scope, removals per filter (rows passing every other
// filter and failing it).
export function evaluate(store, draft, choice, srcChoice) {
  const n = store.n;
  const filters = (draft && draft.filters) || [];
  const masks = filters.map((f) => filterMask(store, f));
  const base = choiceMask(store, choice);
  const src = sourceMask(store, srcChoice), scope = scopeMask(store, draft && draft.scope);
  for (let i = 0; i < n; i++) base[i] &= src[i] & scope[i];
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

// kept = (passes the recipe and not dropped by hand) or kept by hand, within the choices and the scope (`base`).
export function finalMask(store, after, base) {
  const n = store.n;
  const out = new Uint8Array(n);
  const v = store.verdict;
  for (let i = 0; i < n; i++) out[i] = ((after[i] && v[i] !== DROP) || v[i] === KEEP) && (!base || base[i]) ? 1 : 0;
  return out;
}

export function select(store, draft, choice, srcChoice) {
  const ev = evaluate(store, draft, choice, srcChoice);
  const after = applyPostOps(store, ev.passes, draft && draft.post);
  const final = finalMask(store, after, ev.base);
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
  if (group === "subset") {
    // The first subset holding the row, in the rail's order; rows in none last.
    const ord = store.subOrder || [], masks = store.subMasks || {};
    for (let k = 0; k < ord.length; k++) if (masks[ord[k]] && masks[ord[k]][row]) return k;
    return 1e9;
  }
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

// The σ-by-tomogram strip's rows: every frame with a pick, the best CTF fit first (none last), then
// project and name. `rowOf[frame code]` is its row, -1 for none.
export function heatRows(store) {
  const nF = store.frames.length, frame = store.cat.frame, c = store.num.ctf_res;
  const ctf = new Float64Array(nF).fill(NaN), seen = new Uint8Array(nF);
  for (let i = 0; i < store.n; i++) {
    const f = frame[i];
    if (f >= 0 && !seen[f]) { seen[f] = 1; ctf[f] = c[i]; }
  }
  const codes = [];
  for (let f = 0; f < nF; f++) if (seen[f]) codes.push(f);
  const name = (f) => store.frames[f].tomo || "", proj = (f) => store.frames[f].project || "";
  const fit = (f) => (ctf[f] === ctf[f] ? ctf[f] : Infinity);
  codes.sort((a, b) => fit(a) - fit(b) || proj(a).localeCompare(proj(b)) || name(a).localeCompare(name(b), undefined, { numeric: true }) || a - b);
  const rowOf = new Int32Array(nF).fill(-1);
  codes.forEach((f, r) => { rowOf[f] = r; });
  return { codes: Int32Array.from(codes), rowOf, ctf };
}

// Counts per (strip row, σ bin) of every row with a σ, and of rows in `mask`; per strip row, all rows
// and rows in `mask`. `sc` null (no σ in the pool): the per-row totals only.
export function heatCounts(store, rowOf, nRows, mask, sc) {
  const bins = sc ? sc.bins : 0, all = new Float32Array(nRows * bins), sel = new Float32Array(nRows * bins);
  const rowAll = new Int32Array(nRows), rowSel = new Int32Array(nRows);
  const frame = store.cat.frame, sig = store.num.sigma, w = sc ? (sc.hi - sc.lo) / sc.bins : 1;
  for (let i = 0; i < store.n; i++) {
    const f = frame[i];
    if (f < 0) continue;
    const r = rowOf[f];
    if (r < 0) continue;
    rowAll[r]++;
    if (mask[i]) rowSel[r]++;
    const v = sig[i];
    if (!bins || v !== v) continue;
    let b = Math.floor((v - sc.lo) / w);
    if (b < 0) b = 0; else if (b >= bins) b = bins - 1;
    all[r * bins + b]++;
    if (mask[i]) sel[r * bins + b]++;
  }
  return { bins, all, sel, rowAll, rowSel };
}

// Pairs of rows in `mask` closer than `rmax` Å (centred) within one frame (a row without a frame: within its
// member and tomogram), from a grid of rmax cells: {a, b, d} with a < b, and `full` false when `cap` cut it.
export function closePairs(store, mask, rmax, cap = 2000000) {
  const groups = new Map();
  for (let i = 0; i < store.n; i++) {
    if (!mask[i]) continue;
    const g = postGroup(store, i);
    let rows = groups.get(g);
    if (!rows) groups.set(g, (rows = []));
    rows.push(i);
  }
  const x = store.num.x_a, y = store.num.y_a, z = store.num.z_a, r2 = rmax * rmax, O = 65536, M = 131072;
  const key = (cx, cy, cz) => ((cx + O) * M + (cy + O)) * M + (cz + O);
  const A = [], B = [], D = [];
  let full = true;
  for (const rows of groups.values()) {
    if (rows.length < 2) continue;
    const cells = new Map(), at = new Map();
    for (const i of rows) {
      if (!(x[i] === x[i] && y[i] === y[i] && z[i] === z[i])) continue;
      const c = [Math.floor(x[i] / rmax), Math.floor(y[i] / rmax), Math.floor(z[i] / rmax)];
      const k = key(c[0], c[1], c[2]);
      let list = cells.get(k);
      if (!list) cells.set(k, (list = []));
      list.push(i);
      at.set(i, c);
    }
    for (const [i, c] of at) {
      for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) for (let dz = -1; dz <= 1; dz++) {
        const list = cells.get(key(c[0] + dx, c[1] + dy, c[2] + dz));
        if (!list) continue;
        for (const j of list) {
          if (j <= i) continue;
          const ex = x[i] - x[j], ey = y[i] - y[j], ez = z[i] - z[j], d2 = ex * ex + ey * ey + ez * ez;
          if (d2 >= r2) continue;
          if (A.length >= cap) { full = false; break; }
          A.push(i); B.push(j); D.push(Math.sqrt(d2));
        }
      }
    }
  }
  return { a: Int32Array.from(A), b: Int32Array.from(B), d: Float32Array.from(D), full, rmax };
}

// The rows of the pairs `lo ≤ d < hi` apart, closest pair first, each pair's two rows side by side (a row
// in several pairs shows at its closest); `dist` maps a row to that pair's distance.
export function pairOrder(pairs, lo, hi) {
  const idx = [];
  for (let k = 0; k < pairs.d.length; k++) if (pairs.d[k] >= lo && pairs.d[k] < hi) idx.push(k);
  idx.sort((p, q) => pairs.d[p] - pairs.d[q] || p - q);
  const out = [], dist = new Map();
  for (const k of idx) {
    for (const r of [pairs.a[k], pairs.b[k]]) {
      if (dist.has(r)) continue;
      dist.set(r, pairs.d[k]);
      out.push(r);
    }
  }
  return { rows: Int32Array.from(out), dist, nPairs: idx.length };
}

// The review sample (roadmap 40 S6.6): up to `k` random rows the recipe keeps (and no hand drops) and `k`
// it removes (untouched by hand), among rows with a cutout, by a seeded hash of their uid; the populations
// they stand for are counted the same way.
export function reviewSample(store, sel, seed, k = 50) {
  const kept = [], removed = [];
  for (let i = 0; i < store.n; i++) {
    if (!(store.cat.frame[i] >= 0 && store.num.x_px[i] === store.num.x_px[i])) continue;
    const v = store.verdict[i];
    if (sel.after[i] && v !== DROP) kept.push(i);
    else if (!sel.after[i] && sel.base[i] && v === 0) removed.push(i);
  }
  const h = (i) => { let s = (store.uid[i] % 2147483647) ^ seed; s = Math.imul(s ^ (s >>> 15), 2246822507); s = Math.imul(s ^ (s >>> 13), 3266489909); return (s ^ (s >>> 16)) >>> 0; };
  const take = (rows) => rows.sort((a, b) => h(a) - h(b) || a - b).slice(0, k).map((i) => uidHex(store, i));
  return { seed, kept: take(kept), removed: take(removed), n_kept: kept.length, n_removed: removed.length };
}

// A proportion's Wilson score interval (95 %): [estimate, low, high]; NaN without a label.
export function wilson(k, n, z = 1.96) {
  if (!n) return [NaN, NaN, NaN];
  const p = k / n, d = 1 + (z * z) / n, c = (p + (z * z) / (2 * n)) / d;
  const h = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / d;
  return [p, Math.max(0, c - h), Math.min(1, c + h)];
}

// What the labels say about the cut: precision (good among the kept), the good share among the removed,
// the good picks kept and lost in the populations, and the share of good picks the cut loses, with
// intervals from the two Wilson intervals (the lost share's ends take the pessimistic and the optimistic
// pairing of them).
export function reviewEstimates(sample, labels) {
  const count = (list) => {
    let g = 0, b = 0;
    for (const u of list) { const l = labels[u]; if (l === "good") g++; else if (l === "bad") b++; }
    return { g, b, n: g + b };
  };
  const K = count(sample.kept), R = count(sample.removed), nK = sample.n_kept, nR = sample.n_removed;
  const p = wilson(K.g, K.n), q = wilson(R.g, R.n);
  const share = (pk, qr) => (qr * nR) / (pk * nK + qr * nR);
  return { K, R, precision: p, removedGood: q, goodKept: p[0] * nK, goodLost: q[0] * nR, lost: [share(p[0], q[0]), share(p[2], q[1]), share(p[1], q[2])] };
}

// The by-tomogram stream: one entry per frame holding a shown row, its shown rows ordered by their
// dot's Y on the slab (top first) then X; frames with nothing shown apart.
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

// One response of /api/kitchen/tiles: records of a 12-character uid, one byte (j: a JPEG follows, r: the
// reason there is none), a length (uint32 LE) and that many bytes (services/sta_kitchen/tiles.py `record`).
// `push` takes the response's chunks as they arrive and returns the records completed so far.
export class TileReader {
  constructor() {
    this.buf = new Uint8Array(0);
  }
  push(chunk) {
    let b = chunk;
    if (this.buf.length) {
      b = new Uint8Array(this.buf.length + chunk.length);
      b.set(this.buf);
      b.set(chunk, this.buf.length);
    }
    const out = [];
    let o = 0;
    while (b.length - o >= 17) {
      const len = (b[o + 13] | (b[o + 14] << 8) | (b[o + 15] << 16) | (b[o + 16] << 24)) >>> 0;
      if (b.length - o - 17 < len) break;
      out.push({ uid: String.fromCharCode(...b.subarray(o, o + 12)), kind: String.fromCharCode(b[o + 12]), data: b.slice(o + 17, o + 17 + len) });
      o += 17 + len;
    }
    this.buf = b.slice(o);
    return out;
  }
}

// ── Component ───────────────────────────────────────────────────────────────────────────────────

const SIZES = { S: 64, M: 96, L: 160, XL: 256 };
const GAP = 5; // between tiles
const PAD = 8; // the views' inset from their panel
const HEADER_H = 20; // a group header in the gallery
const GLYPH = { tm: "", hand: "✋", imported: "⇣", merged: "∪" };
const EDGE_PX = 30; // the edge-of-the-cut tiles
const TILE_BATCH = 120; // cutouts per request
const TILE_FLIGHTS = 2; // requests in flight
const TILE_CACHE_MAX = 6000; // cutouts kept in this tab, the least recently shown dropped first
const ROW_PAD = 6; // inside a by-tomogram card
const TITLE_H = 16;
const ROW_GAP = 8; // between by-tomogram cards
const SLAB_GAP = 8; // between a card's slabs and its cutouts
const CARD_SLAB = { S: 132, M: 160, L: 200, XL: 260 }; // a card's slab width per tile size
const CARD_TILE_MAX = 128; // a card's cutouts are at most this large
const CARD_COLS_MAX = 4; // columns of cutouts in a card before its box scrolls
const SPARE_CARDS = 120; // cards scrolled out of view kept whole, slabs loaded, for when they come back
const RAIL_W = 184; // the subsets rail
const RAIL_FOLDED = 18;
const DOT_R = { S: 1.8, M: 2.6, L: 4 };
const HEAT_BOX_H = 112; // the σ-by-tomogram strip's scroll box
const HEAT_ROW = [3, 8]; // a strip row's height in px: at least, at most
const HIST_H = 54; // a range filter's histogram
const MENU_STYLE = "position:fixed;z-index:9000;max-height:360px;overflow-y:auto;padding:4px;background:#ffffff;border:1px solid #cbd5e1;border-radius:6px;box-shadow:0 10px 28px rgba(15,23,42,0.18),0 1px 3px rgba(15,23,42,0.12);font-family:'IBM Plex Sans',sans-serif;font-size:10px;color:#0f172a;";

function el(tag, style, cls) {
  const e = document.createElement(tag);
  if (style) e.style.cssText = style;
  if (cls) e.className = cls;
  return e;
}

function fmt(v, d = 1) {
  return v === v && v != null ? (+v).toFixed(d) : "–";
}

// A tile's corner number: the chosen column, short, in its own unit.
function metricText(col, unit, v) {
  if (v == null || !(v === v)) return "";
  v = +v;
  if (unit === "σ") return `${v.toFixed(1)}σ`;
  if (unit === "Å") return `${Math.abs(v) < 100 ? v.toFixed(1) : Math.round(v).toLocaleString()} Å`;
  if (unit === "%") return `${Math.round(v)}%`;
  if (col === "lcc") return v.toFixed(3);
  if (col === "rank") return `#${Math.round(v)}`;
  return Number.isInteger(v) ? `${v}` : v.toPrecision(3);
}

export default {
  template: `
    <div ref="root" class="cbk" style="display:flex;flex-direction:column;width:100%;height:100%;min-height:0;position:relative;">
      <div ref="cards" class="cbk-cards" style="display:flex;flex-wrap:wrap;gap:6px;align-items:stretch;"></div>
      <div class="cbk-line" style="display:flex;align-items:center;flex-wrap:wrap;gap:4px 16px;min-height:30px;padding:4px 0 6px 0;">
        <div ref="scope" style="display:flex;align-items:center;gap:4px;font-size:10px;color:#334155;"></div>
        <div ref="post" style="display:flex;align-items:center;gap:6px;font-size:9px;color:#475569;"></div>
        <div ref="edge" style="display:flex;align-items:center;gap:2px;"></div>
        <div ref="counts" style="margin-left:auto;display:flex;align-items:center;gap:8px;"></div>
      </div>
      <div ref="body" class="cbk-body" style="flex:1 1 0%;min-height:0;position:relative;border-radius:8px;background:#eef2f7;overflow:hidden;">
        <div ref="rail" class="cbk-rail" style="position:absolute;left:0;top:0;bottom:0;width:184px;display:flex;flex-direction:column;background:#f8fafc;border-right:1px solid #dbe3ec;font-size:10px;color:#334155;z-index:4;"></div>
        <div ref="selbar" class="cbk-selbar" style="position:absolute;left:50%;bottom:10px;transform:translateX(-50%);z-index:7;display:none;align-items:center;gap:8px;padding:5px 10px;background:#ffffff;border:1px solid #cbd5e1;border-radius:8px;box-shadow:0 8px 24px rgba(15,23,42,0.18);font-size:10px;color:#0f172a;white-space:nowrap;"></div>
        <div ref="grid" class="cbk-grid" tabindex="0" style="position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;outline:none;">
          <div ref="pin" style="position:sticky;top:0;z-index:3;height:0;"></div>
          <div ref="sizer" style="position:relative;width:100%;"></div>
        </div>
        <div ref="stream" class="cbk-stream" tabindex="0" style="position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;visibility:hidden;pointer-events:none;outline:none;">
          <div ref="streamSizer" style="position:relative;width:100%;"></div>
        </div>
        <div ref="review" class="cbk-review" tabindex="0" style="position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;visibility:hidden;pointer-events:none;outline:none;"></div>
      </div>
      <div ref="status" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;font-size:11px;color:#64748b;"></div>
    </div>`,
  props: {
    table_url: String,
    tiles_url: String,
    slab_url: String,
    draft: Object,
    frame_choice: Object,
    mode: String,
    size: String,
    sort: String,
    group: String,
    metric: String,
    species_color: String,
    curating: Boolean,
    row_order: String,
    diameter_a: Number,
    sets: Array,
    set_masks: Object,
    review: Object,
    loupe_url: String,
    loupe_png_url: String,
    source_choice: Object,
    subsets: Array,
    groups: Array,
    dots: Object,
  },
  mounted() {
    this.s = null; // the store; non-reactive on purpose
    this.sel = null;
    this.shown = null; // what the views show: what passes the recipe, and what a hand keeps
    this.order = new Int32Array(0);
    this.posOf = null; // table row -> its place in `order`
    this.layout = null;
    this.tiles = new Map(); // the gallery's nodes: table row -> tile, "h<i>" -> group header
    this.selRow = -1; // the last clicked tile (the focus, the anchor of a shift-click), kept across sorts and modes
    this.picked = new Set(); // the selection: table rows; every action applies to it
    this.subGone = {}; // subset id -> its uids no longer in the pool
    this.pendingSubs = null; // memberships sent before the table arrived
    this.railOpen = true;
    this.railHover = null;
    this.localDraft = this.draft ? JSON.parse(JSON.stringify(this.draft)) : { filters: [], post: {} };
    this.seed = Math.floor(Math.random() * 1e9);
    this.inverted = -1;
    this.drag = null;
    this.lasso = null;
    this.cache = new Map(); // uid -> {url} | {reason}: this tab's cutouts, in the order last shown
    this.queue = [];
    this.pending = new Set();
    this.flights = 0;
    this.wish = { view: [], edge: [], later: [] }; // what the views want cut: on screen, the edge, the overscan
    this.stream = { layout: null, nodes: new Map(), spare: new Map() }; // the by-tomogram cards: frame code -> card
    this.showEmpty = false;
    this.dirty = { grid: true, stream: true }; // a view the selection moved under while it was hidden
    this.menu = null;
    this.rafs = {};
    this.charts = []; // the chart cards shown after the filters: they show, they never filter
    this.pairs = null; // close pairs among the kept picks (the pairs chart)
    this.pairRange = null; // its selected band, Å
    this.pairView = null; // the gallery showing that band's pairs instead of the kept picks
    this.reviewState = this.review ? JSON.parse(JSON.stringify(this.review)) : null; // {sample, labels}
    this.reviewTiles = new Map(); // uid -> tile, in the review's order
    this.reviewFocus = null;
    const grid = this.$refs.grid;
    grid.addEventListener("scroll", () => this.onFrame("grid", () => this.renderWindow()), { passive: true });
    this.$refs.stream.addEventListener("scroll", () => this.onFrame("stream", () => this.renderStream()), { passive: true });
    this.ro = new ResizeObserver(() => this.onFrame("resize", () => this.onResize()));
    this.ro.observe(this.$refs.body);
    grid.addEventListener("click", (e) => this.onTileClick(e));
    this.$refs.root.addEventListener("contextmenu", (e) => this.onTileMenu(e));
    grid.addEventListener("keydown", (e) => this.onKey(e));
    grid.addEventListener("pointerdown", (e) => this.onGridDown(e));
    this.$refs.review.addEventListener("click", (e) => this.onReviewClick(e));
    this.$refs.stream.addEventListener("keydown", (e) => this.actKey(e));
    this.$refs.review.addEventListener("keydown", (e) => this.onReviewKey(e));
    window.addEventListener("pointermove", (this.onWinMove = (e) => this.onGridMove(e)));
    window.addEventListener("pointerup", (this.onWinUp = (e) => this.onGridUp(e)));
    this.applyMode();
    this.renderRail();
    if (this.table_url) this.load(this.table_url);
    else this.status("Opening the pool…");
  },
  unmounted() {
    this.ro && this.ro.disconnect();
    window.removeEventListener("pointermove", this.onWinMove);
    window.removeEventListener("pointerup", this.onWinUp);
    this.closeMenu();
    this.closeLoupe();
    this.endDrag();
    for (const e of this.cache.values()) if (e.url) URL.revokeObjectURL(e.url);
  },
  watch: {
    table_url(url) { if (url) this.load(url); },
    tiles_url() { this.resetCache(); },
    size() {
      if (this.isStream()) { this.dirty.grid = true; this.buildStream(); }
      else { this.dirty.stream = true; this.relayout(); }
    },
    sort() { this.resort(); },
    group() { this.resort(); },
    row_order() { this.dirty.stream = true; if (this.isStream()) this.buildStream(null); },
    metric() { for (const t of this.$refs.root.querySelectorAll(".cbk-tile")) this.paintBadge(t, +t.dataset.row); },
    mode() { this.applyMode(); },
    draft(d) {
      this.localDraft = d ? JSON.parse(JSON.stringify(d)) : { filters: [], post: {} };
      this.buildStrip();
      this.recompute(true);
    },
    frame_choice() { this.recompute(true); },
    source_choice() { this.recompute(true); },
    subsets(v) {
      if (this.s) {
        const ids = new Set((v || []).map((x) => x.id));
        for (const id of Object.keys(this.s.subMasks || {})) if (!ids.has(id)) delete this.s.subMasks[id];
        this.s.subOrder = this.orderedSubsets().map((x) => x.id);
      }
      this.renderRail(true);
      if (this.s && this.sel) { this.refreshTiles(); this.renderSelBar(); }
    },
    groups() {
      if (this.s) this.s.subOrder = this.orderedSubsets().map((x) => x.id);
      this.renderRail(true);
      if (this.s && this.sel) this.refreshTiles();
    },
    dots() { for (const R of this.stream.nodes.values()) if (!R.fold) this.drawStreamRow(R); },
    set_masks() {
      if (!this.s) return;
      this.applySetMasks();
      this.buildStrip();
      this.recompute(true);
    },
    sets() { if (this.s) this.buildStrip(); this.recompute(false); },
    review(v) {
      if (JSON.stringify(v || null) === JSON.stringify(this.reviewState)) return;
      this.reviewState = v ? JSON.parse(JSON.stringify(v)) : null;
      if (this.isReview()) this.buildReview();
    },
  },
  methods: {
    url(u) { return (u.startsWith("/") ? window.path_prefix || "" : "") + u; },
    status(text) { this.$refs.status.textContent = text || ""; },
    isStream() { return this.mode === "tomo"; },
    isReview() { return this.mode === "review"; },
    // `fn` once on the next animation frame, however many times this is asked for before it.
    onFrame(key, fn) {
      if (this.rafs[key]) return;
      this.rafs[key] = requestAnimationFrame(() => { this.rafs[key] = 0; fn(); });
    },
    hasCutout(row) { return this.s.cat.frame[row] >= 0 && this.s.num.x_px[row] === this.s.num.x_px[row]; },

    // ── The table ────────────────────────────────────────────────────────────────────────────
    async load(url) {
      if (!this.s) this.status("Loading the pool table…");
      try {
        const res = await fetch(this.url(url));
        if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
        const next = parsePayload(await res.arrayBuffer());
        const old = this.s;
        // A rebuilt table renumbers its rows: the views keep their place by uid, and the verdicts
        // already shown stay until the server's arrive.
        const keep = old ? { grid: this.gridAnchor(), stream: this.streamAnchor(), sel: this.selRow >= 0 ? uidHex(old, this.selRow) : null, picked: [...this.picked].map((r) => uidHex(old, r)) } : null;
        if (old && old.hex) {
          for (let i = 0; i < old.hex.length; i++) if (old.hex[i] !== undefined && old.verdict[i]) {
            const r = rowOfUid(next, old.hex[i]);
            if (r !== undefined) next.verdict[r] = old.verdict[i];
          }
        }
        // Subset memberships follow their uids to the new rows until the server's arrive.
        next.subMasks = {};
        next.subOrder = this.orderedSubsets().map((x) => x.id);
        if (old && old.subMasks) {
          for (const [id, m] of Object.entries(old.subMasks)) {
            const nm = (next.subMasks[id] = new Uint8Array(next.n));
            for (let i = 0; i < old.n; i++) if (m[i]) { const r = rowOfUid(next, uidHex(old, i)); if (r !== undefined) nm[r] = 1; }
          }
        }
        this.s = next;
        if (this.pendingSubs) { const p = this.pendingSubs; this.pendingSubs = null; this.apply_subsets(p.map, p.full, true); }
        this.applySetMasks();
        this.frameRowsOf = null;
        this.heat = null;
        this.pairs = null;
        this.pairView = null;
        this.nFrames = new Set(next.cat.frame).size;
        for (const t of this.tiles.values()) t.remove();
        this.tiles.clear();
        for (const R of this.stream.nodes.values()) R.node.remove();
        this.stream.nodes.clear();
        this.stream.spare.clear();
        this.layout = null;
        this.stream.layout = null;
        this.selRow = keep && keep.sel ? rowOfUid(next, keep.sel) ?? -1 : -1;
        this.picked = new Set(keep ? keep.picked.map((u) => rowOfUid(next, u)).filter((r) => r !== undefined) : []);
        this.status(next.n ? "" : "No picks in the pool yet: add sources.");
        this.$emit("loaded", { n: next.n, version: next.header.version });
        this.buildStrip();
        this.sel = select(next, this.localDraft, this.frame_choice, this.source_choice);
        this.renderCounts();
        this.renderRail(true);
        this.paintStrip();
        this.shown = this.shownMask();
        this.dirty.grid = this.dirty.stream = true;
        if (this.isStream()) this.buildStream(keep ? keep.stream : null);
        else this.layoutGrid(keep ? keep.grid : null);
        if (this.isReview()) this.buildReview();
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
    // Subsets' uids: every subset's (`full`, the others forgotten), or only those named.
    apply_subsets(map, full, quiet) {
      if (!this.s) { this.pendingSubs = full || !this.pendingSubs ? { map, full } : { map: { ...this.pendingSubs.map, ...map }, full: this.pendingSubs.full }; return; }
      const s = this.s;
      if (full) { s.subMasks = {}; this.subGone = {}; }
      for (const [id, uids] of Object.entries(map || {})) {
        const m = new Uint8Array(s.n);
        let gone = 0;
        for (const u of uids) { const r = rowOfUid(s, u); if (r === undefined) gone++; else m[r] = 1; }
        s.subMasks[id] = m;
        this.subGone[id] = gone;
      }
      s.subOrder = this.orderedSubsets().map((x) => x.id);
      if (quiet) return;
      this.recompute(true);
      this.refreshTiles();
    },

    // ── The recipe ───────────────────────────────────────────────────────────────────────────
    recompute(full) {
      if (!this.s) return;
      this.sel = select(this.s, this.localDraft, this.frame_choice, this.source_choice);
      this.updatePairs();
      this.renderCounts();
      this.paintStrip();
      if (full) this.reorder();
      if (this.isReview()) this.paintReview();
    },
    shownMask() {
      const s = this.s, sel = this.sel, n = s.n, out = new Uint8Array(n);
      if (this.pairView) {
        for (const r of this.pairView.rows) out[r] = 1;
        return out;
      }
      const k = this.inverted;
      if (k >= 0 && sel.masks && sel.masks[k]) {
        // What one filter removes: rows passing every other filter and failing this one.
        const m = sel.masks[k];
        for (let i = 0; i < n; i++) out[i] = sel.base[i] && sel.failing[i] === 1 && !m[i] ? 1 : 0;
        return out;
      }
      // A hand drop stays in view, struck through, so it can be taken back.
      const v = s.verdict;
      for (let i = 0; i < n; i++) out[i] = sel.after[i] || v[i] === KEEP ? 1 : 0;
      return out;
    },
    // The shown rows moved: the view on screen follows now, the other one when it is shown.
    reorder() {
      if (!this.s || !this.sel) return;
      this.shown = this.shownMask();
      if (!this.sel.live) {
        // What the recipe no longer shows leaves the selection.
        let moved = false;
        for (const r of this.picked) if (!this.shown[r]) { this.picked.delete(r); moved = true; }
        if (moved) this.renderSelBar();
      }
      this.dirty.grid = this.dirty.grid === "top" ? "top" : true;
      this.dirty.stream = true;
      // While a filter is dragged the cards stay where they are and only their dots and counts follow;
      // the release lays them out again.
      if (this.isStream()) { if (this.sel.live) this.repaintStream(); else this.buildStream(); }
      else this.layoutGrid();
    },
    // A new sort or grouping of the gallery starts it at the top (when it is next shown, if hidden).
    resort() {
      this.dirty.grid = "top";
      if (!this.isStream() && this.shown) this.layoutGrid(null);
    },
    sigmaCut() {
      const f = (this.localDraft.filters || []).find((x) => x.kind === "range" && x.col === "sigma");
      return f && f.lo != null ? f.lo : null;
    },

    // ── The count line ───────────────────────────────────────────────────────────────────────
    renderCounts() {
      const box = this.$refs.counts;
      box.innerHTML = "";
      const s = this.s, sel = this.sel;
      if (!s || !sel) return;
      const fin = countRows(s, sel.final);
      const projects = (s.cols.project || {}).categories || [];
      let kept = 0, dropped = 0;
      for (let i = 0; i < s.n; i++) {
        if (s.verdict[i] === KEEP && !sel.after[i]) kept++;
        else if (s.verdict[i] === DROP && sel.after[i]) dropped++;
      }
      if (kept || dropped) {
        const h = el("span", "font-size:9px;color:#475569;white-space:nowrap;");
        h.textContent = [kept ? `+${kept} kept by hand` : "", dropped ? `−${dropped} dropped by hand` : ""].filter(Boolean).join(" · ");
        box.appendChild(h);
      }
      const line = el("span", "font-family:'IBM Plex Mono',monospace;font-size:10px;white-space:nowrap;color:#1e293b;");
      line.textContent = `${fin.n.toLocaleString()} of ${s.n.toLocaleString()} · ${fin.nFrames}/${this.nFrames} tomograms · ${fin.perProject.size}/${projects.length} projects`;
      box.appendChild(line);
      const bar = el("span", "display:inline-flex;height:6px;width:120px;border-radius:3px;overflow:hidden;outline:1px solid #cbd5e1;flex:0 0 auto;");
      const total = Math.max(1, fin.n);
      const shades = ["#334155", "#64748b", "#94a3b8", "#cbd5e1", "#475569", "#1e293b", "#7c8ea3", "#a8b5c4"];
      const tip = [];
      [...fin.perProject.entries()].sort((a, b) => b[1] - a[1]).forEach(([p, c], k) => {
        bar.appendChild(el("span", `width:${(100 * c) / total}%;background:${shades[k % shades.length]};`));
        tip.push(`${projects[p]}: ${c.toLocaleString()}`);
      });
      bar.title = tip.join("\n") || "nothing kept";
      box.appendChild(bar);
      this.renderScope();
      this.paintRail();
    },
    // The subset terms of the scope, each with its ✕.
    renderScope() {
      const box = this.$refs.scope, sc = this.localDraft.scope || {};
      box.innerHTML = "";
      const name = (id) => ((this.subsets || []).find((x) => x.id === id) || {}).name || id;
      const chip = (text, drop) => {
        const c = el("span", "padding:1px 6px;border-radius:5px;background:#e8eef6;border:1px solid #c9d6e5;white-space:nowrap;", "cbk-chip");
        c.textContent = `${text}  ✕`;
        c.onclick = () => { drop(); this.release(); };
        box.appendChild(c);
      };
      if ((sc.include || []).length) chip(`in ${sc.include.map(name).join(" ∪ ")}`, () => { sc.include = []; });
      for (const id of sc.exclude || []) chip(`not ${name(id)}`, () => { sc.exclude = sc.exclude.filter((x) => x !== id); });
    },

    // ── Cutouts: this tab's cache and the requests that fill it ──────────────────────────────
    resetCache() {
      for (const e of this.cache.values()) if (e.url) URL.revokeObjectURL(e.url);
      this.cache.clear();
      this.queue = [];
      for (const t of this.$refs.root.querySelectorAll(".cbk-tile")) {
        const img = t.querySelector("img");
        if (img) img.remove();
        if (this.s && this.hasCutout(+t.dataset.row)) this.paintImage(t, t.dataset.uid);
      }
      this.requestTiles();
    },
    cachePut(uid, entry) {
      const old = this.cache.get(uid);
      if (old && old.url) URL.revokeObjectURL(old.url);
      this.cache.delete(uid);
      this.cache.set(uid, entry);
      if (this.cache.size <= TILE_CACHE_MAX) return;
      const live = new Set();
      for (const t of this.$refs.root.querySelectorAll(".cbk-tile")) live.add(t.dataset.uid);
      for (const [u, e] of this.cache) {
        if (this.cache.size <= TILE_CACHE_MAX - 500) break;
        if (live.has(u)) continue;
        if (e.url) URL.revokeObjectURL(e.url);
        this.cache.delete(u);
      }
    },
    // What the views want, on screen first: the queue is replaced, so tiles scrolled past are not cut.
    requestTiles() {
      this.onFrame("tiles", () => {
        const seen = new Set(), list = [];
        for (const part of [this.wish.view, this.wish.edge, this.wish.later]) {
          for (const u of part) {
            if (seen.has(u)) continue;
            seen.add(u);
            if (!this.cache.has(u) && !this.pending.has(u)) list.push(u);
          }
        }
        this.queue = list;
        this.pump();
      });
    },
    pump() {
      if (!this.tiles_url) return;
      while (this.flights < TILE_FLIGHTS && this.queue.length) {
        const batch = this.queue.splice(0, TILE_BATCH).filter((u) => !this.cache.has(u) && !this.pending.has(u));
        if (!batch.length) continue;
        for (const u of batch) this.pending.add(u);
        this.flights++;
        this.fetchTiles(batch).finally(() => {
          this.flights--;
          for (const u of batch) this.pending.delete(u);
          // From the views' wishes again: a tile dropped with an old tile setting is asked for anew.
          this.requestTiles();
        });
      }
    },
    async fetchTiles(uids) {
      const v = this.tiles_url;
      try {
        const res = await fetch(this.url(v), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ uids }) });
        if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
        const reader = res.body.getReader(), tr = new TileReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (v !== this.tiles_url) { reader.cancel(); return; }
          for (const rec of tr.push(value)) this.landTile(rec);
        }
      } catch (e) {
        if (v !== this.tiles_url) return;
        const reason = new TextEncoder().encode(`the cutouts could not be fetched: ${e.message}`);
        for (const u of uids) if (!this.cache.has(u)) this.landTile({ uid: u, kind: "r", data: reason });
      }
    },
    landTile(rec) {
      const entry = rec.kind === "j" ? { url: URL.createObjectURL(new Blob([rec.data], { type: "image/jpeg" })) } : { reason: new TextDecoder().decode(rec.data) };
      this.cachePut(rec.uid, entry);
      for (const t of this.$refs.root.querySelectorAll(`.cbk-tile[data-uid="${rec.uid}"]`)) this.paintImage(t, rec.uid);
    },
    // One tile, wherever it shows (the gallery, a card's cutouts, the edge of the cut): the house
    // `.cb-gallery-tile` of the Journey's galleries, its cutout, its source glyph and its corner number.
    makeTile(row, px, small) {
      const s = this.s, uid = uidHex(s, row);
      const t = el("div", `position:absolute;left:0;top:0;width:${px}px;height:${px}px;`, "cb-gallery-tile cbk-tile");
      t.dataset.row = row;
      t.dataset.uid = uid;
      t._px = px;
      t._x = 0;
      t._y = 0;
      if (!this.hasCutout(row)) {
        t.classList.add("cbk-none");
        t.dataset.reason = "no voxel position: its tomogram has no geometry";
      } else this.paintImage(t, uid);
      if (!small) {
        const src = (s.cols.source.categories || [])[s.cat.source[row]];
        if (GLYPH[src]) {
          const g = el("span", "", "cbk-glyph");
          g.textContent = GLYPH[src];
          t.appendChild(g);
        }
        t._badge = el("span", "", "cb-tile-score");
        t.appendChild(t._badge);
        this.paintBadge(t, row);
      }
      this.dressTile(t, row);
      return t;
    },
    sizeTile(t, px) {
      t.style.width = `${px}px`;
      t.style.height = `${px}px`;
      t._px = px;
    },
    placeTile(t, x, y) {
      if (t._x === x && t._y === y) return;
      t.style.left = `${x}px`;
      t.style.top = `${y}px`;
      t._x = x;
      t._y = y;
    },
    dressTile(t, row) {
      if (t._review) return;
      const v = this.s.verdict[row];
      t.classList.toggle("cb-tile-dropped", v === DROP);
      t.classList.toggle("cbk-keep", v === KEEP);
      t.classList.toggle("selected", this.picked.has(row));
      // One dot per subset holding the pick, in the rail's colours.
      const masks = this.s.subMasks || {}, colors = [];
      for (const x of this.orderedSubsets()) if (masks[x.id] && masks[x.id][row]) colors.push(x.color);
      const key = colors.join(",");
      if ((t._subKey || "") === key) return;
      t._subKey = key;
      if (!t._subs) { t._subs = el("span", "", "cbk-subdots"); t.appendChild(t._subs); }
      t._subs.innerHTML = colors.map((c) => `<i style="background:${c}"></i>`).join("");
    },
    paintImage(t, uid) {
      const c = this.cache.get(uid);
      if (!c) { t.classList.add("cbk-wait"); return; }
      this.cache.delete(uid);
      this.cache.set(uid, c); // the most recently shown last
      t.classList.remove("cbk-wait");
      let img = t.querySelector("img");
      if (c.reason) {
        t.classList.add("cbk-none");
        t.dataset.reason = c.reason;
        if (img) img.remove();
        return;
      }
      t.classList.remove("cbk-none");
      if (!img) {
        img = document.createElement("img");
        img.draggable = false;
        img.setAttribute("data-cb-img", "ok"); // from memory: no loading look
        t.prepend(img);
      }
      if (img.getAttribute("src") !== c.url) img.src = c.url;
    },
    paintBadge(t, row) {
      if (!t._badge) return;
      const col = this.metric && this.metric !== "none" ? this.metric : "";
      const vals = col ? this.s.num[col] : null;
      const text = vals ? metricText(col, (this.s.cols[col] || {}).unit, vals[row]) : "";
      t._badge.textContent = text;
      t._badge.style.display = text ? "" : "none";
    },
    // Verdict and selection marks on every tile shown, in both views; the cards' dots too.
    refreshTiles() {
      for (const t of this.$refs.root.querySelectorAll(".cbk-tile")) this.dressTile(t, +t.dataset.row);
      for (const R of this.stream.nodes.values()) if (!R.fold) this.drawStreamRow(R);
    },
    tileText(row) {
      const s = this.s, fr = s.frames[s.cat.frame[row]] || {};
      const src = (s.cols.source.categories || [])[s.cat.source[row]];
      const v = s.verdict[row] === KEEP ? " · kept by hand" : s.verdict[row] === DROP ? " · dropped by hand" : "";
      const pd = this.pairView ? this.pairView.dist.get(row) : undefined;
      const pair = pd !== undefined ? `\nin a pair ${fmt(pd, 0)} Å apart (its partner beside it)` : "";
      return `${fr.project || "?"} · ${fr.tomo || "?"} · ${src}\nσ ${fmt(s.num.sigma[row], 2)} · LCC ${fmt(s.num.lcc[row], 3)} · depth ${fmt(s.num.depth_a[row], 0)} Å · NN ${fmt(s.num.nn_a[row], 0)} Å${v}${pair}`;
    },
    // Select one tile alone (-1: nothing).
    pick(row) {
      this.selRow = row;
      this.picked = new Set(row >= 0 ? [row] : []);
      this.refreshTiles();
      this.renderSelBar();
    },
    setPicked(rows, add) {
      if (!add) this.picked = new Set();
      for (const r of rows) this.picked.add(r);
      if (rows.length) this.selRow = rows[0];
      this.refreshTiles();
      this.renderSelBar();
    },
    // A click with its keys: alone, ctrl / cmd toggles, shift takes the range from the last click
    // (`range(a, b)`: the rows between two, in the view's order).
    selectClick(row, e, range) {
      if (e.shiftKey && this.selRow >= 0 && range) {
        const rows = range(this.selRow, row);
        if (rows) {
          const anchor = this.selRow;
          this.setPicked(rows, e.ctrlKey || e.metaKey);
          this.selRow = anchor;
          return;
        }
      }
      if (e.ctrlKey || e.metaKey) {
        if (this.picked.has(row)) this.picked.delete(row);
        else this.picked.add(row);
        this.selRow = row;
        this.refreshTiles();
        this.renderSelBar();
        return;
      }
      this.pick(this.picked.size === 1 && this.picked.has(row) ? -1 : row);
    },
    gridRange(a, b) {
      const pa = this.posOf ? this.posOf[a] : -1, pb = this.posOf ? this.posOf[b] : -1;
      return pa < 0 || pb < 0 ? null : Array.from(this.order.subarray(Math.min(pa, pb), Math.max(pa, pb) + 1));
    },
    shownRows() {
      if (!this.isStream()) return Array.from(this.order);
      const out = [];
      for (let i = 0; i < this.s.n; i++) if (this.shown[i]) out.push(i);
      return out;
    },
    // Keys on the selection, in both views: k / x / u the verdicts, 1–9 the subset in that slot of the rail,
    // ctrl-A everything shown, Escape nothing, space the loupe. True when the key was taken.
    actKey(e) {
      if (!this.s) return false;
      if (e.key === "Escape") {
        if (!this.picked.size) return false;
        e.preventDefault();
        e.stopPropagation();
        this.pick(-1);
        return true;
      }
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "a") {
        e.preventDefault();
        this.setPicked(this.shownRows(), false);
        return true;
      }
      const rows = [...this.picked];
      if (!rows.length) return false;
      if (e.key === " ") { e.preventDefault(); if (this.selRow >= 0) this.openLoupe(this.selRow); return true; }
      const v = { k: "keep", x: "drop", u: "clear" }[e.key];
      if (v) { e.preventDefault(); this.setVerdict(rows, v); return true; }
      if (/^[1-9]$/.test(e.key) && !e.ctrlKey && !e.metaKey && !e.altKey) {
        const sub = this.orderedSubsets()[+e.key - 1];
        if (sub) { e.preventDefault(); this.toggleIn(sub.id, rows); }
        return true;
      }
      return false;
    },

    // ── The gallery (pool mode) ──────────────────────────────────────────────────────────────
    tilePx() { return SIZES[this.size || "S"] || SIZES.S; },
    // Sort the shown rows and lay them out. `anchor`: where to keep the view (undefined: where it is,
    // by the uid of its first tile; null: the top).
    layoutGrid(anchor) {
      if (!this.s || !this.shown) return;
      if (this.isReview()) { this.dirty.grid = this.dirty.grid || true; return; } // laid out when shown
      if (anchor === undefined) anchor = this.gridAnchor();
      this.order = this.pairView ? this.pairView.rows : ordered(this.s, this.shown, this.sort || "sigma", this.sigmaCut(), this.seed);
      const group = this.group || "none";
      if (!this.pairView && group !== "none") {
        // Groups together, the sort within each.
        const o = this.order, key = new Float64Array(o.length), idx = new Int32Array(o.length);
        for (let k = 0; k < o.length; k++) { key[k] = groupOf(this.s, group, o[k]); idx[k] = k; }
        idx.sort((a, b) => key[a] - key[b] || a - b);
        this.order = Int32Array.from(idx, (k) => o[k]);
      }
      const pos = (this.posOf = new Int32Array(this.s.n).fill(-1));
      for (let k = 0; k < this.order.length; k++) pos[this.order[k]] = k;
      this.dirty.grid = false;
      this.relayout(anchor);
    },
    relayout(anchor) {
      if (this.isReview()) { this.dirty.grid = this.dirty.grid || true; return; }
      // A tab that is not on screen has no width: it lays out when it shows (the resize observer).
      if (!this.s || !this.posOf || !this.$refs.grid.clientWidth) return;
      if (anchor === undefined) anchor = this.gridAnchor();
      const grid = this.$refs.grid, px = this.tilePx();
      const cols = Math.max(1, Math.floor((grid.clientWidth - 2 * PAD + GAP) / (px + GAP)));
      const rows = []; // {y, h, header, key} | {y, h, start, end}
      const order = this.order, group = this.pairView ? "none" : this.group || "none";
      let y = PAD, i = 0, gi = 0;
      while (i < order.length) {
        let j = order.length;
        if (group !== "none") {
          const g = groupOf(this.s, group, order[i]);
          j = i;
          while (j < order.length && groupOf(this.s, group, order[j]) === g) j++;
          rows.push({ y, h: HEADER_H, header: this.groupLabel(group, order[i], j - i), key: `h${gi++}`, start: i, end: j });
          y += HEADER_H;
        }
        for (let k = i; k < j; k += cols) {
          rows.push({ y, h: px + GAP, start: k, end: Math.min(j, k + cols) });
          y += px + GAP;
        }
        i = j;
      }
      this.layout = { cols, px, rows };
      this.$refs.sizer.style.height = `${y + PAD}px`;
      if (anchor) this.restoreGridAnchor(anchor);
      else if (anchor === null) grid.scrollTop = 0;
      this.renderWindow();
    },
    gridAnchor() {
      const L = this.layout, s = this.s;
      if (!L || !s || !this.order.length) return null;
      const top = this.$refs.grid.scrollTop;
      if (top <= 0) return null;
      for (const r of L.rows) {
        if (r.header || r.y + r.h <= top) continue;
        const row = this.order[r.start];
        return row === undefined ? null : { uid: uidHex(s, row), off: top - r.y };
      }
      return null;
    },
    restoreGridAnchor(a) {
      const row = rowOfUid(this.s, a.uid);
      const k = row === undefined ? -1 : this.posOf[row];
      const r = k >= 0 ? this.layoutRowOfK(k) : null;
      if (r) this.$refs.grid.scrollTop = Math.max(0, r.y + a.off);
    },
    layoutRowOfK(k) {
      for (const r of this.layout.rows) if (!r.header && k >= r.start && k < r.end) return r;
      return null;
    },
    groupLabel(group, row, count) {
      const s = this.s;
      if (group === "tomogram") { const f = s.frames[s.cat.frame[row]] || {}; return `${f.tomo || "?"} · ${f.project || ""} · ${count}`; }
      if (group === "project") return `${s.cols.project.categories[s.cat.project[row]]} · ${count}`;
      if (group === "sigma") { const v = s.num.sigma[row]; return v === v ? `${Math.floor(v)}–${Math.floor(v) + 1} σ · ${count}` : `no σ · ${count}`; }
      if (group === "subset") {
        const k = groupOf(s, group, row), x = this.orderedSubsets()[k];
        return `${x ? x.name : "in no subset"} · ${count}`;
      }
      return "";
    },
    // The tiles in view and a screen of overscan each way: nodes are kept by table row and moved, never
    // rebuilt, so a tile that stays in view keeps its picture through any re-sort.
    renderWindow() {
      const L = this.layout;
      if (!L || !this.s) return;
      const grid = this.$refs.grid, sizer = this.$refs.sizer, s = this.s;
      const top = grid.scrollTop, h = grid.clientHeight || 600;
      const lo = top - h, hi = top + 2 * h;
      const { rows, px } = L;
      const want = new Map(), view = [], later = [];
      let pin = "";
      for (const r of rows) {
        if (r.header && r.y <= top) pin = r.header;
        if (r.y + r.h < lo) continue;
        if (r.y > hi) break;
        if (r.header) { want.set(r.key, r); continue; }
        const onScreen = r.y + r.h >= top && r.y <= top + h;
        for (let k = r.start; k < r.end; k++) {
          const row = this.order[k];
          want.set(row, [PAD + (k - r.start) * (px + GAP), r.y]);
          if (this.hasCutout(row)) (onScreen ? view : later).push(uidHex(s, row));
        }
      }
      const pinBox = this.$refs.pin;
      pinBox.textContent = "";
      if (pin && this.group && this.group !== "none") {
        const p = el("div", "font-size:9px;color:#334155;background:rgba(238,242,247,0.94);padding:3px 8px;", "cbk-ghead");
        p.textContent = pin;
        pinBox.appendChild(p);
      }
      for (const [key, node] of this.tiles) if (!want.has(key)) { node.remove(); this.tiles.delete(key); }
      for (const [key, w] of want) {
        let node = this.tiles.get(key);
        if (typeof key === "string") {
          if (!node) {
            node = el("div", `position:absolute;left:${PAD}px;right:${PAD}px;height:${HEADER_H}px;display:flex;align-items:flex-end;font-size:9px;color:#475569;cursor:pointer;`, "cbk-ghead");
            // A click on a group's header selects the group.
            node.onclick = (e) => { const [a, b] = node._range; this.setPicked(Array.from(this.order.subarray(a, b)), e.ctrlKey || e.metaKey); };
            sizer.appendChild(node);
            this.tiles.set(key, node);
          }
          node._range = [w.start, w.end];
          node.style.top = `${w.y}px`;
          if (node.textContent !== w.header) node.textContent = w.header;
          continue;
        }
        if (!node) {
          node = this.makeTile(key, px);
          sizer.appendChild(node);
          this.tiles.set(key, node);
        } else if (node._px !== px) this.sizeTile(node, px);
        this.placeTile(node, w[0], w[1]);
      }
      this.wish.view = view;
      this.wish.later = later;
      this.requestTiles();
    },
    scrollToK(k) {
      const r = this.layout && this.layoutRowOfK(k);
      if (!r) return;
      const grid = this.$refs.grid;
      if (r.y < grid.scrollTop || r.y + r.h > grid.scrollTop + grid.clientHeight) grid.scrollTop = Math.max(0, r.y - grid.clientHeight / 3);
    },
    onTileClick(e) {
      if (this.suppressClick) { this.suppressClick = false; return; }
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!t) return;
      const row = +t.dataset.row;
      this.$refs.grid.focus({ preventScroll: true });
      if (this.curating) this.clickVerdict(row);
      else this.selectClick(row, e, (a, b) => this.gridRange(a, b));
    },
    onKey(e) {
      if (!this.s || !this.layout || !this.order.length) return;
      const cols = this.layout.cols;
      const moves = { ArrowRight: 1, ArrowLeft: -1, ArrowDown: cols, ArrowUp: -cols };
      if (e.key in moves) {
        e.preventDefault();
        const k = this.selRow >= 0 ? this.posOf[this.selRow] : -1;
        const next = k < 0 ? 0 : Math.min(Math.max(k + moves[e.key], 0), this.order.length - 1);
        this.pick(this.order[next]);
        this.scrollToK(next);
        return;
      }
      this.actKey(e);
    },
    // A drag in the gallery: from a selected tile, the selection goes to a subset of the rail; anywhere else
    // a box selects the tiles it touches (ctrl adds); in paint mode the box keeps or drops them by majority.
    onGridDown(e) {
      if (e.button !== 0) return;
      const grid = this.$refs.grid, b = grid.getBoundingClientRect();
      if (e.clientX - b.left >= grid.clientWidth) return; // the scrollbar
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!this.curating && t && this.picked.has(+t.dataset.row)) {
        this.dragSel = { x0: e.clientX, y0: e.clientY, moved: false, ghost: null, over: null };
        return;
      }
      this.lasso = { x0: e.clientX - b.left, y0: e.clientY - b.top + grid.scrollTop, moved: false, box: null, add: e.ctrlKey || e.metaKey };
    },
    onGridMove(e) {
      if (this.slabBox) { this.moveSlabBox(e); return; }
      if (this.dragSel) { this.moveDragSel(e); return; }
      const l = this.lasso;
      if (!l) return;
      const grid = this.$refs.grid, b = grid.getBoundingClientRect();
      const x = e.clientX - b.left, y = e.clientY - b.top + grid.scrollTop;
      if (!l.moved && Math.hypot(x - l.x0, y - l.y0) < 6) return;
      l.moved = true;
      if (!l.box) {
        l.box = el("div", "position:absolute;border:1px solid #4e79a7;background:rgba(78,121,167,0.12);pointer-events:none;z-index:4;border-radius:3px;");
        this.$refs.sizer.appendChild(l.box);
      }
      Object.assign(l.box.style, { left: `${Math.min(x, l.x0)}px`, top: `${Math.min(y, l.y0)}px`, width: `${Math.abs(x - l.x0)}px`, height: `${Math.abs(y - l.y0)}px` });
      l.x1 = x;
      l.y1 = y;
    },
    onGridUp(e) {
      if (this.slabBox) { this.endSlabBox(e); return; }
      if (this.dragSel) { this.endDragSel(); return; }
      const l = this.lasso;
      this.lasso = null;
      if (!l || !l.moved) return;
      if (l.box) l.box.remove();
      const [x0, x1] = [Math.min(l.x0, l.x1), Math.max(l.x0, l.x1)], [y0, y1] = [Math.min(l.y0, l.y1), Math.max(l.y0, l.y1)];
      const rows = [];
      for (const [key, node] of this.tiles) {
        if (typeof key !== "number") continue;
        const w = node._px;
        if (node._x + w >= x0 && node._x <= x1 && node._y + w >= y0 && node._y <= y1) rows.push(key);
      }
      this.suppressClick = true;
      if (!this.curating) {
        rows.sort((a, b) => this.posOf[a] - this.posOf[b]);
        this.setPicked(rows, l.add);
        return;
      }
      if (!rows.length) return;
      const dropped = rows.filter((r) => this.s.verdict[r] === DROP).length;
      this.setVerdict(rows, dropped * 2 > rows.length ? "clear" : "drop");
    },
    moveDragSel(e) {
      const d = this.dragSel;
      if (!d.moved && Math.hypot(e.clientX - d.x0, e.clientY - d.y0) < 6) return;
      d.moved = true;
      if (!d.ghost) {
        d.ghost = el("div", "position:fixed;z-index:9500;pointer-events:none;padding:2px 8px;border-radius:5px;background:#1e293b;color:#f8fafc;font-size:10px;white-space:nowrap;box-shadow:0 4px 12px rgba(15,23,42,0.3);");
        document.body.appendChild(d.ghost);
      }
      const hit = document.elementFromPoint(e.clientX, e.clientY);
      const item = hit && hit.closest ? hit.closest("[data-sid]") : null;
      if (d.over !== item) {
        if (d.over) d.over.classList.remove("cbk-drop");
        d.over = item;
        if (item) item.classList.add("cbk-drop");
      }
      const sub = item ? (this.subsets || []).find((x) => x.id === item.dataset.sid) : null;
      d.ghost.textContent = `${this.picked.size.toLocaleString()} picks` + (sub ? ` → ${sub.group ? "move to" : "add to"} ${sub.name}` : " → drop on a subset");
      d.ghost.style.left = `${e.clientX + 12}px`;
      d.ghost.style.top = `${e.clientY + 10}px`;
    },
    endDragSel() {
      const d = this.dragSel;
      this.endDrag();
      if (!d || !d.moved) return;
      this.suppressClick = true;
      if (d.over) this.editSubset(d.over.dataset.sid, [...this.picked], "add");
    },
    endDrag() {
      const d = this.dragSel;
      this.dragSel = null;
      if (!d) return;
      if (d.ghost) d.ghost.remove();
      if (d.over) d.over.classList.remove("cbk-drop");
    },

    // ── Verdicts ─────────────────────────────────────────────────────────────────────────────
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
      this.sel = { ...this.sel, final: finalMask(s, this.sel.after, this.sel.base) };
      this.renderCounts();
      this.refreshTiles();
    },
    clickVerdict(row) {
      const v = this.s.verdict[row];
      if (this.sel.after[row]) this.setVerdict([row], v === DROP ? "clear" : "drop");
      else this.setVerdict([row], v === KEEP ? "clear" : "keep");
    },

    // ── Subsets ──────────────────────────────────────────────────────────────────────────────
    // The subsets in the rail's order: those in no group, then each group's. Slots 1–9 and tile dots follow it.
    orderedSubsets() {
      const subs = this.subsets || [], gs = this.groups || [];
      const grouped = new Set(gs.map((g) => g.id));
      return [...subs.filter((x) => !x.group || !grouped.has(x.group)), ...gs.flatMap((g) => subs.filter((x) => x.group === g.id))];
    },
    subById(id) { return (this.subsets || []).find((x) => x.id === id); },
    // Add rows to a subset or take them out; adding to a subset of a group takes them out of its others.
    // The browser shows it at once, the server keeps it.
    editSubset(id, rows, op) {
      const s = this.s, sub = this.subById(id);
      if (!s || !sub || !rows.length) return;
      const m = s.subMasks[id] || (s.subMasks[id] = new Uint8Array(s.n));
      const v = op === "add" ? 1 : 0;
      for (const r of rows) m[r] = v;
      if (op === "add" && sub.group) {
        for (const x of this.subsets) {
          const o = x.group === sub.group && x.id !== id ? s.subMasks[x.id] : null;
          if (o) for (const r of rows) o[r] = 0;
        }
      }
      this.$emit("subset", { op, id, uids: rows.map((r) => uidHex(s, r)) });
      const sc = this.localDraft.scope || {};
      if ((sc.include || []).length || (sc.exclude || []).length) this.recompute(true);
      else {
        this.renderCounts();
        if (this.group === "subset" && !this.isStream()) this.layoutGrid();
      }
      this.refreshTiles();
    },
    toggleIn(id, rows) {
      const m = this.s.subMasks[id];
      this.editSubset(id, rows, m && rows.every((r) => m[r]) ? "remove" : "add");
    },
    newSubset(rows) {
      this.$emit("subset_cmd", { cmd: "new", uids: rows.map((r) => uidHex(this.s, r)) });
    },
    // Menu entries for the subsets: "toggle" (✓ = every row in it; a click takes them out), "add" or "remove"
    // (only the subsets holding some of the rows).
    subsetItems(menu, rows, mode) {
      const s = this.s, masks = s.subMasks || {};
      let group = null;
      for (const x of this.orderedSubsets()) {
        const m = masks[x.id];
        let n = 0;
        if (m) for (const r of rows) n += m[r];
        if (mode === "remove" && !n) continue;
        const g = x.group ? ((this.groups || []).find((y) => y.id === x.group) || {}).name || "" : "";
        if (g !== group) {
          group = g;
          if (g) { const h = el("div", "font-size:9px;font-weight:600;color:#64748b;padding:5px 8px 1px 8px;"); h.textContent = `${g} (adding moves)`; menu.appendChild(h); }
        }
        const all = n === rows.length;
        const label = mode === "remove" ? `${x.name} (${n.toLocaleString()})` : x.name;
        this.menuItem(menu, label, "", mode === "toggle" && all, () => {
          if (mode === "remove") this.editSubset(x.id, rows, "remove");
          else if (mode === "add") this.editSubset(x.id, rows, "add");
          else this.editSubset(x.id, rows, all ? "remove" : "add");
        }, x.color);
      }
    },
    menuItem(menu, label, key, on, act, color) {
      const it = el("div", "display:flex;align-items:center;gap:8px;padding:3px 8px;cursor:pointer;border-radius:4px;", "cbk-mitem");
      const mark = el("span", "width:10px;color:#16a34a;");
      mark.textContent = on ? "✓" : "";
      it.appendChild(mark);
      if (color) it.appendChild(el("span", `width:8px;height:8px;border-radius:50%;background:${color};flex:0 0 auto;`));
      const a = el("span", "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;");
      a.textContent = label;
      it.appendChild(a);
      if (key) {
        const k = el("span", "margin-left:auto;padding-left:16px;color:#94a3b8;font-family:'IBM Plex Mono',monospace;");
        k.textContent = key;
        it.appendChild(k);
      }
      it.onclick = () => { this.closeMenu(); act(); };
      menu.appendChild(it);
      return it;
    },
    menuHead(menu, text) {
      const h = el("div", "font-size:9px;font-weight:600;color:#64748b;padding:6px 8px 2px 8px;");
      h.textContent = text;
      menu.appendChild(h);
    },

    // ── The selection bar ────────────────────────────────────────────────────────────────────
    renderSelBar() {
      const bar = this.$refs.selbar, n = this.picked.size;
      if (!n || !this.s) { bar.style.display = "none"; bar.innerHTML = ""; return; }
      const s = this.s, rows = [...this.picked], frames = new Set(), projects = new Set();
      for (const r of rows) { frames.add(s.cat.frame[r]); projects.add(s.cat.project[r]); }
      bar.style.display = "flex";
      bar.innerHTML = "";
      const what = el("span", "font-family:'IBM Plex Mono',monospace;");
      what.textContent = `${n.toLocaleString()} selected · ${frames.size} tomogram${frames.size === 1 ? "" : "s"} · ${projects.size} project${projects.size === 1 ? "" : "s"}`;
      bar.appendChild(what);
      const btn = (text, act, style) => {
        const b = el("span", `padding:1px 7px;border-radius:5px;border:1px solid #cbd5e1;background:#ffffff;${style || ""}`, "cbk-chip");
        b.textContent = text;
        b.onclick = (e) => act(e, b);
        bar.appendChild(b);
        return b;
      };
      const pop = (b, mode) => {
        if (this.menu) { this.closeMenu(); return; }
        const menu = el("div", `${MENU_STYLE}min-width:200px;`, "cbk-menu");
        this.subsetItems(menu, [...this.picked], mode);
        if (!menu.childNodes.length) this.menuHead(menu, mode === "remove" ? "In no subset" : "No subsets yet: New subset…");
          this.showMenu(menu, b, b.getBoundingClientRect());
      };
      if ((this.subsets || []).length) {
        btn("Add to ▾", (_e, b) => pop(b, "add"));
        btn("Remove from ▾", (_e, b) => pop(b, "remove"));
      }
      btn("New subset…", () => this.newSubset([...this.picked]));
      btn("Keep", () => this.setVerdict([...this.picked], "keep"), "color:#15803d;");
      btn("Drop", () => this.setVerdict([...this.picked], "drop"), "color:#b91c1c;");
      btn("Clear", () => this.setVerdict([...this.picked], "clear"));
      btn("✕", () => this.pick(-1), "border-color:transparent;color:#94a3b8;");
    },

    // ── The subsets rail ─────────────────────────────────────────────────────────────────────
    railShown() { return !this.isReview(); },
    renderRail(rebuild) {
      const rail = this.$refs.rail, shown = this.railShown();
      const w = !shown ? 0 : this.railOpen ? RAIL_W : RAIL_FOLDED;
      rail.style.display = shown ? "flex" : "none";
      rail.style.width = `${w}px`;
      for (const ref of ["grid", "stream"]) this.$refs[ref].style.left = `${w}px`;
      if (this.railW !== undefined && this.railW !== w && this.s && this.shown) { this.railW = w; this.onResize(); }
      this.railW = w;
      const key = JSON.stringify([this.subsets, this.groups, this.railOpen]);
      if (!rebuild && key === this.railKey) { this.paintRail(); return; }
      this.railKey = key;
      rail.innerHTML = "";
      this.railItems = new Map();
      if (!this.railOpen) {
        const open = el("div", "padding:8px 0;text-align:center;color:#64748b;font-size:11px;", "cbk-chip");
        open.textContent = "›";
        open.onclick = () => { this.railOpen = true; this.renderRail(true); };
        rail.appendChild(open);
        return;
      }
      const head = el("div", "display:flex;align-items:center;gap:6px;padding:6px 8px 4px 10px;");
      const title = el("span", "font-weight:600;color:#1e293b;");
      title.textContent = "Subsets";
      const fold = el("span", "margin-left:auto;color:#94a3b8;padding:0 4px;border-radius:3px;", "cbk-chip");
      fold.textContent = "‹";
      fold.onclick = () => { this.railOpen = false; this.renderRail(true); };
      head.append(title, fold);
      const list = el("div", "flex:1 1 0%;min-height:0;overflow-y:auto;padding:0 4px;");
      const item = (id, label, color, slot) => {
        const it = el("div", "display:flex;align-items:center;gap:5px;padding:3px 6px;border-radius:5px;cursor:pointer;border-left:3px solid transparent;", "cbk-ritem");
        if (id) it.dataset.sid = id;
        const dot = el("span", `width:8px;height:8px;border-radius:50%;flex:0 0 auto;background:${color || "transparent"};${color ? "" : "border:1px solid #94a3b8;"}`);
        const sl = el("span", "width:8px;font-size:8px;color:#94a3b8;font-family:'IBM Plex Mono',monospace;");
        sl.textContent = slot || "";
        const name = el("span", "flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;");
        name.textContent = label;
        const cnt = el("span", "font-family:'IBM Plex Mono',monospace;font-size:9px;color:#64748b;white-space:nowrap;");
        it.append(dot, sl, name, cnt);
        it.onclick = (e) => this.onRailClick(id, e);
        if (id) {
          it.oncontextmenu = (e) => this.subsetMenu(id, e);
          it.onmouseenter = () => { this.railHover = id; this.paintRailReadout(); };
          it.onmouseleave = () => { if (this.railHover === id) { this.railHover = null; this.paintRailReadout(); } };
        }
        list.appendChild(it);
        this.railItems.set(id || "", { it, name, cnt, label });
        return it;
      };
      item(null, "All picks", null, "");
      const subs = this.orderedSubsets(), gs = this.groups || [];
      let group;
      subs.forEach((x, k) => {
        const g = x.group && gs.find((y) => y.id === x.group);
        if ((g ? g.id : null) !== group) {
          group = g ? g.id : null;
          const h = el("div", "display:flex;align-items:center;gap:5px;padding:7px 6px 2px 6px;font-size:9px;font-weight:600;color:#64748b;");
          h.textContent = g ? `▣ ${g.name}` : "Subsets (may overlap)";
          if (g) {
            h.style.cursor = "context-menu";
            h.title = "A group: its subsets never share a pick. Right-click to rename or delete it";
            h.oncontextmenu = (e) => this.groupMenu(g.id, e);
          }
          list.appendChild(h);
        }
        item(x.id, x.name, x.color, k < 9 ? `${k + 1}` : "");
      });
      for (const g of gs) {
        if (subs.some((x) => x.group === g.id)) continue;
        const h = el("div", "padding:7px 6px 2px 6px;font-size:9px;font-weight:600;color:#64748b;cursor:context-menu;");
        h.textContent = `▣ ${g.name} (empty: put subsets in it from their menu)`;
        h.oncontextmenu = (e) => this.groupMenu(g.id, e);
        list.appendChild(h);
      }
      const foot = el("div", "display:flex;gap:6px;padding:6px 8px;border-top:1px solid #e2e8f0;");
      const add = (text, tip, act) => {
        const b = el("span", "padding:1px 7px;border-radius:5px;border:1px dashed #b8cce0;background:#ffffff;color:#2c4a6b;", "cbk-chip");
        b.textContent = text;
        b.title = tip;
        b.onclick = act;
        foot.appendChild(b);
      };
      add("+ subset", "A new subset holding the selected picks (none selected: an empty one)", () => this.newSubset([...this.picked]));
      add("+ kept", "A new subset holding every pick kept now: a snapshot, not a saved filter", () => this.$emit("subset_cmd", { cmd: "new_passing" }));
      add("+ group", "A new group: subsets in it never share a pick", () => this.$emit("subset_cmd", { cmd: "new_group" }));
      this.railReadout = el("div", "padding:4px 10px 8px 10px;font-size:9px;line-height:1.45;color:#64748b;min-height:40px;");
      rail.append(head, list, foot, this.railReadout);
      this.paintRail();
    },
    paintRail() {
      if (!this.railItems || !this.s || !this.sel) return;
      const s = this.s, fin = this.sel.final, sc = this.localDraft.scope || {}, inc = new Set(sc.include || []), exc = new Set(sc.exclude || []);
      for (const [id, R] of this.railItems) {
        let total = 0, kept = 0;
        if (!id) { total = s.n; for (let i = 0; i < s.n; i++) kept += fin[i]; }
        else {
          const m = (s.subMasks || {})[id];
          if (m) for (let i = 0; i < s.n; i++) if (m[i]) { total++; kept += fin[i]; }
        }
        R.cnt.textContent = `${kept.toLocaleString()}/${total.toLocaleString()}`;
        const on = id ? inc.has(id) : !inc.size && !exc.size, off = id && exc.has(id);
        const x = id ? this.subById(id) : null;
        R.it.style.background = on ? "#e8eef6" : "";
        R.it.style.borderLeftColor = on ? (x ? x.color : "#2c4a6b") : "transparent";
        R.name.style.fontWeight = on ? "600" : "";
        R.name.style.textDecoration = off ? "line-through" : "";
        R.name.textContent = off ? `not ${R.label}` : R.label;
      }
      this.paintRailReadout();
    },
    paintRailReadout() {
      const box = this.railReadout;
      if (!box || !this.s) return;
      const id = this.railHover, s = this.s;
      if (!id) {
        box.textContent = "kept / in the pool. Click: only it · ctrl-click: or it · alt-click: not it · right-click: more. Drag selected tiles onto a subset to add them.";
        return;
      }
      const m = (s.subMasks || {})[id], x = this.subById(id);
      if (!m || !x) { box.textContent = ""; return; }
      const parts = [];
      for (const y of this.orderedSubsets()) {
        const o = y.id !== id ? s.subMasks[y.id] : null;
        if (!o) continue;
        let both = 0;
        for (let i = 0; i < s.n; i++) both += m[i] & o[i];
        if (both) parts.push(`∩ ${y.name}: ${both.toLocaleString()}`);
      }
      const gone = this.subGone[id] || 0;
      box.textContent = [x.name, ...(parts.length ? parts : ["shares no pick with another subset"]), gone ? `${gone.toLocaleString()} of its picks are no longer in the pool` : ""].filter(Boolean).join(" · ");
    },
    // Click: only this subset; ctrl / cmd: or this one too; alt: not this one. "All picks": no subset limit.
    onRailClick(id, e) {
      const sc = this.localDraft.scope || (this.localDraft.scope = { include: [], exclude: [] });
      sc.include = sc.include || [];
      sc.exclude = sc.exclude || [];
      if (!id) { sc.include = []; sc.exclude = []; }
      else if (e.altKey) {
        sc.include = sc.include.filter((x) => x !== id);
        sc.exclude = sc.exclude.includes(id) ? sc.exclude.filter((x) => x !== id) : [...sc.exclude, id];
      } else if (e.ctrlKey || e.metaKey) {
        sc.exclude = sc.exclude.filter((x) => x !== id);
        sc.include = sc.include.includes(id) ? sc.include.filter((x) => x !== id) : [...sc.include, id];
      } else {
        const only = sc.include.length === 1 && sc.include[0] === id && !sc.exclude.length;
        sc.include = only ? [] : [id];
        sc.exclude = [];
      }
      this.release();
    },
    subsetMenu(id, e) {
      e.preventDefault();
      const x = this.subById(id);
      if (!x || !this.s) return;
      this.closeMenu();
      const menu = el("div", `${MENU_STYLE}min-width:210px;`, "cbk-menu");
      this.menuHead(menu, x.name);
      const m = this.s.subMasks[id];
      this.menuItem(menu, "Select its picks shown", "", false, () => {
        const rows = this.shownRows().filter((r) => m && m[r]);
        this.setPicked(rows, false);
      });
      if (this.picked.size) this.menuItem(menu, `${x.group ? "Move" : "Add"} the ${this.picked.size.toLocaleString()} selected here`, "", false, () => this.editSubset(id, [...this.picked], "add"));
      if (this.picked.size) this.menuItem(menu, "Take the selected out of it", "", false, () => this.editSubset(id, [...this.picked], "remove"));
      this.menuItem(menu, "Rename…", "", false, () => this.$emit("subset_cmd", { cmd: "rename", id }));
      const sw = el("div", "display:flex;gap:4px;padding:4px 8px 4px 26px;");
      for (const c of ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7", "#9c755f"]) {
        const d = el("span", `width:12px;height:12px;border-radius:50%;background:${c};cursor:pointer;${c === x.color ? "box-shadow:0 0 0 2px #0f172a;" : ""}`);
        d.onclick = () => { this.closeMenu(); this.$emit("subset_cmd", { cmd: "color", id, color: c }); };
        sw.appendChild(d);
      }
      menu.appendChild(sw);
      this.menuHead(menu, "Group");
      this.menuItem(menu, "none (may overlap)", "", !x.group, () => this.$emit("subset_cmd", { cmd: "group", id, group: null }));
      for (const g of this.groups || []) this.menuItem(menu, g.name, "", x.group === g.id, () => this.$emit("subset_cmd", { cmd: "group", id, group: g.id }));
      this.menuItem(menu, "New group…", "", false, () => this.$emit("subset_cmd", { cmd: "group", id, group: "new" }));
      this.menuHead(menu, "");
      this.menuItem(menu, "Delete…", "", false, () => this.$emit("subset_cmd", { cmd: "delete", id }));
      this.showMenu(menu, e.currentTarget || this.$refs.rail, { left: e.clientX, top: e.clientY, bottom: e.clientY });
    },
    groupMenu(gid, e) {
      e.preventDefault();
      this.closeMenu();
      const menu = el("div", `${MENU_STYLE}min-width:180px;`, "cbk-menu");
      this.menuItem(menu, "Rename group…", "", false, () => this.$emit("subset_cmd", { cmd: "rename_group", group: gid }));
      this.menuItem(menu, "Delete group (its subsets stay)…", "", false, () => this.$emit("subset_cmd", { cmd: "delete_group", group: gid }));
      this.showMenu(menu, this.$refs.rail, { left: e.clientX, top: e.clientY, bottom: e.clientY });
    },

    // ── The filter strip ─────────────────────────────────────────────────────────────────────
    binsFor(col) {
      const v = this.s.num[col];
      let [lo, hi] = quantiles(v, [0.005, 0.995]);
      if (!(lo === lo)) return null;
      if (col === "sigma") {
        lo = Math.floor(lo * 4) / 4;
        hi = Math.ceil(hi * 4) / 4;
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
    // The cards are rebuilt only when a filter comes or goes; every other change repaints them.
    buildStrip() {
      const box = this.$refs.cards;
      box.innerHTML = "";
      this.strip = { cards: [], charts: [], scales: {} };
      this.edgeKey = null;
      if (!this.s) return;
      (this.localDraft.filters || []).forEach((f, k) => box.appendChild(this.buildCard(f, k)));
      for (const name of this.charts) if (name === "pairs") box.appendChild(this.buildPairsCard());
      const add = el("div", "align-self:center;font-size:10px;color:#2c4a6b;padding:2px 9px;border-radius:5px;border:1px dashed #b8cce0;background:#ffffff;", "cbk-chip");
      add.textContent = "+ filter";
      add.title = "Filter the pool on a column; filters stack: a pick must pass every one";
      add.onclick = () => this.openAddMenu(add);
      box.appendChild(add);
      this.buildPostOps();
    },
    buildCard(f, k) {
      if (f.kind === "tomograms") return this.buildHeatCard(f, k);
      if (f.kind === "set") return this.buildSetCard(f, k);
      const col = this.s.cols[f.col] || { label: f.col, unit: "" };
      const entry = this.filterCard(f, k, col.label + (col.unit && col.unit !== "σ" ? ` (${col.unit})` : ""), `${col.label}: drag across the histogram to set a range, drag a handle to move one end, double-click to clear it`, "min-width:250px;max-width:420px;flex:0 1 330px;");
      const card = entry.card;
      if (f.kind === "range") {
        const sc = this.binsFor(f.col);
        if (sc) {
          this.strip.scales[f.col] = sc;
          entry.canvas = el("canvas", `width:100%;height:${HIST_H}px;display:block;cursor:crosshair;touch-action:none;`);
          entry.scale = sc;
          entry.read = el("div", "display:flex;gap:8px;font-size:9px;color:#64748b;white-space:nowrap;min-width:0;font-family:'IBM Plex Mono',monospace;");
          card.append(entry.canvas, entry.read);
          this.wireHistogram(entry);
        }
        entry.cov = el("div", "font-size:9px;color:#64748b;");
        card.appendChild(entry.cov);
      } else if (f.kind === "category") {
        entry.chips = el("div", "display:flex;flex-wrap:wrap;gap:3px;max-height:68px;overflow-y:auto;");
        card.appendChild(entry.chips);
      }
      this.strip.cards.push(entry);
      return card;
    },
    // A filter's card and its head: name, range, what it keeps of the picks every other filter passes,
    // "removes N" (flips the gallery to what it removes), ✕.
    filterCard(f, k, label, tip, size) {
      const card = el("div", `display:flex;flex-direction:column;gap:4px;padding:6px 10px 7px 10px;background:#ffffff;border:1px solid #dbe3ec;border-radius:7px;box-shadow:0 1px 2px rgba(16,42,67,0.05);${size}`, "cbk-fcard");
      const head = el("div", "display:flex;align-items:center;gap:8px;font-size:10px;color:#475569;white-space:nowrap;min-width:0;");
      const name = el("span", "font-weight:600;color:#1e293b;overflow:hidden;text-overflow:ellipsis;font-size:10.5px;");
      name.textContent = label;
      name.title = tip;
      const range = el("span", "font-family:'IBM Plex Mono',monospace;color:#334155;");
      const keep = el("span", "margin-left:auto;font-family:'IBM Plex Mono',monospace;color:#15803d;");
      keep.title = "Of the picks every other filter passes, how many this one keeps";
      const rem = el("span", "padding:0 5px;border-radius:3px;color:#b45309;font-family:'IBM Plex Mono',monospace;", "cbk-chip");
      rem.title = "Show only the picks this filter removes (they pass every other filter); click again to go back";
      rem.onclick = () => { this.inverted = this.inverted === k ? -1 : k; this.pairView = null; this.reorder(); this.paintStrip(); };
      const x = el("span", "color:#94a3b8;padding:0 3px;border-radius:3px;", "cbk-chip");
      x.textContent = "✕";
      x.title = "Remove this filter";
      x.onclick = () => {
        this.localDraft.filters.splice(k, 1);
        this.inverted = this.inverted === k ? -1 : this.inverted > k ? this.inverted - 1 : this.inverted;
        this.release(true);
      };
      head.append(name, range, keep, rem, x);
      card.appendChild(head);
      return { f, k, card, range, keep, rem };
    },
    // The σ-by-tomogram strip (a tomograms filter): a row per tomogram, the best CTF fit at the top, x = σ;
    // dark cells count the picks every other filter passes, pale ones the rest. A click drops a tomogram or
    // brings it back, a drag does the same to a band of rows, a double-click brings every one back.
    buildHeatCard(f, k) {
      const tip = "σ by tomogram: one row per tomogram, the best CTF fit at the top; dark = picks every other filter passes. Click a row to drop that tomogram or bring it back, drag over rows to do it to a band (the band follows its first row), double-click to bring every tomogram back";
      const entry = this.filterCard(f, k, "σ by tomogram", tip, "flex:1 1 100%;min-width:260px;");
      entry.scroll = el("div", `position:relative;max-height:${HEAT_BOX_H}px;overflow-y:auto;overflow-x:hidden;border-radius:3px;background:#f8fafc;`, "cbk-heat");
      entry.canvas = el("canvas", "display:block;width:100%;height:0;cursor:pointer;touch-action:none;");
      entry.scroll.appendChild(entry.canvas);
      entry.readout = el("div", "display:flex;gap:8px;font-size:9px;color:#64748b;white-space:nowrap;min-width:0;");
      entry.card.append(entry.scroll, entry.readout);
      entry.hover = -1;
      this.wireHeat(entry);
      this.strip.cards.push(entry);
      return entry.card;
    },
    // The server's matches of the sets the draft names, for this table's rows only.
    applySetMasks() {
      const s = this.s;
      s.setMasks = {};
      this.setInfo = {};
      for (const [sid, m] of Object.entries(this.set_masks || {})) {
        if (m.v !== s.header.version) continue;
        s.setMasks[sid] = decodeMask(m.bits, s.n);
        this.setInfo[sid] = m;
      }
    },
    // "In set X" / "not in set X": the set's particles matched to the pool's picks by the server.
    buildSetCard(f, k) {
      const x = (this.sets || []).find((y) => y.id === f.set_id) || {};
      const tip = "A particle set of this project as a filter: its particles are matched to the pool's picks (by the picks a kitchen set names, otherwise by position on the same tomogram); in = only the picks it holds, not in = every other pick";
      const entry = this.filterCard(f, k, x.name || (this.setInfo[f.set_id] || {}).name || f.set_id, tip, "min-width:200px;max-width:320px;flex:0 1 260px;");
      const line = el("div", "display:flex;align-items:center;gap:6px;font-size:9px;color:#64748b;white-space:nowrap;min-width:0;");
      const mode = (value, text) => {
        const c = el("span", "padding:0 3px;border-radius:3px;", "cbk-chip");
        c.textContent = text;
        c.onclick = () => { if (f.mode !== value) { f.mode = value; this.release(); } };
        return c;
      };
      entry.modes = [mode("in", "in the set"), mode("out", "not in it")];
      entry.note = el("span", "overflow:hidden;text-overflow:ellipsis;");
      line.append(entry.modes[0], document.createTextNode("|"), entry.modes[1], entry.note);
      entry.card.appendChild(line);
      this.strip.cards.push(entry);
      return entry.card;
    },
    paintSetCard(e) {
      const f = e.f, info = this.setInfo[f.set_id];
      e.modes.forEach((c, i) => {
        const on = (i === 0) === (f.mode !== "out");
        c.style.fontWeight = on ? "600" : "";
        c.style.color = on ? "#0f172a" : "";
      });
      e.range.textContent = "";
      e.note.style.color = info && info.error ? "#b91c1c" : "";
      if (!info) e.note.textContent = "matching its particles…";
      else if (info.error) e.note.textContent = `could not be read: ${info.error}`;
      else {
        const by = info.how === "uids" ? "named by the kitchen" : "matched by position";
        const lost = info.n_set - info.n_matched;
        e.note.textContent = `${info.n_matched.toLocaleString()} of ${info.n_set.toLocaleString()} ${by}` + (lost > 0 ? ` · ${lost.toLocaleString()} not in this pool` : "");
      }
    },
    heatInfo() {
      if (!this.heat) this.heat = heatRows(this.s);
      return this.heat;
    },
    paintHeat(e, others) {
      const s = this.s, H = this.heatInfo(), R = H.codes.length;
      if (!this.strip.scales.sigma) {
        const sc = this.binsFor("sigma");
        if (sc) this.strip.scales.sigma = sc;
      }
      e.scale = this.strip.scales.sigma || null;
      e.cnt = heatCounts(s, H.rowOf, R, others, e.scale);
      const ex = new Set(e.f.excluded || []);
      e.off = new Uint8Array(R);
      for (let r = 0; r < R; r++) if (ex.has(s.frames[H.codes[r]].key)) e.off[r] = 1;
      let nOff = 0;
      for (const v of e.off) nOff += v;
      e.range.textContent = `${(R - nOff).toLocaleString()} of ${R.toLocaleString()} tomograms`;
      this.drawHeat(e);
      this.heatReadout(e);
    },
    heatRowH() {
      const R = this.heatInfo().codes.length;
      return Math.max(HEAT_ROW[0], Math.min(HEAT_ROW[1], Math.floor(HEAT_BOX_H / Math.max(1, R))));
    },
    drawHeat(e) {
      const cv = e.canvas, w = e.scroll.clientWidth, c = e.cnt;
      if (!w || !c) return;
      const R = c.rowAll.length, rh = this.heatRowH(), h = R * rh, dpr = window.devicePixelRatio || 1;
      cv.style.height = `${h}px`;
      cv.width = Math.round(w * dpr);
      cv.height = Math.round(h * dpr);
      const g = cv.getContext("2d");
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, w, h);
      for (let r = 0; r < R; r++) if (e.off[r]) { g.fillStyle = "rgba(220,38,38,0.10)"; g.fillRect(0, r * rh, w, rh); }
      if (c.bins) {
        let mAll = 1, mSel = 1;
        for (const v of c.all) mAll = Math.max(mAll, v);
        for (const v of c.sel) mSel = Math.max(mSel, v);
        const bw = w / c.bins;
        for (let r = 0; r < R; r++) {
          const fade = e.off[r] ? 0.35 : 1;
          for (let b = 0; b < c.bins; b++) {
            const i = r * c.bins + b, ns = c.sel[i], na = c.all[i];
            if (!na) continue;
            g.fillStyle = ns ? `rgba(30,41,59,${fade * (0.18 + 0.82 * Math.sqrt(ns / mSel))})` : `rgba(148,163,184,${fade * (0.2 + 0.45 * Math.sqrt(na / mAll))})`;
            g.fillRect(b * bw, r * rh, Math.max(1, bw - 0.5), rh - (rh > 3 ? 1 : 0));
          }
        }
        // The σ filter's ends across every row.
        const sf = (this.localDraft.filters || []).find((x) => x.kind === "range" && x.col === "sigma");
        g.fillStyle = "#2c4a6b";
        const sc = e.scale, xOf = (v) => ((v - sc.lo) / (sc.hi - sc.lo)) * w;
        if (sf) for (const v of [sf.lo, sf.hi]) if (v != null) g.fillRect(Math.min(w - 1, Math.max(0, xOf(v) - 0.5)), 0, 1, h);
      } else {
        // No σ in the pool: each row's bar is its share of the largest tomogram's picks.
        let m = 1;
        for (const v of c.rowAll) m = Math.max(m, v);
        for (let r = 0; r < R; r++) {
          g.fillStyle = e.off[r] ? "rgba(148,163,184,0.35)" : "rgba(148,163,184,0.6)";
          g.fillRect(0, r * rh, (w * c.rowAll[r]) / m, rh - 1);
          g.fillStyle = e.off[r] ? "rgba(30,41,59,0.3)" : "#1e293b";
          g.fillRect(0, r * rh, (w * c.rowSel[r]) / m, rh - 1);
        }
      }
      const b = e.brush;
      if (b) {
        const [r0, r1] = [Math.min(b.r0, b.r1), Math.max(b.r0, b.r1)];
        g.fillStyle = "rgba(78,121,167,0.22)";
        g.fillRect(0, r0 * rh, w, (r1 - r0 + 1) * rh);
      }
      if (e.hover >= 0) {
        g.strokeStyle = "#f59e0b";
        g.lineWidth = 1;
        g.strokeRect(0.5, e.hover * rh + 0.5, w - 1, rh - 1);
      }
    },
    heatReadout(e) {
      const box = e.readout, s = this.s, H = this.heatInfo(), c = e.cnt;
      box.textContent = "";
      const span = (text, style) => { const x = el("span", style || ""); x.textContent = text; box.appendChild(x); return x; };
      if (e.hover >= 0 && c) {
        const code = H.codes[e.hover], fr = s.frames[code] || {};
        span(`${fr.tomo || "?"} · ${fr.project || "?"}`, "color:#1e293b;overflow:hidden;text-overflow:ellipsis;");
        span(`CTF ${fmt(H.ctf[code], 1)} Å · ${c.rowSel[e.hover].toLocaleString()} of ${c.rowAll[e.hover].toLocaleString()} picks pass the other filters`);
        span(e.off[e.hover] ? "dropped: click to bring it back" : "click to drop it", `margin-left:auto;${e.off[e.hover] ? "color:#b91c1c;" : ""}`);
        return;
      }
      const sc = e.scale;
      span(sc ? `${fmt(sc.lo, 2)} σ` : "picks per tomogram");
      span("best CTF fit at the top", "margin:0 auto;");
      if (sc) span(`${fmt(sc.hi, 2)} σ`);
    },
    wireHeat(e) {
      const cv = e.canvas;
      const rowAt = (ev) => {
        const b = cv.getBoundingClientRect(), R = this.heatInfo().codes.length;
        return Math.max(0, Math.min(R - 1, Math.floor((ev.clientY - b.top) / this.heatRowH())));
      };
      cv.addEventListener("pointerdown", (ev) => {
        if (ev.button !== 0) return;
        const r = rowAt(ev);
        e.brush = { r0: r, r1: r };
        cv.setPointerCapture(ev.pointerId);
        this.drawHeat(e);
      });
      cv.addEventListener("pointermove", (ev) => {
        const r = rowAt(ev);
        if (e.brush) {
          if (r !== e.brush.r1) { e.brush.r1 = r; this.drawHeat(e); }
        } else if (r !== e.hover) {
          e.hover = r;
          this.drawHeat(e);
          this.heatReadout(e);
        }
      });
      cv.addEventListener("pointerleave", () => {
        if (e.brush || e.hover < 0) return;
        e.hover = -1;
        this.drawHeat(e);
        this.heatReadout(e);
      });
      cv.addEventListener("pointerup", () => {
        const b = e.brush;
        e.brush = null;
        if (!b) return;
        const s = this.s, codes = this.heatInfo().codes, key = (r) => s.frames[codes[r]].key;
        const ex = new Set(e.f.excluded || []);
        const drop = !ex.has(key(b.r0)); // the band follows its first row
        for (let r = Math.min(b.r0, b.r1); r <= Math.max(b.r0, b.r1); r++) {
          if (drop) ex.add(key(r));
          else ex.delete(key(r));
        }
        e.f.excluded = [...ex];
        this.release();
      });
      cv.addEventListener("dblclick", () => { e.f.excluded = []; this.release(); });
    },
    // ── The pairs chart: distances between kept picks of one tomogram, up to 2 × Ø ─────────────────
    pairsRmax() { return this.diameter_a > 0 ? 2 * this.diameter_a : 1000; },
    updatePairs() {
      if (!this.s || !this.sel || !this.charts.includes("pairs")) return;
      this.pairs = closePairs(this.s, this.sel.final, this.pairsRmax());
      if (!this.pairRange) this.pairRange = { lo: 0, hi: this.diameter_a > 0 ? this.diameter_a : this.pairsRmax() / 2 };
      if (this.pairView) this.pairView = { ...this.pairRange, ...pairOrder(this.pairs, this.pairRange.lo, this.pairRange.hi) };
    },
    buildPairsCard() {
      const card = el("div", "display:flex;flex-direction:column;gap:2px;padding:3px 7px 4px 7px;background:#ffffff;border:1px solid #dbe3ec;border-radius:6px;box-shadow:0 1px 2px rgba(16,42,67,0.05);min-width:220px;max-width:360px;flex:0 1 300px;", "cbk-fcard");
      const head = el("div", "display:flex;align-items:center;gap:6px;font-size:9px;color:#475569;white-space:nowrap;min-width:0;");
      const name = el("span", "font-weight:600;color:#1e293b;overflow:hidden;text-overflow:ellipsis;");
      name.textContent = "pair distances";
      name.title = "Distances between the kept picks of each tomogram, up to twice the species' diameter (the line marks Ø, the dashed one Ø/2). Pairs closer than Ø are one particle picked twice or two touching: drag across the histogram to choose a band, then show its pairs side by side in the gallery. A chart: it filters nothing";
      const range = el("span", "font-family:'IBM Plex Mono',monospace;color:#334155;");
      const show = el("span", "margin-left:auto;padding:0 4px;border-radius:3px;color:#475569;", "cbk-chip");
      show.title = "Show the pairs in the band side by side in the gallery, closest first; click again to go back";
      show.onclick = () => this.togglePairView();
      const x = el("span", "color:#94a3b8;padding:0 3px;border-radius:3px;", "cbk-chip");
      x.textContent = "✕";
      x.title = "Close this chart";
      x.onclick = () => {
        this.charts = this.charts.filter((c) => c !== "pairs");
        if (this.pairView) { this.pairView = null; this.reorder(); }
        this.buildStrip();
        this.paintStrip();
      };
      head.append(name, range, show, x);
      const canvas = el("canvas", "width:100%;height:30px;display:block;cursor:crosshair;touch-action:none;");
      const foot = el("div", "font-size:9px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;");
      card.append(head, canvas, foot);
      const entry = { chart: "pairs", card, range, show, canvas, foot };
      this.wirePairs(entry);
      this.strip.charts.push(entry);
      return card;
    },
    paintPairs(e) {
      const p = this.pairs, pr = this.pairRange, cv = e.canvas;
      if (!p || !pr) return;
      const rmax = p.rmax, bins = 60, counts = new Float64Array(bins);
      let inBand = 0;
      for (const d of p.d) {
        counts[Math.min(bins - 1, Math.floor((d / rmax) * bins))]++;
        if (d >= pr.lo && d < pr.hi) inBand++;
      }
      e.range.textContent = `${fmt(pr.lo, 0)}–${fmt(pr.hi, 0)} Å`;
      e.show.textContent = this.pairView ? `showing ${this.pairView.rows.length.toLocaleString()} picks in ${this.pairView.nPairs.toLocaleString()} pairs · back` : `show ${inBand.toLocaleString()} pairs`;
      e.show.style.fontWeight = this.pairView ? "600" : "";
      let kept = 0;
      for (let i = 0; i < this.s.n; i++) kept += this.sel.final[i];
      e.foot.textContent = `${p.d.length.toLocaleString()} pairs closer than ${Math.round(rmax)} Å among the ${kept.toLocaleString()} kept picks` + (this.diameter_a > 0 ? ` · Ø ${Math.round(this.diameter_a)} Å` : " · the species has no diameter: no Ø marked") + (p.full ? "" : ` · only the first ${p.d.length.toLocaleString()} pairs`);
      const w = cv.clientWidth, h = cv.clientHeight, dpr = window.devicePixelRatio || 1;
      if (!w) return;
      cv.width = Math.round(w * dpr);
      cv.height = Math.round(h * dpr);
      const g = cv.getContext("2d");
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, w, h);
      let mx = 1;
      for (const c of counts) mx = Math.max(mx, c);
      const bw = w / bins, scale = (h - 2) / Math.sqrt(mx), xOf = (v) => (v / rmax) * w;
      g.fillStyle = "rgba(78,121,167,0.14)";
      g.fillRect(xOf(pr.lo), 0, xOf(pr.hi) - xOf(pr.lo), h);
      for (let b = 0; b < bins; b++) {
        const hb = Math.sqrt(counts[b]) * scale, mid = ((b + 0.5) / bins) * rmax;
        g.fillStyle = mid >= pr.lo && mid < pr.hi ? "#2c4a6b" : "#94a3b8";
        g.fillRect(b * bw + 0.5, h - hb, bw - 1, hb);
      }
      if (this.diameter_a > 0) {
        g.fillStyle = "#c2410c";
        g.fillRect(xOf(this.diameter_a) - 0.5, 0, 1, h);
        for (let y = 0; y < h; y += 4) g.fillRect(xOf(this.diameter_a / 2) - 0.5, y, 1, 2);
      }
    },
    wirePairs(e) {
      const cv = e.canvas;
      const vOf = (ev) => {
        const b = cv.getBoundingClientRect(), x = Math.min(Math.max(ev.clientX - b.left, 0), cv.clientWidth);
        const rmax = this.pairsRmax(), step = rmax / 60;
        return Math.round(((x / (cv.clientWidth || 1)) * rmax) / step) * step;
      };
      let drag = null;
      cv.addEventListener("pointerdown", (ev) => {
        if (ev.button !== 0) return;
        drag = { v0: vOf(ev) };
        cv.setPointerCapture(ev.pointerId);
      });
      cv.addEventListener("pointermove", (ev) => {
        if (!drag) return;
        const v = vOf(ev);
        if (v === drag.v0) return;
        this.pairRange = { lo: Math.min(drag.v0, v), hi: Math.max(drag.v0, v) };
        this.paintPairs(e);
      });
      cv.addEventListener("pointerup", () => {
        if (!drag) return;
        drag = null;
        if (this.pairView) {
          this.pairView = { ...this.pairRange, ...pairOrder(this.pairs, this.pairRange.lo, this.pairRange.hi) };
          this.reorder();
        }
        this.paintPairs(e);
      });
    },
    togglePairView() {
      if (this.pairView) this.pairView = null;
      else if (this.pairs && this.pairRange) {
        this.inverted = -1;
        this.pairView = { ...this.pairRange, ...pairOrder(this.pairs, this.pairRange.lo, this.pairRange.hi) };
      }
      // Either way the gallery starts at the top: the pairs closest first, or the kept picks again.
      this.shown = this.shownMask();
      this.dirty.stream = true;
      if (this.isStream()) { this.dirty.grid = "top"; this.buildStream(); }
      else this.layoutGrid(null);
      this.paintStrip();
    },
    buildPostOps() {
      const line = this.$refs.post;
      line.innerHTML = "";
      const post = this.localDraft.post || (this.localDraft.post = {});
      const field = (label, key, unit, step) => {
        const wrap = el("label", "display:inline-flex;align-items:center;gap:3px;white-space:nowrap;");
        const t = el("span");
        t.textContent = label;
        const inp = el("input", "width:52px;height:16px;font-size:10px;font-family:'IBM Plex Mono',monospace;border:1px solid #cbd5e1;border-radius:4px;padding:0 4px;background:#fff;color:#0f172a;", "cbk-num");
        inp.type = "number";
        inp.min = "0";
        inp.step = step;
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
      const then = el("span", "color:#64748b;white-space:nowrap;");
      then.textContent = "then, per tomogram:";
      line.append(then, field("no two closer than", "nn_a", "Å", "10"), field("· at most", "top_n", "best", "1"));
    },
    // "+ filter": every column with its coverage, on a solid card that stays until a pick, a click
    // outside it, or Escape.
    openAddMenu(anchor) {
      if (this.menu) { this.closeMenu(); return; }
      const s = this.s;
      const menu = el("div", `${MENU_STYLE}min-width:290px;`, "cbk-menu");
      const used = new Set((this.localDraft.filters || []).map((f) => (f.kind === "tomograms" ? "frame" : f.col)));
      let group = "";
      for (const c of s.header.columns) {
        if (used.has(c.name)) continue;
        if (c.group !== group) {
          group = c.group;
          const g = el("div", "font-size:9px;font-weight:600;color:#64748b;padding:6px 8px 2px 8px;");
          g.textContent = group;
          menu.appendChild(g);
        }
        const item = el("div", "display:flex;gap:12px;padding:3px 8px;cursor:pointer;border-radius:4px;", "cbk-mitem");
        const a = el("span");
        const tomos = c.name === "frame"; // the reconstructions: the σ-by-tomogram strip
        a.textContent = tomos ? "tomograms (σ by tomogram)" : c.label + (c.unit && c.unit !== "σ" ? ` (${c.unit})` : "");
        const b = el("span", "margin-left:auto;color:#64748b;font-family:'IBM Plex Mono',monospace;white-space:nowrap;");
        b.textContent = tomos ? `${(c.categories || []).length.toLocaleString()} tomograms` : c.coverage < s.n ? `${c.coverage.toLocaleString()} of ${s.n.toLocaleString()}` : "all rows";
        item.append(a, b);
        item.onclick = () => {
          this.closeMenu();
          this.localDraft.filters.push(tomos
            ? { kind: "tomograms", excluded: [] }
            : c.kind === "num"
              ? { kind: "range", col: c.name, lo: null, hi: null, keep_missing: c.keep_missing !== false }
              : { kind: "category", col: c.name, allowed: [...(c.categories || [])] });
          this.release(true);
        };
        menu.appendChild(item);
      }
      const sets = (this.sets || []).filter((x) => !(this.localDraft.filters || []).some((f) => f.kind === "set" && f.set_id === x.id));
      if (sets.length) {
        const g = el("div", "font-size:9px;font-weight:600;color:#64748b;padding:6px 8px 2px 8px;");
        g.textContent = "Particle sets of this project (in or not in)";
        menu.appendChild(g);
      }
      for (const x of sets) {
        const item = el("div", "display:flex;gap:12px;padding:3px 8px;cursor:pointer;border-radius:4px;", "cbk-mitem");
        const a = el("span");
        a.textContent = x.name;
        const b = el("span", "margin-left:auto;color:#64748b;font-family:'IBM Plex Mono',monospace;white-space:nowrap;");
        b.textContent = `${(x.n || 0).toLocaleString()} ${x.level === "extracted" ? "particles" : "coordinates"}`;
        item.append(a, b);
        item.onclick = () => {
          this.closeMenu();
          this.localDraft.filters.push({ kind: "set", set_id: x.id, mode: "in" });
          this.release(true);
        };
        menu.appendChild(item);
      }
      if (!this.charts.includes("pairs")) {
        const g = el("div", "font-size:9px;font-weight:600;color:#64748b;padding:6px 8px 2px 8px;");
        g.textContent = "Charts (they show, they never filter)";
        const item = el("div", "display:flex;gap:12px;padding:3px 8px;cursor:pointer;border-radius:4px;", "cbk-mitem");
        const a = el("span");
        a.textContent = "pair distances in each tomogram";
        const b = el("span", "margin-left:auto;color:#64748b;font-family:'IBM Plex Mono',monospace;white-space:nowrap;");
        b.textContent = this.diameter_a > 0 ? `to ${Math.round(2 * this.diameter_a)} Å` : "no diameter";
        item.append(a, b);
        item.onclick = () => {
          this.closeMenu();
          this.charts.push("pairs");
          this.updatePairs();
          this.buildStrip();
          this.paintStrip();
        };
        menu.append(g, item);
      }
      this.showMenu(menu, anchor, anchor.getBoundingClientRect());
    },
    // A tile's right-click menu: what the pick is (in place of a hover tooltip), and what can be done to it, or
    // to the whole selection when the tile is part of it.
    onTileMenu(e) {
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!t || t._review || t.dataset.row === undefined || !this.s) return;
      e.preventDefault();
      const row = +t.dataset.row;
      if (!this.picked.has(row)) this.pick(row);
      this.closeMenu();
      const rows = [...this.picked], many = rows.length > 1, v = this.s.verdict[row];
      const menu = el("div", `${MENU_STYLE}min-width:220px;max-width:380px;`, "cbk-menu");
      const head = el("div", "padding:3px 8px 7px 8px;font-size:9px;line-height:1.5;color:#475569;white-space:pre-line;");
      head.textContent = many ? `${rows.length.toLocaleString()} selected picks` : this.tileText(row) + (t.dataset.reason ? `\n${t.dataset.reason}` : "");
      menu.appendChild(head);
      this.menuItem(menu, "Keep by hand", "k", !many && v === KEEP, () => this.setVerdict(rows, "keep"));
      this.menuItem(menu, "Drop by hand", "x", !many && v === DROP, () => this.setVerdict(rows, "drop"));
      if (many || v) this.menuItem(menu, "Clear the hand verdict", "u", false, () => this.setVerdict(rows, "clear"));
      if ((this.subsets || []).length) {
        this.menuHead(menu, "Subsets (✓ = in it; a click toggles)");
        this.subsetItems(menu, rows, "toggle");
      }
      this.menuItem(menu, many ? "New subset from these…" : "New subset from it…", "", false, () => this.newSubset(rows));
      if (!many) this.menuItem(menu, "Loupe", "space", false, () => this.openLoupe(row));
      this.showMenu(menu, t, { left: e.clientX, top: e.clientY, bottom: e.clientY });
    },
    // A menu on a solid card under (or above) `r`, until a pick, a click outside it, a scroll or Escape.
    showMenu(menu, anchor, r) {
      document.body.appendChild(menu);
      const mh = menu.offsetHeight;
      const below = r.bottom + 4 + mh <= window.innerHeight - 8;
      menu.style.left = `${Math.max(8, Math.min(r.left, window.innerWidth - menu.offsetWidth - 8))}px`;
      menu.style.top = `${below ? r.bottom + 4 : Math.max(8, r.top - 4 - mh)}px`;
      this.menu = menu;
      this.menuOff = (ev) => { if (!menu.contains(ev.target) && !anchor.contains(ev.target)) this.closeMenu(); };
      this.menuKey = (ev) => { if (ev.key === "Escape") this.closeMenu(); };
      this.menuScroll = (ev) => { if (!menu.contains(ev.target)) this.closeMenu(); };
      document.addEventListener("pointerdown", this.menuOff, true);
      document.addEventListener("keydown", this.menuKey, true);
      window.addEventListener("scroll", this.menuScroll, true);
    },
    closeMenu() {
      if (!this.menu) return;
      this.menu.remove();
      this.menu = null;
      document.removeEventListener("pointerdown", this.menuOff, true);
      document.removeEventListener("keydown", this.menuKey, true);
      window.removeEventListener("scroll", this.menuScroll, true);
    },
    wireHistogram(entry) {
      const cv = entry.canvas, f = entry.f, sc = entry.scale;
      const xOf = (v) => ((v - sc.lo) / (sc.hi - sc.lo)) * cv.clientWidth;
      const vOf = (x) => sc.lo + (x / cv.clientWidth) * (sc.hi - sc.lo);
      cv.addEventListener("pointerdown", (e) => {
        const b = cv.getBoundingClientRect(), x = e.clientX - b.left;
        const lx = f.lo == null ? -1e9 : xOf(f.lo), hx = f.hi == null ? 1e9 : xOf(f.hi);
        const which = Math.abs(x - lx) <= 6 ? "lo" : Math.abs(x - hx) <= 6 ? "hi" : "new";
        this.drag = { entry, which, x0: x };
        cv.setPointerCapture(e.pointerId);
      });
      cv.addEventListener("pointermove", (e) => {
        if (!this.drag || this.drag.entry !== entry) {
          // Not dragging: the cursor says what a press would take, the readout what is under it.
          const b = cv.getBoundingClientRect(), x = e.clientX - b.left;
          const near = (v) => v != null && Math.abs(x - xOf(v)) <= 6;
          cv.style.cursor = near(f.lo) || near(f.hi) ? "ew-resize" : "crosshair";
          const bin = Math.max(0, Math.min(sc.bins - 1, Math.floor((x / (cv.clientWidth || 1)) * sc.bins)));
          if (bin !== entry.hoverBin) { entry.hoverBin = bin; this.histReadout(entry); }
          return;
        }
        const b = cv.getBoundingClientRect(), x = Math.min(Math.max(e.clientX - b.left, 0), cv.clientWidth);
        const v = this.snap(f.col, vOf(x));
        if (this.drag.which === "new") {
          if (Math.abs(x - this.drag.x0) < 3) return;
          const v0 = this.snap(f.col, vOf(this.drag.x0));
          f.lo = Math.min(v0, v);
          f.hi = Math.max(v0, v);
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
      cv.addEventListener("pointerleave", () => { if (entry.hoverBin != null) { entry.hoverBin = null; this.histReadout(entry); } });
      cv.addEventListener("dblclick", () => { f.lo = null; f.hi = null; this.release(); });
    },
    // A drag frame: filters only (the post-ops and the server's count wait for the release); the views
    // re-sort at most every 120 ms, keeping their place.
    liveUpdate() {
      this.onFrame("live", () => {
        const ev = evaluate(this.s, this.localDraft, this.frame_choice, this.source_choice);
        this.sel = { ...ev, after: ev.passes, final: finalMask(this.s, ev.passes, ev.base), live: true };
        this.renderCounts();
        const now = performance.now();
        const slow = !this.lastLive || now - this.lastLive > 120;
        this.paintStrip(!slow);
        if (slow) {
          this.lastLive = now;
          this.reorder();
        }
      });
    },
    release(rebuild) {
      if (rebuild) this.buildStrip();
      this.recompute(true);
      clearTimeout(this.draftTimer);
      this.draftTimer = setTimeout(() => {
        let n = 0;
        for (let i = 0; i < this.s.n; i++) n += this.sel.final[i];
        const pending = (this.localDraft.filters || []).some((f) => f.kind === "set" && !(this.s.setMasks || {})[f.set_id]);
        this.$emit("draft", { draft: this.localDraft, n, pending });
      }, 300);
    },
    paintStrip(skipEdge) {
      if (!this.strip || !this.sel || !this.s) return;
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
          e.cov.style.display = c < n ? "" : "none";
          if (c < n) {
            const t = el("span");
            t.textContent = `${(n - c).toLocaleString()} of ${n.toLocaleString()} have no value: `;
            const k = el("span", `padding:0 2px;border-radius:3px;${f.keep_missing !== false ? "font-weight:600;color:#0f172a;" : ""}`, "cbk-chip");
            k.textContent = "keep";
            k.onclick = () => { f.keep_missing = true; this.release(); };
            const d = el("span", `padding:0 2px;border-radius:3px;${f.keep_missing === false ? "font-weight:600;color:#b91c1c;" : ""}`, "cbk-chip");
            d.textContent = "drop";
            d.onclick = () => { f.keep_missing = false; this.release(); };
            e.cov.append(t, k, document.createTextNode(" | "), d);
          }
        } else if (f.kind === "category" && e.chips) {
          this.paintChips(e, others);
        } else if (f.kind === "tomograms") {
          this.paintHeat(e, others);
        } else if (f.kind === "set") {
          this.paintSetCard(e);
        }
        const r = (sel.removes || [])[e.k] || 0;
        let kept = 0;
        for (let i = 0; i < s.n; i++) kept += others[i] && (!m || m[i]) ? 1 : 0;
        e.keep.textContent = `keeps ${kept.toLocaleString()}`;
        e.rem.textContent = this.inverted === e.k ? `showing the ${r.toLocaleString()} it removes · back` : `removes ${r.toLocaleString()}`;
        e.rem.style.fontWeight = this.inverted === e.k ? "600" : "";
      }
      for (const c of this.strip.charts) this.paintPairs(c);
      if (!skipEdge) this.paintEdge();
    },
    paintHistogram(e, others) {
      const cv = e.canvas, sc = e.scale, f = e.f;
      const w = cv.clientWidth, h = cv.clientHeight, dpr = window.devicePixelRatio || 1;
      if (!w) return;
      cv.width = Math.round(w * dpr);
      cv.height = Math.round(h * dpr);
      const g = cv.getContext("2d");
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      g.clearRect(0, 0, w, h);
      const { all, sel } = histogram(this.s.num[f.col], others, sc.bins, sc.lo, sc.hi);
      e.hist = { all, sel };
      let mx = 1;
      for (const c of all) mx = Math.max(mx, c);
      const bw = w / sc.bins, scale = (h - 2) / Math.sqrt(mx);
      for (let b = 0; b < sc.bins; b++) {
        const ha = Math.sqrt(all[b]) * scale, hs = Math.sqrt(sel[b]) * scale;
        g.fillStyle = "#dbe3ec";
        g.fillRect(b * bw + 0.5, h - ha, bw - 1, ha);
        g.fillStyle = "#475569";
        g.fillRect(b * bw + 0.5, h - hs, bw - 1, hs);
      }
      const xOf = (v) => ((v - sc.lo) / (sc.hi - sc.lo)) * w;
      g.fillStyle = "rgba(255,255,255,0.65)";
      if (f.lo != null) g.fillRect(0, 0, Math.max(0, xOf(f.lo)), h);
      if (f.hi != null) g.fillRect(Math.min(w, xOf(f.hi)), 0, w, h);
      // The ends: a line and a grip to take hold of.
      g.fillStyle = "#2c4a6b";
      for (const v of [f.lo, f.hi]) {
        if (v == null) continue;
        const x = Math.min(w - 2, Math.max(0, xOf(v) - 1));
        g.fillRect(x, 0, 2, h);
        g.fillRect(Math.min(w - 8, Math.max(0, x - 3)), 0, 8, 12);
      }
      this.histReadout(e);
    },
    // Under the histogram: its two ends, or the hovered bin's counts.
    histReadout(e) {
      const box = e.read;
      if (!box) return;
      const sc = e.scale, f = e.f, unit = ((this.s.cols[f.col] || {}).unit) || "";
      box.textContent = "";
      const span = (text, style) => { const x = el("span", style || ""); x.textContent = text; box.appendChild(x); };
      const v = (x) => `${fmt(x, Math.abs(sc.hi - sc.lo) < 10 ? 2 : 0)}${unit && unit !== "%" ? ` ${unit}` : unit}`;
      if (e.hoverBin != null && e.hist) {
        const b = e.hoverBin, w = (sc.hi - sc.lo) / sc.bins;
        span(`${v(sc.lo + b * w)} – ${v(sc.lo + (b + 1) * w)}`, "color:#1e293b;");
        span(`${e.hist.all[b].toLocaleString()} picks · ${e.hist.sel[b].toLocaleString()} pass the other filters`, "margin-left:auto;");
        return;
      }
      span(v(sc.lo));
      span("drag across to set a range · a grip moves one end · double-click clears", "margin:0 auto;font-family:'IBM Plex Sans',sans-serif;");
      span(v(sc.hi));
    },
    paintChips(e, others) {
      const s = this.s, f = e.f, codes = s.cat[f.col], names = (s.cols[f.col] || {}).categories || [];
      const tot = new Map(), pass = new Map();
      for (let i = 0; i < s.n; i++) {
        const c = codes[i];
        tot.set(c, (tot.get(c) || 0) + 1);
        if (others[i]) pass.set(c, (pass.get(c) || 0) + 1);
      }
      const allowed = new Set(f.allowed || []);
      e.chips.innerHTML = "";
      const list = [...tot.entries()].sort((a, b) => b[1] - a[1]);
      const shown = e.expanded ? list : list.slice(0, 12);
      for (const [c, n] of shown) {
        const name = names[c] === undefined ? "?" : names[c];
        const on = allowed.has(name);
        const chip = el("span", `font-size:10px;padding:1px 5px;border-radius:4px;outline:1px solid ${on ? "#2c4a6b" : "#cbd5e1"};color:${on ? "#0f172a" : "#94a3b8"};`, "cbk-chip");
        chip.textContent = `${name.length > 28 ? name.slice(0, 12) + "…" + name.slice(-14) : name} ${(pass.get(c) || 0).toLocaleString()}/${n.toLocaleString()}`;
        chip.title = name;
        chip.onclick = () => {
          if (on) allowed.delete(name);
          else allowed.add(name);
          f.allowed = [...allowed];
          this.release();
        };
        e.chips.appendChild(chip);
      }
      if (list.length > 12) {
        const more = el("span", "font-size:9px;color:#2c4a6b;", "cbk-chip");
        more.textContent = e.expanded ? "fewer" : `+${list.length - 12} more`;
        more.onclick = () => { e.expanded = !e.expanded; this.paintChips(e, others); };
        e.chips.appendChild(more);
      }
      const all = el("span", "font-size:9px;color:#2c4a6b;", "cbk-chip");
      all.textContent = allowed.size ? "none" : "all";
      all.onclick = () => { f.allowed = allowed.size ? [] : names.slice(); this.release(); };
      e.chips.appendChild(all);
    },
    // The six cutouts just below the σ filter's low edge and the six just above it, among the rows that
    // pass every other filter; rebuilt only when those twelve change.
    paintEdge() {
      const box = this.$refs.edge;
      const filters = this.localDraft.filters || [];
      const k = filters.findIndex((f) => f.kind === "range" && f.col === "sigma" && f.lo != null);
      if (k < 0 || !this.sel || !this.sel.masks || !this.sel.masks[k]) {
        if (this.edgeKey !== "") {
          box.innerHTML = "";
          this.edgeKey = "";
          this.wish.edge = [];
        }
        return;
      }
      const s = this.s, sel = this.sel, cut = Math.fround(filters[k].lo), sig = s.num.sigma, m = sel.masks[k];
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
        if (v >= cut) keep6(above, i, up);
        else keep6(below, i, down);
      }
      const key = `${cut}|${below.join(",")}|${above.join(",")}`;
      if (key === this.edgeKey) return;
      this.edgeKey = key;
      box.innerHTML = "";
      const label = el("span", "font-size:9px;color:#64748b;margin-right:4px;white-space:nowrap;");
      label.textContent = `edge of the cut at ${fmt(cut, 2)} σ`;
      label.title = "The six cutouts just below the σ cut (faint) and the six just above it, among the picks every other filter passes";
      box.appendChild(label);
      const add = (i, dim) => {
        const t = this.makeTile(i, EDGE_PX, true);
        t.style.position = "relative";
        t.style.flex = "0 0 auto";
        if (dim) t.style.opacity = "0.55";
        t.onclick = () => this.edgeClick(i);
        box.appendChild(t);
      };
      for (const i of below.slice().reverse()) add(i, true);
      box.appendChild(el("span", "width:2px;height:26px;background:#2c4a6b;margin:0 3px;border-radius:1px;"));
      for (const i of above) add(i, false);
      this.wish.edge = [...below, ...above].filter((i) => this.hasCutout(i)).map((i) => uidHex(s, i));
      this.requestTiles();
    },
    edgeClick(row) {
      this.pick(row);
      const k = this.posOf ? this.posOf[row] : -1;
      if (k >= 0 && !this.isStream()) this.scrollToK(k);
    },

    // ── Modes ────────────────────────────────────────────────────────────────────────────────
    // Both views stay laid out, each with its own scroll position; the hidden one is invisible, takes
    // no events, and catches up on a selection that moved while it was hidden only when it is shown.
    applyMode() {
      const tomo = this.isStream(), view = this.isReview() ? "review" : tomo ? "stream" : "grid";
      this.renderRail();
      if (view === "review") this.$refs.selbar.style.display = "none";
      else this.renderSelBar();
      for (const ref of ["grid", "stream", "review"]) {
        const v = this.$refs[ref];
        v.style.visibility = ref === view ? "visible" : "hidden";
        v.style.pointerEvents = ref === view ? "auto" : "none";
      }
      if (!this.s || !this.sel || !this.shown) return;
      if (view === "review") this.buildReview();
      else if (tomo) {
        if (this.dirty.stream) this.buildStream();
        else this.renderStream();
      } else if (this.dirty.grid) this.layoutGrid(this.dirty.grid === "top" ? null : undefined);
      else this.renderWindow();
    },
    onResize() {
      if (!this.s || !this.shown || !this.$refs.body.clientWidth) return;
      this.paintStrip(true); // cards painted while the tab had no size are blank until now
      if (this.isReview()) {
        this.dirty.grid = this.dirty.grid || true;
        this.dirty.stream = true;
      } else if (this.isStream()) {
        this.dirty.grid = true;
        // The cards tile across the width: a new width lays them out again.
        if (!this.stream.layout || this.stream.layout.W !== Math.max(200, this.$refs.stream.clientWidth - 2 * PAD)) this.buildStream();
        else this.renderStream(true);
      } else {
        this.dirty.stream = true;
        this.relayout();
      }
    },

    // ── The loupe: one pick up close (space on a selected tile) ───────────────────────────────
    // Its reconstruction's X/Y, X/Z and Y/Z slabs around it, and its template turned to the pick's matched
    // angles under them, made by the server once per pick (services/sta_kitchen/loupe.py).
    async openLoupe(row) {
      if (!this.loupe_url || !this.loupe_png_url || !this.s) return;
      this.closeLoupe();
      const s = this.s, uid = uidHex(s, row), fr = s.frames[s.cat.frame[row]] || {};
      const card = el("div", "position:absolute;left:50%;top:40px;transform:translateX(-50%);z-index:20;display:flex;flex-direction:column;gap:6px;padding:8px 10px 10px 10px;background:#ffffff;border:1px solid #cbd5e1;border-radius:8px;box-shadow:0 12px 32px rgba(15,23,42,0.22);font-size:10px;color:#334155;", "cbk-loupe");
      const head = el("div", "display:flex;align-items:center;gap:10px;white-space:nowrap;");
      const title = el("span", "font-weight:600;color:#0f172a;overflow:hidden;text-overflow:ellipsis;max-width:560px;");
      title.textContent = `${fr.tomo || "?"} · ${fr.project || "?"} · σ ${fmt(s.num.sigma[row], 2)} · LCC ${fmt(s.num.lcc[row], 3)}`;
      const x = el("span", "margin-left:auto;color:#94a3b8;padding:0 4px;border-radius:3px;", "cbk-chip");
      x.textContent = "✕";
      x.title = "Close (Escape, space)";
      x.onclick = () => this.closeLoupe();
      head.append(title, x);
      const body = el("div", "display:flex;flex-direction:column;gap:3px;min-width:728px;min-height:120px;");
      body.textContent = "cutting the pick's box…";
      card.append(head, body);
      this.$refs.root.appendChild(card);
      this.loupe = card;
      this.loupeOff = (ev) => { if (!card.contains(ev.target)) this.closeLoupe(); };
      this.loupeKey = (ev) => { if (ev.key === "Escape" || ev.key === " ") { ev.preventDefault(); ev.stopPropagation(); this.closeLoupe(); } };
      document.addEventListener("pointerdown", this.loupeOff, true);
      document.addEventListener("keydown", this.loupeKey, true);
      let info;
      try {
        const res = await fetch(this.url(this.loupe_url.replace("{uid}", uid)));
        info = res.ok ? await res.json() : { error: `${res.status} ${await res.text()}` };
      } catch (e) {
        info = { error: e.message };
      }
      if (this.loupe !== card) return;
      body.textContent = "";
      if (info.error) {
        body.textContent = info.error;
        return;
      }
      const cols = el("div", "display:grid;grid-template-columns:repeat(3,240px);gap:4px;font-size:9px;color:#64748b;");
      for (const t of ["X/Y", "X/Z", "Y/Z"]) { const c = el("span"); c.textContent = t; cols.appendChild(c); }
      const img = el("img", "display:block;width:728px;border-radius:4px;background:#e2e8f0;");
      img.draggable = false;
      img.src = this.url(this.loupe_png_url.replace("{name}", info.name));
      const foot = el("div", "font-size:9px;color:#475569;line-height:1.5;");
      const a = info.angles;
      foot.textContent = `above: the reconstruction, ${info.box_a} Å box, ${info.slab_a} Å slabs through the pick · ` + (info.template ? `below: ${info.template_name} turned to the matched angles (rot ${fmt(a[0], 1)}°, tilt ${fmt(a[1], 1)}°, psi ${fmt(a[2], 1)}°), the same box and slabs` : `no template: ${info.why}`);
      body.append(cols, img, foot);
    },
    closeLoupe() {
      if (!this.loupe) return;
      this.loupe.remove();
      this.loupe = null;
      document.removeEventListener("pointerdown", this.loupeOff, true);
      document.removeEventListener("keydown", this.loupeKey, true);
    },

    // ── Review: a blind sample of kept and removed picks, labelled good or bad ─────────────────
    draftKey() {
      return JSON.stringify({ filters: this.localDraft.filters || [], post: this.localDraft.post || {}, choice: this.frame_choice || {} });
    },
    newReview() {
      if (!this.s || !this.sel) return;
      const sample = reviewSample(this.s, this.sel, Math.floor(Math.random() * 2147483647));
      this.reviewState = { sample: { ...sample, draft: this.draftKey(), at: new Date().toISOString() }, labels: {} };
      this.saveReview();
      this.buildReview();
    },
    saveReview() {
      clearTimeout(this.reviewTimer);
      this.reviewTimer = setTimeout(() => this.$emit("review", this.reviewState), 400);
    },
    // The sample's tiles, kept and removed shuffled together, with no number and no hint of which half a
    // tile is from: the labels are the reviewer's alone, and they mark nothing in the pool.
    buildReview() {
      const box = this.$refs.review, st = this.reviewState;
      box.innerHTML = "";
      this.reviewTiles.clear();
      if (!this.s) return;
      const head = el("div", "position:sticky;top:0;z-index:3;display:flex;flex-direction:column;gap:3px;padding:8px 10px 6px 10px;background:rgba(238,242,247,0.96);font-size:10px;color:#334155;");
      const line = el("div", "display:flex;align-items:center;gap:10px;flex-wrap:wrap;");
      const what = el("span");
      what.textContent = st
        ? `${(st.sample.kept.length + st.sample.removed.length).toLocaleString()} picks, kept and removed by the cut in equal parts, shuffled. Click a tile: good, again: bad, again: no label (keys g, b, u).`
        : "Up to 50 picks the cut keeps and 50 it removes, shown blind: labelling them good or bad estimates the cut's precision and what it loses.";
      const btn = el("span", "padding:1px 8px;border-radius:5px;border:1px solid #94a3b8;background:#ffffff;color:#1e293b;white-space:nowrap;", "cbk-chip");
      btn.textContent = st ? "New sample for this cut" : "Draw a sample for this cut";
      btn.title = "A new random sample of the picks this cut keeps and removes; the labels of the old one are dropped";
      btn.onclick = () => this.newReview();
      line.append(what, btn);
      this.reviewLine = el("div", "font-family:'IBM Plex Mono',monospace;font-size:10px;color:#0f172a;");
      this.reviewNote = el("div", "font-size:9px;color:#b45309;");
      head.append(line, this.reviewLine, this.reviewNote);
      box.appendChild(head);
      if (st) {
        const wrap = el("div", `display:flex;flex-wrap:wrap;gap:${GAP}px;padding:${PAD}px;`);
        const px = this.tilePx(), seed = st.sample.seed >>> 0;
        const mix = (u) => { let s = parseInt(u.slice(-8), 16) ^ seed; s = Math.imul(s ^ (s >>> 15), 2246822507); s = Math.imul(s ^ (s >>> 13), 3266489909); return (s ^ (s >>> 16)) >>> 0; };
        const uids = [...st.sample.kept, ...st.sample.removed].sort((a, b) => mix(a) - mix(b) || (a < b ? -1 : 1));
        const want = [];
        for (const uid of uids) {
          const row = rowOfUid(this.s, uid);
          let t;
          if (row === undefined) {
            t = el("div", `position:relative;width:${px}px;height:${px}px;`, "cb-gallery-tile cbk-tile cbk-none");
            t.dataset.uid = uid;
          } else {
            t = this.makeTile(row, px, true);
            t._review = true; // blind: no verdict or selection marks of the pool
            t.classList.remove("cb-tile-dropped", "cbk-keep", "selected");
            t.style.position = "relative";
            if (this.hasCutout(row)) want.push(uid);
          }
          t._mark = el("span", "position:absolute;right:3px;top:1px;font-size:12px;font-weight:700;text-shadow:0 0 2px #000;pointer-events:none;");
          t.appendChild(t._mark);
          wrap.appendChild(t);
          this.reviewTiles.set(uid, t);
        }
        box.appendChild(wrap);
        this.wish.view = want;
        this.wish.later = [];
        this.requestTiles();
      }
      this.paintReview();
    },
    paintReview() {
      const st = this.reviewState;
      if (!this.reviewLine) return;
      if (!st) { this.reviewLine.textContent = ""; this.reviewNote.textContent = ""; return; }
      const labels = st.labels || {};
      for (const [uid, t] of this.reviewTiles) {
        const l = labels[uid];
        t.style.boxShadow = l === "good" ? "0 0 0 2px #16a34a" : l === "bad" ? "0 0 0 2px #dc2626" : "";
        t._mark.textContent = l === "good" ? "✓" : l === "bad" ? "✗" : "";
        t._mark.style.color = l === "good" ? "#4ade80" : "#f87171";
        t.classList.toggle("selected", uid === this.reviewFocus);
      }
      const e = reviewEstimates(st.sample, labels), pct = (v) => (v === v ? `${Math.round(100 * v)}%` : "–");
      const total = st.sample.kept.length + st.sample.removed.length;
      this.reviewLine.textContent = `precision ${pct(e.precision[0])} (${pct(e.precision[1])}–${pct(e.precision[2])}) · good lost at the cut ${pct(e.lost[0])} (${pct(e.lost[1])}–${pct(e.lost[2])}) · ≈ ${e.goodKept === e.goodKept ? Math.round(e.goodKept).toLocaleString() : "–"} good kept, ≈ ${e.goodLost === e.goodLost ? Math.round(e.goodLost).toLocaleString() : "–"} good removed · ${(e.K.n + e.R.n).toLocaleString()} of ${total.toLocaleString()} labelled`;
      this.reviewLine.title = `Of ${st.sample.n_kept.toLocaleString()} picks the cut keeps, ${e.K.n} labelled (${e.K.g} good); of ${st.sample.n_removed.toLocaleString()} it removes, ${e.R.n} labelled (${e.R.g} good). Ranges: 95 % Wilson intervals`;
      const notes = [];
      if (st.sample.draft !== this.draftKey()) notes.push("drawn for an earlier cut: these numbers describe that cut, not this one");
      const gone = [...this.reviewTiles.keys()].filter((u) => rowOfUid(this.s, u) === undefined).length;
      if (gone) notes.push(`${gone} of its picks are no longer in the pool`);
      this.reviewNote.textContent = notes.join(" · ");
    },
    setReviewLabel(uid, label) {
      const st = this.reviewState;
      if (!st || !this.reviewTiles.has(uid)) return;
      st.labels = st.labels || {};
      if (label) st.labels[uid] = label;
      else delete st.labels[uid];
      this.reviewFocus = uid;
      this.saveReview();
      this.paintReview();
    },
    onReviewClick(e) {
      const t = e.target.closest && e.target.closest(".cbk-tile");
      if (!t || !this.reviewState) return;
      this.$refs.review.focus({ preventScroll: true });
      const l = (this.reviewState.labels || {})[t.dataset.uid];
      this.setReviewLabel(t.dataset.uid, !l ? "good" : l === "good" ? "bad" : null);
    },
    onReviewKey(e) {
      const uids = [...this.reviewTiles.keys()];
      if (!uids.length) return;
      const k = Math.max(0, uids.indexOf(this.reviewFocus));
      const first = this.reviewTiles.get(uids[0]), px = this.tilePx();
      const cols = Math.max(1, Math.floor(((first && first.parentNode && first.parentNode.clientWidth) || px) / (px + GAP)));
      const moves = { ArrowRight: 1, ArrowLeft: -1, ArrowDown: cols, ArrowUp: -cols };
      if (e.key in moves) {
        e.preventDefault();
        this.reviewFocus = uids[this.reviewFocus === null ? 0 : Math.min(Math.max(k + moves[e.key], 0), uids.length - 1)];
        this.paintReview();
        return;
      }
      const label = { g: "good", b: "bad", u: null }[e.key];
      if (label === undefined || this.reviewFocus === null) return;
      e.preventDefault();
      this.setReviewLabel(this.reviewFocus, label);
    },

    // ── The by-tomogram stream ───────────────────────────────────────────────────────────────
    frameRows(code) {
      if (!this.frameRowsOf) {
        const by = new Map(), f = this.s.cat.frame;
        for (let i = 0; i < this.s.n; i++) {
          let a = by.get(f[i]);
          if (!a) by.set(f[i], (a = []));
          a.push(i);
        }
        this.frameRowsOf = by;
      }
      return this.frameRowsOf.get(code) || [];
    },
    // One card per tomogram with a shown pick, tiled in rows across the view: a small slab and only as many
    // columns of cutouts as its picks need. `anchor` as for `layoutGrid`, by the card's frame.
    buildStream(anchor) {
      if (!this.s || !this.shown) return;
      if (this.isReview()) { this.dirty.stream = true; return; }
      if (anchor === undefined) anchor = this.streamAnchor();
      const st = streamFrames(this.s, this.shown, this.row_order || "passing");
      const W = Math.max(200, (this.$refs.stream.clientWidth || 1200) - 2 * PAD);
      const cw = CARD_SLAB[this.size || "S"] || CARD_SLAB.S, tpx = Math.min(this.tilePx(), CARD_TILE_MAX);
      const rows = [];
      let x = 0, y = PAD, rowH = 0;
      const brk = () => { if (x > 0) { y += rowH + ROW_GAP; x = 0; rowH = 0; } };
      const add = (e) => {
        const fr = this.s.frames[e.code] || {}, dims = fr.dims || [1, 1, 1];
        const hxy = Math.round((cw * dims[1]) / dims[0]), hxz = Math.max(18, Math.round((cw * dims[2]) / dims[0] / 2));
        const bodyH = hxy + 4 + hxz, n = e.rows.length;
        const perCol = Math.max(1, Math.floor((bodyH + GAP) / (tpx + GAP)));
        const fit = Math.max(1, Math.floor((W - 2 * ROW_PAD - cw - SLAB_GAP + GAP) / (tpx + GAP)));
        const cols = Math.max(1, Math.min(Math.ceil(n / perCol), CARD_COLS_MAX, fit));
        const boxW = cols * (tpx + GAP) - GAP + (n > cols * perCol ? 8 : 0); // room for its scrollbar
        const w = 2 * ROW_PAD + cw + SLAB_GAP + boxW, h = 2 * ROW_PAD + TITLE_H + 4 + bodyH + 2;
        if (x > 0 && x + w > W) brk();
        rows.push({ key: e.code, e, x: PAD + x, y, w, h, hxy, hxz, cw, cols, tpx, boxW });
        x += w + ROW_GAP;
        rowH = Math.max(rowH, h);
      };
      for (const e of st.frames) add(e);
      if (st.empty.length) {
        brk();
        rows.push({ key: "fold", fold: true, x: PAD, y, w: W, h: 24, n: st.empty.length });
        y += 24 + ROW_GAP;
        if (this.showEmpty) for (const e of st.empty) add(e);
      }
      brk();
      const index = new Map(rows.map((r, i) => [r.key, i]));
      this.stream.layout = { rows, index, W };
      this.$refs.streamSizer.style.height = `${y + PAD}px`;
      const sc = this.$refs.stream;
      if (anchor) {
        const i = index.get(anchor.key);
        if (i !== undefined) sc.scrollTop = Math.max(0, rows[i].y + anchor.off);
      } else if (anchor === null) sc.scrollTop = 0;
      this.dirty.stream = false;
      this.renderStream(true);
    },
    // During a filter drag: each card on screen repaints its dots and count, nothing moves.
    repaintStream() {
      for (const R of this.stream.nodes.values()) if (!R.fold && R.r) { this.setRowTitle(R); this.drawStreamRow(R); }
    },
    streamAnchor() {
      const L = this.stream.layout;
      if (!L) return null;
      const top = this.$refs.stream.scrollTop;
      if (top <= 0) return null;
      for (const r of L.rows) if (r.y + r.h > top) return { key: r.key, off: top - r.y };
      return null;
    },
    // The cards in view and a screen each way, kept by frame and moved: a card that stays keeps its
    // slabs and cutouts. `changed`: the layout moved, so every kept card is placed and filled again.
    renderStream(changed) {
      const L = this.stream.layout;
      if (!L || !this.$refs.stream.clientWidth) return;
      const sc = this.$refs.stream, top = sc.scrollTop, h = sc.clientHeight || 600;
      const want = new Map();
      for (const r of L.rows) {
        if (r.y + r.h < top - h) continue;
        if (r.y > top + 2 * h) break;
        want.set(r.key, r);
      }
      // A card that leaves the view is kept whole, slabs loaded, so that coming back shows it at once.
      const spare = this.stream.spare;
      for (const [key, R] of this.stream.nodes) {
        if (want.has(key)) continue;
        R.node.remove();
        this.stream.nodes.delete(key);
        if (R.fold) continue;
        spare.delete(key);
        spare.set(key, R);
        if (spare.size > SPARE_CARDS) spare.delete(spare.keys().next().value);
      }
      for (const [key, r] of want) {
        let R = this.stream.nodes.get(key);
        if (!R) {
          R = r.fold ? null : spare.get(key);
          if (R) {
            spare.delete(key);
            this.$refs.streamSizer.appendChild(R.node);
            this.stream.nodes.set(key, R);
            this.placeStreamRow(R, r);
            continue;
          }
          R = r.fold ? this.makeFold(r) : this.makeStreamRow(r);
          this.$refs.streamSizer.appendChild(R.node);
          this.stream.nodes.set(key, R);
          if (!R.fold) this.fillRow(R);
        } else if (changed || R.r !== r) this.placeStreamRow(R, r);
      }
      this.collectStreamWish();
    },
    collectStreamWish() {
      const sc = this.$refs.stream, top = sc.scrollTop, h = sc.clientHeight || 600;
      const view = [], later = [];
      for (const R of this.stream.nodes.values()) {
        if (R.fold || !R.uids) continue;
        const on = R.r.y + R.r.h >= top && R.r.y <= top + h;
        (on ? view : later).push(...R.uids);
      }
      this.wish.view = view;
      this.wish.later = later;
      this.requestTiles();
    },
    makeFold(r) {
      const node = el("div", `position:absolute;left:${r.x}px;width:${r.w}px;top:${r.y}px;height:${r.h}px;display:flex;align-items:center;padding:0 8px;font-size:10px;color:#475569;border-radius:6px;box-sizing:border-box;`, "cbk-chip");
      node.onclick = () => { this.showEmpty = !this.showEmpty; this.buildStream(); };
      const R = { node, fold: true, r };
      this.placeStreamRow(R, r);
      return R;
    },
    makeStreamRow(r) {
      const node = el("div", `position:absolute;left:${r.x}px;top:${r.y}px;width:${r.w}px;height:${r.h}px;box-sizing:border-box;`, "cbk-srow");
      const title = el("div", `position:absolute;left:${ROW_PAD + 2}px;right:${ROW_PAD}px;top:${ROW_PAD - 1}px;height:${TITLE_H}px;line-height:${TITLE_H}px;font-size:10px;color:#334155;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`, "cbk-ttl");
      const body = el("div", `position:absolute;left:${ROW_PAD}px;right:${ROW_PAD}px;top:${ROW_PAD + TITLE_H + 4}px;bottom:${ROW_PAD}px;`);
      const card = el("div", "position:absolute;left:0;top:0;display:flex;flex-direction:column;gap:4px;");
      const panes = [];
      for (const which of ["xy", "xz"]) {
        const pane = el("div", "", "cbk-pane");
        const img = el("img", "position:absolute;inset:0;width:100%;height:100%;display:block;");
        img.draggable = false;
        const note = el("div", "position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:9px;color:#94a3b8;text-align:center;padding:4px;");
        const cv = el("canvas", "position:absolute;inset:0;width:100%;height:100%;");
        pane.append(img, note, cv);
        card.appendChild(pane);
        panes.push({ which, pane, img, note, cv, w: 1, h: 1, pw: 1, ph: 1 });
      }
      const box = el("div", "position:absolute;top:0;overflow-y:auto;overflow-x:hidden;", "cbk-sbox");
      const inner = el("div", "position:relative;width:100%;");
      box.appendChild(inner);
      body.append(card, box);
      node.append(title, body);
      const R = { node, title, body, card, panes, box, inner, r: null, e: r.e, tiles: new Map(), hover: -1, uids: [] };
      this.placeStreamRow(R, r, true);
      this.loadSlabs(R);
      box.addEventListener("scroll", () => this.onFrame(`box${r.key}`, () => { this.renderBox(R); this.drawStreamRow(R); this.collectStreamWish(); }), { passive: true });
      box.addEventListener("mouseover", (ev) => {
        const t = ev.target.closest && ev.target.closest(".cbk-tile");
        const row = t ? +t.dataset.row : -1;
        if (row !== R.hover) { R.hover = row; this.drawStreamRow(R); }
      });
      box.addEventListener("mouseleave", () => { if (R.hover !== -1) { R.hover = -1; this.drawStreamRow(R); } });
      box.addEventListener("click", (ev) => {
        const t = ev.target.closest && ev.target.closest(".cbk-tile");
        if (!t) return;
        const row = +t.dataset.row;
        this.$refs.stream.focus({ preventScroll: true });
        if (this.curating) this.clickVerdict(row);
        else this.selectClick(row, ev, (a, b) => this.cardRange(R, a, b));
      });
      const xy = panes[0].cv;
      xy.style.cursor = "crosshair";
      xy.addEventListener("mousemove", (ev) => {
        if (this.slabBox) return;
        const i = this.nearestDot(R, ev);
        if (i !== R.hover) { R.hover = i; this.drawStreamRow(R); }
      });
      xy.addEventListener("mouseleave", () => { if (R.hover !== -1) { R.hover = -1; this.drawStreamRow(R); } });
      // A press on the slab: a click picks the nearest dot, a drag selects the dots in its box.
      xy.addEventListener("pointerdown", (ev) => {
        if (ev.button !== 0) return;
        const b = xy.getBoundingClientRect();
        this.slabBox = { R, x0: ev.clientX - b.left, y0: ev.clientY - b.top, x1: 0, y1: 0, moved: false, ev, box: null };
      });
      return R;
    },
    placeStreamRow(R, r, fresh) {
      const prev = R.r;
      R.r = r;
      R.node.style.top = `${r.y}px`;
      R.node.style.left = `${r.x}px`;
      R.node.style.width = `${r.w}px`;
      if (R.fold) {
        R.node.textContent = `${r.n} tomogram${r.n === 1 ? "" : "s"} with nothing passing ${this.showEmpty ? "▾" : "▸"}`;
        return;
      }
      R.e = r.e;
      R.node.style.height = `${r.h}px`;
      R.box.style.width = `${r.boxW}px`;
      if (fresh || !prev || prev.cw !== r.cw || prev.hxy !== r.hxy || prev.hxz !== r.hxz) {
        const [pxy, pxz] = R.panes;
        pxy.pane.style.cssText = `position:relative;width:${r.cw}px;height:${r.hxy}px;`;
        pxz.pane.style.cssText = `position:relative;width:${r.cw}px;height:${r.hxz}px;`;
        pxy.pw = pxz.pw = r.cw;
        pxy.ph = r.hxy;
        pxz.ph = r.hxz;
        R.box.style.left = `${r.cw + SLAB_GAP}px`;
        R.box.style.height = `${r.hxy + 4 + r.hxz}px`;
      }
      this.setRowTitle(R);
      if (!fresh) this.fillRow(R);
    },
    loadSlabs(R) {
      const fr = this.s.frames[R.r.key] || {}, dims = fr.dims || [1, 1, 1];
      for (const p of R.panes) {
        p.w = dims[0];
        p.h = p.which === "xy" ? dims[1] : dims[2];
        if (!fr.recon_exists || !this.slab_url) {
          p.img.remove();
          p.note.textContent = "no reconstruction on disk";
          continue;
        }
        p.img.onload = () => { p.note.textContent = ""; };
        p.img.onerror = () => { p.img.style.visibility = "hidden"; p.note.textContent = "the slab could not be rendered (see the server log)"; };
        p.img.src = this.url(this.slab_url.replace("{frame}", R.r.key).replace("{which}", p.which));
      }
    },
    setRowTitle(R) {
      const s = this.s, e = R.e, fr = s.frames[e.code] || {}, fin = this.sel.final;
      let kept = 0;
      for (const i of e.rows) kept += fin[i];
      const other = (s.header.conflicts || {})[fr.acq] ? " · ⧉ in another frame" : "";
      R.title.textContent = `${kept}/${e.total} · ${fr.tomo} · ${fr.project}${other} · CTF ${fmt(e.ctf, 1)} Å · ${fmt(e.tilts, 0)} tilts`;
      R.title.title = R.title.textContent;
      R.title.classList.toggle("cbk-red", !fr.apix);
    },
    fillRow(R) {
      this.renderBox(R);
      this.drawStreamRow(R);
    },
    // The card's cutouts, ordered by their dot's Y, virtualized in their box.
    renderBox(R) {
      const px = R.r.tpx, cols = R.r.cols;
      const rows = R.e.rows, n = rows.length;
      R.cols = cols;
      R.px = px;
      R.inner.style.height = `${Math.ceil(n / cols) * (px + GAP)}px`;
      const top = R.box.scrollTop, h = R.box.clientHeight || 1;
      const k0 = Math.max(0, Math.floor((top - h) / (px + GAP)) * cols), k1 = Math.min(n, Math.ceil((top + 2 * h) / (px + GAP)) * cols);
      const want = new Map();
      for (let k = k0; k < k1; k++) want.set(rows[k], k);
      for (const [row, t] of R.tiles) if (!want.has(row)) { t.remove(); R.tiles.delete(row); }
      const uids = [];
      for (const [row, k] of want) {
        let t = R.tiles.get(row);
        if (!t) {
          t = this.makeTile(row, px);
          R.inner.appendChild(t);
          R.tiles.set(row, t);
        } else if (t._px !== px) this.sizeTile(t, px);
        this.placeTile(t, (k % cols) * (px + GAP), Math.floor(k / cols) * (px + GAP));
        t._k = k;
        if (this.hasCutout(row)) uids.push(uidHex(this.s, row));
      }
      R.uids = uids;
    },
    cardRange(R, a, b) {
      const rows = R.e.rows, pa = rows.indexOf(a), pb = rows.indexOf(b);
      return pa < 0 || pb < 0 ? null : Array.from(rows.subarray(Math.min(pa, pb), Math.max(pa, pb) + 1));
    },
    moveSlabBox(ev) {
      const B = this.slabBox, cv = B.R.panes[0].cv, b = cv.getBoundingClientRect();
      B.x1 = Math.min(Math.max(ev.clientX - b.left, 0), b.width || B.R.panes[0].pw);
      B.y1 = Math.min(Math.max(ev.clientY - b.top, 0), b.height || B.R.panes[0].ph);
      if (!B.moved && Math.hypot(B.x1 - B.x0, B.y1 - B.y0) < 4) return;
      B.moved = true;
      if (!B.box) {
        B.box = el("div", "position:absolute;border:1px solid #f59e0b;background:rgba(245,158,11,0.14);pointer-events:none;");
        B.R.panes[0].pane.appendChild(B.box);
      }
      Object.assign(B.box.style, { left: `${Math.min(B.x0, B.x1)}px`, top: `${Math.min(B.y0, B.y1)}px`, width: `${Math.abs(B.x1 - B.x0)}px`, height: `${Math.abs(B.y1 - B.y0)}px` });
    },
    endSlabBox(ev) {
      const B = this.slabBox;
      this.slabBox = null;
      if (B.box) B.box.remove();
      const R = B.R, add = ev.ctrlKey || ev.metaKey || B.ev.ctrlKey || B.ev.metaKey;
      this.$refs.stream.focus({ preventScroll: true });
      if (!B.moved) {
        const i = this.nearestDot(R, B.ev);
        if (i < 0) { if (!add) this.pick(-1); return; }
        this.selectClick(i, B.ev, (a, b) => this.cardRange(R, a, b));
        this.scrollBoxTo(R, i);
        return;
      }
      const [x0, x1] = [Math.min(B.x0, B.x1), Math.max(B.x0, B.x1)], [y0, y1] = [Math.min(B.y0, B.y1), Math.max(B.y0, B.y1)];
      const rows = (R.pts || []).filter(([x, y]) => x >= x0 && x <= x1 && y >= y0 && y <= y1).map((p) => p[2]);
      this.setPicked(rows, add);
      if (rows.length) this.scrollBoxTo(R, rows[0]);
    },
    scrollBoxTo(R, row) {
      const k = R.e.rows.indexOf(row);
      if (k < 0 || !R.cols) return;
      const y = Math.floor(k / R.cols) * (R.px + GAP);
      if (y < R.box.scrollTop || y + R.px > R.box.scrollTop + R.box.clientHeight) R.box.scrollTop = Math.max(0, y - R.box.clientHeight / 3);
    },
    nearestDot(R, ev) {
      const b = R.panes[0].cv.getBoundingClientRect(), mx = ev.clientX - b.left, my = ev.clientY - b.top;
      let best = -1, bd = 64;
      for (const [x, y, i] of R.pts || []) {
        const d = (x - mx) ** 2 + (y - my) ** 2;
        if (d < bd) { bd = d; best = i; }
      }
      return best;
    },
    // Dots on the card's slabs as the Dots control says: the mark and its size; kept picks filled (the species
    // colour, or their first subset's); the others hollow, faint or hidden; hand drops a red ×, as the others,
    // or hidden; the selected ringed in amber. The hovered pick's dot and tile are marked together.
    dotStyle() {
      return { shape: "dot", size: "M", others: "hollow", dropped: "cross", color: "species", ...(this.dots || {}) };
    },
    drawStreamRow(R) {
      if (!R.r || !R.cols) return;
      const s = this.s, base = this.species_color || "#3b82f6", dpr = window.devicePixelRatio || 1, fin = this.sel.final;
      const D = this.dotStyle(), rad = DOT_R[D.size] || DOT_R.M, masks = s.subMasks || {}, subs = this.orderedSubsets();
      const colorOf = (i) => {
        if (D.color === "subset") for (const x of subs) if (masks[x.id] && masks[x.id][i]) return x.color;
        return base;
      };
      const rows = this.frameRows(R.r.key);
      const path = (g, x, y, r) => {
        g.beginPath();
        if (D.shape === "square") g.rect(x - r, y - r, 2 * r, 2 * r);
        else if (D.shape === "cross") { g.moveTo(x - r * 1.4, y); g.lineTo(x + r * 1.4, y); g.moveTo(x, y - r * 1.4); g.lineTo(x, y + r * 1.4); }
        else g.arc(x, y, r, 0, 2 * Math.PI);
      };
      const lines = D.shape === "ring" || D.shape === "cross";
      for (const p of R.panes) {
        p.cv.width = Math.round(p.pw * dpr);
        p.cv.height = Math.round(p.ph * dpr);
        const g = p.cv.getContext("2d");
        g.setTransform(dpr, 0, 0, dpr, 0, 0);
        g.clearRect(0, 0, p.pw, p.ph);
        const sx = p.pw / p.w, sy = p.ph / p.h, vcol = p.which === "xy" ? s.num.y_px : s.num.z_px;
        if (p.which === "xy") R.pts = [];
        const dot = (i) => {
          const x = s.num.x_px[i] * sx, y = p.ph - vcol[i] * sy;
          if (!(x === x && y === y)) return;
          let mode = fin[i] ? "kept" : D.others;
          if (!fin[i] && s.verdict[i] === DROP && D.dropped !== "hollow") mode = D.dropped;
          if (mode === "hidden") return;
          const r = i === R.hover ? rad + 2 : rad, c = colorOf(i);
          g.globalAlpha = mode === "faint" ? 0.35 : 1;
          if (mode === "cross") {
            const q = r + 1.5;
            g.beginPath(); g.moveTo(x - q, y - q); g.lineTo(x + q, y + q); g.moveTo(x + q, y - q); g.lineTo(x - q, y + q);
            g.lineWidth = 1.6; g.strokeStyle = "#ef4444"; g.stroke();
          } else if (mode === "hollow") {
            path(g, x, y, r); g.lineWidth = 1; g.strokeStyle = "rgba(226,232,240,0.85)"; g.stroke();
          } else if (lines) {
            path(g, x, y, r); g.lineWidth = 3.2; g.strokeStyle = "rgba(255,255,255,0.9)"; g.stroke();
            path(g, x, y, r); g.lineWidth = 1.8; g.strokeStyle = c; g.stroke();
          } else {
            path(g, x, y, r); g.fillStyle = c; g.fill(); g.lineWidth = 1; g.strokeStyle = "#ffffff"; g.stroke();
          }
          g.globalAlpha = 1;
          if (this.picked.has(i)) { g.beginPath(); g.arc(x, y, r + 4.5, 0, 2 * Math.PI); g.lineWidth = 2; g.strokeStyle = "#f59e0b"; g.stroke(); }
          if (p.which === "xy") R.pts.push([x, y, i]);
        };
        for (const i of rows) if (!fin[i]) dot(i);
        for (const i of rows) if (fin[i]) dot(i);
      }
      for (const [row, t] of R.tiles) t.classList.toggle("cb-tile-highlight", row === R.hover);
    },
  },
};
