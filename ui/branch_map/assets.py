"""The browser half of the branch map (ui/branch_map/view.py), run once per page.

The server sends the cards (HTML strings, ui/branch_map/html.py) into a grid the browser lays out, and once per
change of shape the graph: every node's words and menus, every edge. Here, over that graph:

- the cables, drawn after layout and again whenever a card changes size: a main input from its parent's bottom
  edge into the card's top, a side input dashed into its row's port, the new job's dashed while unsettled. Only
  the new job's and the selected job's lineage show (the stylesheet), on a layer above the cards; a card in a
  folded band has none;
- the lineage of the selected job (everything it was made from and everything made from it, through main and
  side inputs): its cards and cables kept and accented, the rest dimmed; ↑ ↓ walk to what it read and what read
  it, ← → to its siblings, Esc lets it go;
- find, `f` or `/` while the Jobs view has the focus: a palette over the map, the matches listed and lit on the
  map, the rest faded; ↑ ↓ choose, ↵ selects the job;
- choosing what an input reads: the same palette lists the jobs it can read and lights them on the map, each
  with a "use as …" pill; a click on one, or ↵, chooses it;
- the "+" menu (the job types that can read a card) and the number's menu (open, clone, remove), built here from
  the graph, and the magazine's hover and click (which tilt series a mark is, from the pointer);
- fit: a map wider than its view (a large card in a narrow view) shrinks to the view's width, never below 45 %.

What the server must hear goes back as events (`cb-bm-*`, each carrying the map's name)."""

MAP_JS = r"""
(function () {
  if (window.cbBm) return;
  const MAP = 'jobs';
  const M = (window.cbBm = {
    g: null, up: {}, down: {}, mainUp: {}, mainDown: {}, sel: null, selJob: null,
    q: '', hits: [], cur: -1, mode: null, slot: null,
  });
  const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
  const scroller = () => document.querySelector('.cb-bm-scroll');
  const world = () => document.querySelector('.cb-bm-world');
  const q = (s) => (window.CSS && CSS.escape ? CSS.escape(s) : s);
  const attr = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  const nodeEl = (nid) => document.querySelector(`.cb-bm-world .cb-bm-node[data-nid="${q(nid)}"]`);
  const boxOf = (nid) => { const n = nodeEl(nid); return n && (n.querySelector('.cb-bm-card') || n); };
  const emit = (name, args) => emitEvent(name, Object.assign({ map: MAP }, args || {}));
  const shows = () => { const s = scroller(); return !!(s && s.getClientRects().length); };
  const nodeOf = (jid) => (M.g && M.g.alias && M.g.alias[jid]) || jid;
  const info = (nid) => (M.g && M.g.nodes[nid]) || null;
  const numOf = (nid) => { const it = info(nid); return it && it.num ? parseInt(it.num, 10) || 1e9 : 1e9; };
  const NEAR = { block: 'nearest', inline: 'nearest', behavior: 'smooth' };
  const CENTRE = { block: 'center', inline: 'nearest', behavior: 'smooth' };

  M.setGraph = (g) => {
    M.g = g; M.up = {}; M.down = {}; M.mainUp = {}; M.mainDown = {};
    for (const e of g.edges) {
      (M.up[e.b] = M.up[e.b] || []).push(e.a);
      (M.down[e.a] = M.down[e.a] || []).push(e.b);
      if (e.k === 'main') { M.mainUp[e.b] = e.a; (M.mainDown[e.a] = M.mainDown[e.a] || []).push(e.b); }
    }
    if (M.selJob) M.sel = info(nodeOf(M.selJob)) ? nodeOf(M.selJob) : null;
    if (M.mode === 'find') M.search(M.q, false);
    M.later();
  };
  const reach = (start, adj) => {
    const seen = new Set(), todo = [start];
    while (todo.length) {
      const n = todo.pop();
      for (const m of adj[n] || []) if (m !== start && !seen.has(m)) { seen.add(m); todo.push(m); }
    }
    return seen;
  };

  // ── the cables, drawn after the browser has laid the cards out ──
  let raf = 0;
  M.later = () => { if (!raf) raf = requestAnimationFrame(() => { raf = 0; M.draw(); }); };
  const rel = (el, wr, z) => {
    const r = el.getBoundingClientRect();
    return { x: (r.left - wr.left) / z, y: (r.top - wr.top) / z, w: r.width / z, h: r.height / z };
  };
  function fit(W) {
    W.style.zoom = '';
    const S = W.parentElement, need = W.scrollWidth, have = S.clientWidth;
    const z = Math.max(0.45, Math.min(1, have / need));
    if (z < 0.999) W.style.zoom = String(z);
  }
  M.draw = () => {
    const W = world();
    if (!W || !M.g) return;
    const svg = W.querySelector('.cb-bm-cables svg');
    if (!svg) return;
    // The layer is inside the world, so its last size counts in the world's scroll size: measured with it, a map
    // that shrank (every card small again) kept its old height and showed blank paper where the view was. Zero
    // it first; fit and the new size measure the cards alone.
    svg.setAttribute('width', 0);
    svg.setAttribute('height', 0);
    fit(W);
    const z = parseFloat(W.style.zoom) || 1;
    const wr = W.getBoundingClientRect();
    svg.setAttribute('width', Math.ceil(W.scrollWidth));
    svg.setAttribute('height', Math.ceil(W.scrollHeight));
    const out = [];
    for (const e of M.g.edges) {
      const A = boxOf(e.a), B = boxOf(e.b);
      if (!A || !B || !A.getClientRects().length || !B.getClientRects().length) continue;  // in a folded band
      const a = rel(A, wr, z), b = rel(B, wr, z);
      const x0 = a.x + a.w / 2, y0 = a.y + a.h;
      let d;
      if (e.k === 'side') {
        const row = B.querySelector(`.cb-bm-row[data-slot="${q(e.s)}"]`);
        const r = row ? rel(row, wr, z) : { y: b.y, h: 26 };
        const x1 = b.x, y1 = r.y + r.h / 2;
        if (y1 > y0 + 16) {
          const k = Math.max(26, (y1 - y0) * 0.5);
          d = `M ${x0} ${y0} C ${x0} ${y0 + k}, ${x1 - 52} ${y1}, ${x1} ${y1}`;
        } else {
          // The reader sits beside or above: out of the left edge, round the left, into the row's port.
          const xa = a.x, ya = a.y + 13, xl = Math.min(xa, x1) - 34;
          d = `M ${xa} ${ya} C ${xl} ${ya}, ${xl} ${y1}, ${x1} ${y1}`;
        }
      } else if (b.y >= y0 + 8) {
        const x1 = b.x + b.w / 2, y1 = b.y, k = Math.max(14, (y1 - y0) / 2);
        d = `M ${x0} ${y0} C ${x0} ${y0 + k}, ${x1} ${y1 - k}, ${x1} ${y1}`;
      } else {
        // The reader sits beside: out of the right edge, into the left edge of its header.
        const xa = a.x + a.w, ya = a.y + 13, xb = b.x, yb = b.y + 13, k = Math.max(24, Math.abs(xb - xa) / 2);
        d = `M ${xa} ${ya} C ${xa + k} ${ya}, ${xb - k} ${yb}, ${xb} ${yb}`;
      }
      const cls = `k-${e.k}` + (e.n ? ' k-new' : '') + (e.d ? ' k-loose' : '');
      out.push(`<path d="${d}" class="${cls}" data-a="${attr(e.a)}" data-b="${attr(e.b)}"/>`);
    }
    svg.innerHTML = out.join('');
    M.paint();
  };

  // ── selection, lineage, matches, candidates: attributes the stylesheet reads ──
  M.paint = () => {
    const S = scroller();
    if (!S) return;
    const sel = M.sel;
    const up = sel ? reach(sel, M.up) : new Set(), down = sel ? reach(sel, M.down) : new Set();
    S.toggleAttribute('data-has-sel', !!sel);
    const finding = M.mode === 'find' && M.q.trim().length > 0;
    S.toggleAttribute('data-finding', finding);
    S.toggleAttribute('data-slotting', !!M.slot);
    const hits = new Set(M.hits), cur = M.hits[M.cur];
    const cands = new Map((M.slot ? M.slot.cands : []).map((c) => [c.id, c]));
    for (const el of $$('.cb-bm-world .cb-bm-node[data-nid]')) {
      const n = el.dataset.nid;
      const l = n === sel ? 'sel' : up.has(n) ? 'up' : down.has(n) ? 'down' : '';
      if (l) el.dataset.l = l; else delete el.dataset.l;
      if (finding && hits.has(n)) el.dataset.f = n === cur ? 'cur' : 'hit'; else delete el.dataset.f;
      if (M.slot && n === M.slot.nid) { el.dataset.c = 'open'; delete el.dataset.use; }
      else if (cands.has(n)) {
        el.dataset.c = M.slot.current === cands.get(n).src ? 'now' : (n === cur && M.mode === 'slot' ? 'cur' : 'yes');
        el.dataset.use = M.slot.current === cands.get(n).src ? 'reads this now' : `use as ${M.slot.label}`;
      } else { delete el.dataset.c; delete el.dataset.use; }
    }
    for (const r of $$('.cb-bm-world .cb-bm-trow')) {
      if (M.selJob && r.dataset.jid === M.selJob) r.dataset.l = 'sel'; else delete r.dataset.l;
    }
    for (const r of $$('.cb-bm-world .cb-bm-row[data-open]')) r.removeAttribute('data-open');
    if (M.slot) {
      const n = nodeEl(M.slot.nid), row = n && n.querySelector(`.cb-bm-row[data-slot="${q(M.slot.key)}"]`);
      if (row) row.setAttribute('data-open', '');
    }
    for (const p of $$('.cb-bm-cables path')) {
      const a = p.dataset.a, b = p.dataset.b;
      p.classList.toggle('up', !!sel && up.has(a) && (b === sel || up.has(b)));
      p.classList.toggle('down', !!sel && (a === sel || down.has(a)) && down.has(b));
      p.classList.toggle('hit', finding && hits.has(a) && hits.has(b));
    }
    marks();
  };
  // `jid` is a job (one folded into the shared start lights that card); `scroll`: bring its card into view.
  M.select = (jid, scroll) => {
    M.selJob = jid || null;
    const nid = jid ? nodeOf(jid) : null;
    M.sel = nid && info(nid) ? nid : null;
    M.paint();
    if (M.sel && scroll) { const b = boxOf(M.sel); if (b) b.scrollIntoView(NEAR); }
  };
  const pick = (jid) => { M.select(jid, true); emit('cb-bm-select', { nid: jid }); };
  M.walk = (key) => {
    const s = M.sel;
    let t = null;
    if (key === 'ArrowUp') t = M.mainUp[s] || (M.up[s] || [])[0];
    else if (key === 'ArrowDown') t = (M.mainDown[s] || M.down[s] || [])[0];
    else {
      const p = M.mainUp[s];
      const sib = (p ? M.mainDown[p] : null) || [];
      const at = (n) => { const b = boxOf(n); return b ? b.getBoundingClientRect() : { left: 0, top: 0 }; };
      const order = sib.slice().sort((x, y) => at(x).top - at(y).top || at(x).left - at(y).left);
      const i = order.indexOf(s);
      if (i >= 0 && order.length > 1) t = order[(i + (key === 'ArrowRight' ? 1 : -1) + order.length) % order.length];
    }
    if (t && info(t) && !info(t).virtual) pick(info(t).job || t);
  };

  // ── find: each word typed must start a word the job is known by ──
  const subseq = (t, s) => { let i = 0; for (const c of s) if (c === t[i]) i++; return i === t.length; };
  function score(it, toks) {
    let total = 0, weakest = 1000;
    for (const t of toks) {
      let best = 0;
      const n = t.replace(/^(#|job)/, '');
      if (/^\d+$/.test(n) && it.num && parseInt(n, 10) === parseInt(it.num, 10)) best = 100;
      for (const w of it.nw) {
        if (w === t) best = Math.max(best, 95);
        else if (w.startsWith(t)) best = Math.max(best, w === it.first ? 90 : 80);
        else if (t.length >= 3 && w.includes(t)) best = Math.max(best, 35);
      }
      for (const w of it.meta) {
        if (w === t) best = Math.max(best, 70);
        else if (w.startsWith(t)) best = Math.max(best, 60);
        else if (t.length >= 3 && w.includes(t)) best = Math.max(best, 30);
      }
      for (const w of it.src) if (w.startsWith(t)) best = Math.max(best, 40);
      if (!best && t.length >= 3 && subseq(t, it.kind)) best = 25;
      if (!best) return [0, 0];
      total += best;
      weakest = Math.min(weakest, best);
    }
    return [total, weakest];
  }
  // The palette's rows: in find, the matches ranked; choosing a source, the jobs the input can read, narrowed.
  M.search = (text, reveal = true) => {
    M.q = text;
    const toks = text.toLowerCase().trim().split(/\s+/).filter(Boolean);
    const pool = M.mode === 'slot' ? [...new Set(M.slot.cands.map((c) => c.id))]
      : Object.keys((M.g && M.g.nodes) || {});
    let scored = [];
    for (const nid of pool) {
      const it = info(nid);
      if (!it || it.virtual) continue;
      if (!toks.length) { if (M.mode === 'slot') scored.push([0, nid, 0]); continue; }
      const [s, weakest] = score(it, toks);
      if (s > 0) scored.push([s, nid, weakest]);
    }
    // Once something matches by its name, matches only through a set's name or a letter run drop out.
    const top = Math.max(0, ...scored.map((x) => x[2]));
    if (top >= 80) scored = scored.filter((x) => x[2] >= 50);
    scored.sort((x, y) => y[0] - x[0] || numOf(x[1]) - numOf(y[1]));
    M.hits = scored.map((x) => x[1]);
    M.cur = M.hits.length ? 0 : -1;
    if (M.mode === 'slot' && M.slot.current && !toks.length) {
      const i = M.hits.findIndex((n) => (M.slot.cands.find((c) => c.id === n) || {}).src === M.slot.current);
      if (i >= 0) M.cur = i;
    }
    paletteUi();
    M.paint();
    if (reveal && M.hits.length) reveal1(M.hits[M.cur]);
  };
  const reveal1 = (nid) => {
    const b = boxOf(nid), sc = scroller();
    if (!b || !sc) return;
    const r = b.getBoundingClientRect(), v = sc.getBoundingClientRect();
    if (r.bottom < v.top || r.top > v.bottom || r.right < v.left || r.left > v.right) b.scrollIntoView(CENTRE);
  };
  M.step = (d) => {
    if (!M.hits.length) return;
    M.cur = (M.cur + d + M.hits.length) % M.hits.length;
    paletteUi();
    M.paint();
    reveal1(M.hits[M.cur]);
  };
  function marks() {
    if (!(window.CSS && CSS.highlights && window.Highlight)) return;
    CSS.highlights.delete('cb-bm-find');
    const toks = M.q.toLowerCase().trim().split(/\s+/).filter((t) => t && !/^(#|job)?\d+$/.test(t));
    if (!M.mode || !toks.length) return;
    const ranges = [];
    const sel = M.mode === 'find' ? '[data-f] .cb-bm-title, [data-f] .cb-bm-tab' : '[data-c] .cb-bm-title';
    for (const el of $$(`.cb-bm-world ${sel}, .cb-bm-find-item .t`)) {
      const node = Array.from(el.childNodes).find((c) => c.nodeType === 3);
      if (!node) continue;
      const text = node.textContent.toLowerCase();
      for (const t of toks) {
        const re = new RegExp('(^|[^a-z0-9])' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g');
        let m;
        while ((m = re.exec(text))) {
          const s = m.index + m[1].length, r = new Range();
          r.setStart(node, s); r.setEnd(node, Math.min(text.length, s + t.length)); ranges.push(r);
          if (re.lastIndex === m.index) re.lastIndex++;
        }
      }
    }
    if (ranges.length) CSS.highlights.set('cb-bm-find', new Highlight(...ranges));
  }

  // ── the palette: frosted glass over the map ──
  function box() {
    let b = document.querySelector('.cb-bm-find');
    if (b) return b;
    b = document.createElement('div');
    b.className = 'cb-bm-find';
    b.innerHTML = '<div class="cb-bm-find-title"></div><div class="cb-bm-find-bar"><i class="material-icons">search</i>'
      + '<input type="text" spellcheck="false" autocomplete="off">'
      + '<span class="cb-bm-find-count"></span><i class="material-icons cb-bm-find-x" title="Close (esc)">close</i>'
      + '</div><div class="cb-bm-find-list"></div><div class="cb-bm-find-hint"></div>';
    document.body.appendChild(b);
    const inp = b.querySelector('input');
    inp.addEventListener('input', () => M.search(inp.value));
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); M.close(); return; }
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault(); M.step(e.key === 'ArrowDown' ? 1 : -1); return;
      }
      if (e.key === 'Enter') { e.preventDefault(); take(M.hits[M.cur]); }
    });
    b.querySelector('.cb-bm-find-x').addEventListener('click', () => M.close());
    b.querySelector('.cb-bm-find-list').addEventListener('click', (e) => {
      const it = e.target.closest('.cb-bm-find-item');
      if (it) take(it.dataset.nid);
    });
    window.addEventListener('resize', () => { if (M.mode) place(b); });
    return b;
  }
  function place(b) {
    const sc = scroller();
    if (!sc) return;
    const r = sc.getBoundingClientRect();
    const w = Math.max(280, Math.min(560, r.width - 48));
    b.style.width = w + 'px';
    b.style.left = (r.left + (r.width - w) / 2) + 'px';
    b.style.top = (r.top + Math.max(18, r.height * 0.12)) + 'px';
  }
  function paletteUi() {
    const b = document.querySelector('.cb-bm-find');
    if (!b || !M.mode) return;
    const n = M.hits.length;
    b.dataset.mode = M.mode;
    b.querySelector('.cb-bm-find-title').textContent = M.mode === 'slot'
      ? `${M.slot.label} of ${M.slot.job}: what it can read` : '';
    b.querySelector('.cb-bm-find-count').textContent = M.mode === 'slot'
      ? `${n} of ${M.slot.cands.length}` : (!M.q.trim() ? '' : n ? `${n} found` : 'none');
    b.querySelector('.cb-bm-find-hint').textContent = M.mode === 'slot'
      ? (M.slot.cands.length ? '↑ ↓ choose · ↵ or a click: read it · esc leaves it as it is'
        : 'no job in the project makes what this input reads yet · esc')
      : '↑ ↓ choose · ↵ select the job and show its lineage · esc';
    const list = b.querySelector('.cb-bm-find-list');
    list.innerHTML = M.hits.slice(0, 60).map((nid, i) => {
      const c = M.mode === 'slot' ? M.slot.cands.find((x) => x.id === nid) : null;
      const it = c || info(nid);
      const words = c ? c.t : [it.sp, it.chips].filter(Boolean).join(' · ');
      const now = c && c.src === M.slot.current ? '<span class="now">reads this now</span>' : '';
      return `<div class="cb-bm-find-item${i === M.cur ? ' on' : ''}" data-nid="${attr(nid)}">`
        + `<span class="cb-bm-light ${attr(it.status)}"></span><span class="t">${attr(it.name)}</span>`
        + `<span class="n">${attr(it.num || '')}</span>${now}<span class="c">${attr(words)}</span></div>`;
    }).join('');
    const on = list.querySelector('.on');
    if (on) on.scrollIntoView({ block: 'nearest' });
  }
  function openBox(placeholder, text) {
    const b = box();
    b.style.display = 'block';
    place(b);
    const inp = b.querySelector('input');
    inp.placeholder = placeholder;
    inp.value = text;
    inp.focus();
    inp.select();
  }
  function take(nid) {
    if (!nid) return;
    if (M.mode === 'slot') {
      const c = M.slot.cands.find((x) => x.id === nid);
      if (!c) return;
      const s = M.slot;
      hide();
      M.slot = null;
      M.paint();
      emit('cb-bm-choose', { nid: s.nid, key: s.key, src: c.src });
      return;
    }
    const it = info(nid);
    M.close();
    if (it) pick(it.job || nid);
  }
  function hide() {
    const b = document.querySelector('.cb-bm-find');
    if (b) b.style.display = 'none';
    M.mode = null; M.q = ''; M.hits = []; M.cur = -1;
  }
  M.openFind = () => {
    if (M.slot) return;
    M.mode = 'find';
    openBox('find a job: a type, a number, a species, a status', M.q);
    M.search(M.q);
  };
  // Esc, the cross, or a click on the paper: the palette goes; choosing a source, the input is left as it is.
  M.close = () => {
    const wasSlot = !!M.slot;
    hide();
    M.slot = null;
    M.paint();
    if (wasSlot) emit('cb-bm-slot-close', {});
  };
  // The server opened an input's sources (`s`), or closed them (null).
  M.setSlot = (s) => {
    if (!s) { if (M.slot) { hide(); M.slot = null; M.paint(); } return; }
    const same = M.slot && M.slot.nid === s.nid && M.slot.key === s.key;
    M.slot = s;
    M.mode = 'slot';
    if (!same || document.querySelector('.cb-bm-find') === null || box().style.display !== 'block') {
      openBox('narrow them: a type, a number, a species', '');
      M.q = '';
    }
    M.search(M.q);
  };

  // ── the menus of a card: built here from the graph ──
  let menu = null;
  function closeMenu() { if (menu) { menu.remove(); menu = null; } }
  function openMenu(anchor, title, rows) {
    closeMenu();
    menu = document.createElement('div');
    menu.className = 'cb-bm-menu';
    menu.innerHTML = `<div class="h">${attr(title)}</div>` + rows.map((r, i) =>
      `<div class="r${r.off ? ' off' : ''}${r.bad ? ' bad' : ''}" data-i="${i}">`
      + `<i class="material-icons">${attr(r.icon)}</i><span class="t">${attr(r.text)}</span>`
      + `<span class="w">${attr(r.words || '')}</span></div>`).join('');
    document.body.appendChild(menu);
    menu.addEventListener('click', (e) => {
      const r = e.target.closest('.r');
      if (!r || r.classList.contains('off')) return;
      const act = rows[+r.dataset.i].act;
      closeMenu();
      act();
    });
    const a = anchor.getBoundingClientRect(), m = menu.getBoundingClientRect();
    const left = Math.max(6, Math.min(a.right - m.width, window.innerWidth - m.width - 6));
    const below = a.bottom + 4 + m.height <= window.innerHeight - 6;
    menu.style.left = left + 'px';
    menu.style.top = (below ? a.bottom + 4 : Math.max(6, a.top - m.height - 4)) + 'px';
  }
  function plusMenu(anchor, nid) {
    const it = info(nid);
    if (!it || !it.plus) return;
    openMenu(anchor, `Add a job that reads ${it.ident}`, it.plus.map(([kind, name, icon, slot, words]) => ({
      icon, text: name, words, off: !slot,
      act: () => emit('cb-bm-start', { kind, parent: it.plusFrom || it.job || nid, slot }),
    })));
  }
  function jobMenu(anchor, nid) {
    const it = info(nid);
    if (!it) return;
    const jid = it.job || nid;
    const act = (a) => () => emit('cb-bm-job', { nid: jid, act: a });
    const rows = [{ icon: 'open_in_new', text: "Open the job's page", act: act('open') }];
    if (it.clone) rows.push({ icon: 'content_copy', text: 'Clone', words: 'a new job with these parameters, SLURM '
      + 'settings and inputs set by hand', act: act('clone') });
    if (it.remove) rows.push({ icon: 'close', text: 'Remove', bad: true, act: act('remove') });
    openMenu(anchor, it.ident, rows);
  }

  // ── the magazine: which tilt series a mark is, from the pointer ──
  const WORD = { o: 'done', f: 'failed', r: 'running', p: 'waiting', s: 'skipped', n: 'not in this job' };
  function markAt(svg, e) {
    const [per, bw, bh, gap, rgap] = svg.dataset.mag.split(',').map(Number);
    const code = svg.dataset.st || '';
    const r = svg.getBoundingClientRect(), z = r.width / (parseFloat(svg.getAttribute('width')) || r.width || 1);
    const x = (e.clientX - r.left) / z, y = (e.clientY - r.top) / z;
    const col = Math.min(per - 1, Math.max(0, Math.floor((x + gap / 2) / (bw + gap))));
    const row = Math.max(0, Math.floor((y + rgap / 2) / (bh + rgap)));
    const i = row * per + col;
    if (i < 0 || i >= code.length) return null;
    const ts = M.g && M.g.ts && M.g.ts.length === code.length ? M.g.ts[i] : `TS_${String(i + 1).padStart(3, '0')}`;
    const title = (M.g && M.g.tst && M.g.tst[ts]) || ts;
    return { ts, words: `${title} · ${ts} · ${WORD[code[i]] || ''}` };
  }
  let tip = null;
  document.addEventListener('mousemove', (e) => {
    const svg = e.target.closest && e.target.closest('.cb-bm-mag svg[data-mag]');
    if (!svg) { if (tip) tip.style.display = 'none'; return; }
    const m = markAt(svg, e);
    if (!tip) { tip = document.createElement('div'); tip.className = 'cb-title-tip'; document.body.appendChild(tip); }
    if (!m) { tip.style.display = 'none'; return; }
    tip.textContent = m.words;
    tip.style.display = 'block';
    tip.style.left = Math.min(e.clientX + 12, window.innerWidth - tip.offsetWidth - 6) + 'px';
    tip.style.top = (e.clientY + 16) + 'px';
  }, true);

  // ── the browser's events ──
  document.addEventListener('click', (e) => {
    const t = e.target;
    if (menu && !menu.contains(t)) { closeMenu(); if (!(t.closest && t.closest('.cb-bm-add, .cb-bm-num'))) return; }
    const W = world();
    if (!W || !t.closest || !t.closest('.cb-bm-scroll')) return;
    if (t.closest('.cb-bm-node.new')) return;  // the new job's card: its own elements answer
    const mag = t.closest('.cb-bm-mag svg[data-mag]');
    if (mag) { const m = markAt(mag, e); if (m) emit('cb-bm-ts', { ts: m.ts }); return; }
    const node = t.closest('.cb-bm-world .cb-bm-node');
    const nid = node && node.dataset.nid;
    const add = t.closest('.cb-bm-add');
    if (add && nid) { plusMenu(add, nid); return; }
    const num = t.closest('.cb-bm-num:not(.nomenu)');
    if (num && nid) { jobMenu(num, nid); return; }
    if (t.closest('.cb-bm-size') && nid) { emit('cb-bm-size', { nid }); return; }
    if (M.slot && node && node.dataset.c && node.dataset.c !== 'open') { take(nid); return; }
    const row = t.closest('.cb-bm-row.edit');
    if (row && nid) { emit('cb-bm-slot', { nid, key: row.dataset.slot }); return; }
    const trow = t.closest('.cb-bm-trow');
    if (trow) { M.select(trow.dataset.jid, false); emit('cb-bm-click', { nid: trow.dataset.jid }); return; }
    if (t.closest('.cb-bm-trunk')) { emit('cb-bm-trunk', {}); return; }
    const band = t.closest('.cb-bm-band[data-band]');
    if (band) { emit('cb-bm-band', { key: band.dataset.band }); return; }
    if (node && info(nid)) {
      if (M.mode === 'find') M.close();
      const jid = info(nid).job || nid;
      M.select(jid, false);
      emit('cb-bm-click', { nid: jid });
      return;
    }
    if (M.mode || M.slot) { M.close(); return; }
    if (M.sel) { M.select(null); emit('cb-bm-click', { nid: null }); }
  });
  const scrollHide = () => { closeMenu(); if (tip) tip.style.display = 'none'; };
  document.addEventListener('scroll', (e) => {
    if (e.target && e.target.classList && e.target.classList.contains('cb-bm-scroll')) scrollHide();
  }, true);
  document.addEventListener('keydown', (e) => {
    if (!shows() || (e.target.closest && e.target.closest('.cb-bm-find'))) return;
    if (document.querySelector('.q-dialog')) return;
    const pane = document.getElementById('cb-view-pipeline');
    const focused = !!(pane && pane.hasAttribute('data-cb-focus'));
    if (e.key === 'Escape') {
      if (menu) { closeMenu(); return; }
      if (M.mode || M.slot) { M.close(); return; }
      if (M.sel && focused) { M.select(null); emit('cb-bm-select', { nid: null }); }
      return;
    }
    if (!focused) return;
    const a = document.activeElement;
    const fields = 'input, textarea, select, [contenteditable="true"]';
    const typing = a && a !== document.body && a.closest && a.closest(fields);
    if (typing || e.ctrlKey || e.metaKey || e.altKey || e.repeat) return;
    if (e.key === 'f' || e.key === '/') { e.preventDefault(); M.openFind(); return; }
    if (M.sel && ['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(e.key)) {
      e.preventDefault(); M.walk(e.key);
    }
  });
  window.addEventListener('resize', () => M.later());

  // A card that grows, or a skeleton built again, moves the others: the cables follow.
  M.mount = () => {
    const W = world();
    if (!W) return;
    if (!W.__cbBm) {
      W.__cbBm = new ResizeObserver(() => M.later());
      new MutationObserver((ms) => {
        if (ms.some((m) => !(m.target.closest && m.target.closest('.cb-bm-cables')))) { watch(W); M.later(); }
      }).observe(W, { childList: true, subtree: true });
    }
    watch(W);
    const fresh = W.querySelector('.cb-bm-node.new:not([data-seen])');
    if (fresh) { fresh.setAttribute('data-seen', ''); fresh.scrollIntoView(NEAR); }
    M.later();
  };
  const watch = (W) => {
    W.__cbBm.observe(W);
    for (const n of $$('.cb-bm-node', W)) if (!n.__cbBmSeen) { n.__cbBmSeen = true; W.__cbBm.observe(n); }
  };
})();
"""
