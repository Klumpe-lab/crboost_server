// A CryoBoost project snapshot, read-only. The data is window.SNAPSHOT (data.js); each tomogram's
// per-tilt charts are their own script (window.SNAPSHOT_TS[ts]), read when it is opened. Opens from
// disk: no fetch, no modules. Routes live in the hash: #overview, #jobs/<id>, #tomograms/<ts>, #particles.
(function () {
  'use strict';
  const S = window.SNAPSHOT;
  const main = document.getElementById('main');
  const rail = document.getElementById('rail');
  const charts = [];

  // ── helpers ──────────────────────────────────────────────────────────────
  function h(tag, attrs, ...kids) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v === null || v === undefined || v === false) continue;
      if (k === 'class') el.className = v;
      else if (k === 'style') el.style.cssText = v;
      else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
      else el.setAttribute(k, v === true ? '' : v);
    }
    for (const kid of kids.flat()) {
      if (kid === null || kid === undefined || kid === false) continue;
      el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    }
    return el;
  }
  const raw = (html) => { const t = document.createElement('template'); t.innerHTML = html; return t.content; };
  const enc = encodeURIComponent;
  const STATUS = {
    Succeeded: ['#d1fae5', '#065f46'], Failed: ['#fee2e2', '#991b1b'], Running: ['#dbeafe', '#1e40af'],
    Queued: ['#eff6ff', '#2563eb'], Scheduled: ['#fef3c7', '#92400e'],
  };
  function badge(status) {
    const [bg, fg] = STATUS[status] || ['#f1f5f9', '#475569'];
    return h('span', { class: 'badge', style: `background:${bg};color:${fg}` }, status);
  }
  function span(sec) {
    if (sec == null) return '';
    sec = Math.round(sec);
    if (sec < 60) return `${sec} s`;
    const m = Math.floor(sec / 60);
    if (m < 60) return `${m} min`;
    return `${Math.floor(m / 60)} h ${m % 60} min`;
  }
  function fmt(v) {
    if (v === null || v === undefined || v === '') return '—';
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
  }
  function kv(pairs) {
    return h('table', { class: 'kv' }, pairs.map(([k, v]) => h('tr', {}, h('td', {}, k), h('td', {}, fmt(v)))));
  }
  function stats(rows) {
    if (!rows || !rows.length) return null;
    return h('div', { class: 'stats' },
      rows.map(([l, v]) => h('div', { class: 'stat' }, h('div', { class: 'l' }, l), h('div', { class: 'v' }, v))));
  }

  // NiceGUI evaluates option keys that start with ':' as JavaScript; the snapshot does the same.
  function liveOptions(o) {
    if (Array.isArray(o)) return o.map(liveOptions);
    if (o === null || typeof o !== 'object') return o;
    const out = {};
    for (const [k, v] of Object.entries(o)) {
      if (k.startsWith(':')) out[k.slice(1)] = new Function('return ' + v)();
      else out[k] = liveOptions(v);
    }
    return out;
  }
  function chart(el, option) {
    if (!window.echarts) { el.append(h('div', { class: 'muted' }, 'charts unavailable')); return; }
    const c = window.echarts.init(el, null, { renderer: 'canvas' });
    c.setOption(liveOptions(option));
    charts.push(c);
  }
  function plotGrid(list) {
    const grid = h('div', { class: 'plots' });
    const pending = [];
    for (const p of list) {
      const c = h('div', { class: 'c' });
      grid.append(h('div', { class: 'plot' }, h('div', { class: 'l' }, p.title), c));
      pending.push([c, p.option]);
    }
    // Drawn once in the page, so ECharts measures the tiles' real size.
    requestAnimationFrame(() => pending.forEach(([c, o]) => chart(c, o)));
    return grid;
  }
  function clear() {
    while (charts.length) charts.pop().dispose();
    main.replaceChildren();
    main.scrollTop = 0;
  }
  window.addEventListener('resize', () => charts.forEach((c) => c.resize()));

  // Scripts load from disk with a <script> tag (file:// allows no fetch).
  const loading = {};
  function loadPart(path) {
    if (!loading[path]) {
      loading[path] = new Promise((resolve, reject) => {
        const s = h('script', { src: path });
        s.onload = resolve;
        s.onerror = () => reject(new Error(`${path} is missing from the snapshot`));
        document.head.append(s);
      });
    }
    return loading[path];
  }

  // ── overview ─────────────────────────────────────────────────────────────
  function overview() {
    const o = S.overview;
    const facts = [
      ['Project', o.project], ['Created', o.created], ['Snapshot taken', o.snapshot_at],
      ['Tilt series', o.n_series], ['Tomograms', o.n_tomograms], ['Protocol', o.protocol],
      ...o.facts, ['Path on the cluster', o.path], ['CryoBoost version', o.code_version],
    ].filter(([, v]) => v !== null && v !== undefined && v !== '');
    const dl = h('dl', { class: 'facts' }, facts.map(([k, v]) => [h('dt', {}, k), h('dd', {}, v)]));
    const species = S.species.filter((s) => s.id);
    main.append(
      h('div', { class: 'card' }, h('h2', {}, 'Project'), o.note ? h('div', { class: 'note' }, o.note) : null, dl),
      species.length ? h('div', { class: 'card' }, h('h2', {}, 'Species'),
        h('table', { class: 'list' },
          h('tr', {}, ['species', 'Ø (Å)', 'symmetry', 'tomograms with picks', 'picks'].map((t) => h('th', {}, t))),
          species.map((s) => h('tr', {},
            h('td', {}, h('span', { style: `color:${s.color}` }, '● '), s.name),
            h('td', { class: 'num' }, fmt(s.diameter)), h('td', {}, s.symmetry || ''),
            h('td', { class: 'num' }, s.n_tomos_with_picks), h('td', { class: 'num' }, (s.n_picks || 0).toLocaleString()))))) : null,
      h('div', { class: 'card muted' },
        'A read-only snapshot of a CryoBoost project: its jobs, tomograms, tilts, per-tilt measurements and ' +
        'particle results as they stood when it was taken. Nothing here runs or changes anything.'));
  }

  // ── jobs ─────────────────────────────────────────────────────────────────
  function jobs(sel) {
    const list = S.jobs;
    if (!list.length) { main.append(h('div', { class: 'card muted' }, 'No jobs in this project.')); return; }
    const job = list.find((j) => j.iid === sel) || null;
    const table = h('div', { class: 'jobs' },
      h('div', { class: 'row head' }, h('div', {}), h('div', {}, 'job'), h('div', {}, 'state'), h('div', {}, 'elapsed')));
    for (const j of list) {
      const elapsed = j.start && j.end ? span(j.end - j.start) : '';
      table.append(h('div', { class: 'row' + (j === job ? ' on' : ''), onclick: () => go(`jobs/${enc(j.iid)}`) },
        h('div', {}, raw(j.gutter)), h('div', {}, j.name), h('div', {}, badge(j.status)),
        h('div', { class: 'muted mono' }, elapsed)));
    }
    const left = h('div', { class: 'card' }, h('h2', {}, 'Jobs'), table);
    if (!job) {
      main.append(h('div', { class: 'split' }, left,
        h('div', { class: 'card muted' }, 'Choose a job to see its parameters, inputs, resources and log.')));
      return;
    }
    const right = h('div', { class: 'card' },
      h('h2', {}, job.name, ' ', badge(job.status)),
      kv([['Type', job.type], ['Instance', job.iid], ['Directory', job.job_dir],
        ['Started', job.start ? new Date(job.start * 1000).toLocaleString() : null],
        ['Ended', job.end ? new Date(job.end * 1000).toLocaleString() : null]]),
      job.inputs.length ? [h('h3', {}, 'Inputs'),
        kv(job.inputs.map((e) => [e.slot, `${e.producer} · ${e.output}${e.explicit ? '' : ' (automatic)'}`]))] : null,
      h('h3', {}, 'Parameters'), job.params.length ? kv(job.params) : h('div', { class: 'muted' }, 'none'),
      job.slurm ? [h('h3', {}, 'SLURM'), kv(Object.entries(job.slurm).filter(([, v]) => v !== '' && v !== null))] : null,
      job.log ? [h('h3', {}, 'run.out (last lines)'), h('pre', { class: 'log' }, job.log)] : null,
      job.err ? [h('h3', {}, 'run.err (last lines)'), h('pre', { class: 'log' }, job.err)] : null);
    main.append(h('div', { class: 'split' }, left, right));
  }

  // ── tomograms ────────────────────────────────────────────────────────────
  const view = { source: null, layers: new Set(S.pick_layers.map((l) => l.id)), tile: 200 };
  const layerById = Object.fromEntries(S.pick_layers.map((l) => [l.id, l]));
  const sourceKeys = [...new Set(S.tomograms.flatMap((t) => t.sources.map((s) => s.key)))];
  // The version first shown: the one most pick layers were made on, so the picks show; else the first.
  const reconOfKey = Object.fromEntries(S.tomograms.flatMap((t) => t.sources).map((s) => [s.key, s.recon]));
  const layersOn = (k) => S.pick_layers.filter((l) => l.recon != null && l.recon === reconOfKey[k]).length;
  view.source = [...sourceKeys].sort((a, b) => layersOn(b) - layersOn(a))[0] || null;

  function sourceOf(t) {
    return t.sources.find((s) => s.key === view.source) || t.sources[0] || null;
  }
  // A layer follows the version: drawn over the reconstruction its picks were made on, or when either
  // side's reconstruction is not known (the wall's rule, ui/tomo_gallery.py `_layer_shown`).
  function follows(layer, src) {
    return !layer || layer.recon == null || !src || src.recon == null || layer.recon === src.recon;
  }
  function picksSvg(t, r) {
    const src = sourceOf(t);
    const dots = t.picks.filter((p) => view.layers.has(p.layer) && follows(layerById[p.layer], src));
    if (!dots.length) return null;
    const ns = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('class', 'picks');
    for (const p of dots) {
      const g = document.createElementNS(ns, 'g');
      const color = (layerById[p.layer] || {}).color || '#22d3ee';
      g.setAttribute('fill', 'none');
      g.setAttribute('stroke', color);
      g.setAttribute('stroke-width', '1.2');
      for (const [x, y] of p.dots) {
        const c = document.createElementNS(ns, 'circle');
        c.setAttribute('cx', `${x}%`); c.setAttribute('cy', `${y}%`); c.setAttribute('r', r);
        g.append(c);
      }
      svg.append(g);
    }
    return svg;
  }
  function frame(t, big) {
    const src = sourceOf(t);
    const ratio = t.aspect ? `${t.aspect[0]} / ${t.aspect[1]}` : '1 / 1';
    const f = h('div', { class: 'frame', style: `aspect-ratio:${ratio}` });
    if (src) {
      const img = h('img', { src: big ? src.img : src.thumb, alt: t.ts, loading: 'lazy', decoding: 'async' });
      img.onerror = () => { img.remove(); f.append(h('div', { class: 'empty' }, 'picture missing from the snapshot')); };
      f.append(img);
      const svg = picksSvg(t, big ? 4 : 2.5);
      if (svg) f.append(svg);
    } else {
      f.append(h('div', { class: 'empty' }, t.missing_sources.length ? 'not rendered' : 'no tomogram'));
    }
    return f;
  }
  function toolbar(redraw) {
    const bar = h('div', { class: 'toolbar' });
    if (sourceKeys.length > 1) {
      const sel = h('select', { onchange: (e) => { view.source = e.target.value; redraw(); } },
        sourceKeys.map((k) => h('option', { value: k, selected: k === view.source }, k)));
      bar.append(h('span', { class: 'muted' }, 'version'), sel);
    }
    // A layer made on another version than the one showing is left out of the bar.
    const showing = S.tomograms.flatMap((t) => t.sources).find((s) => s.key === view.source) || null;
    for (const l of S.pick_layers) {
      if (!follows(l, showing)) continue;
      const chip = h('span', { class: 'chip' + (view.layers.has(l.id) ? '' : ' off'), title: `${l.n.toLocaleString()} picks` },
        h('span', { class: 'dot', style: `background:${l.color}` }), `${l.species} · ${l.label}`);
      chip.onclick = () => { view.layers.has(l.id) ? view.layers.delete(l.id) : view.layers.add(l.id); redraw(); };
      bar.append(chip);
    }
    return bar;
  }
  function wall() {
    const ts = S.tomograms;
    if (!ts.length) { main.append(h('div', { class: 'card muted' }, 'No tilt series in this project.')); return; }
    const redraw = () => { clear(); wall(); };
    const bar = toolbar(redraw);
    const size = h('input', { type: 'range', min: 120, max: 420, step: 20, value: view.tile });
    const grid = h('div', { class: 'wall', style: `--tile:${view.tile}px` });
    size.oninput = () => { view.tile = +size.value; grid.style.setProperty('--tile', `${view.tile}px`); };
    bar.append(h('span', { class: 'muted', style: 'margin-left:auto' }, 'size'), size);
    for (const t of ts) {
      const n = t.tilts.length ? `${t.tilts.length - t.n_excluded}/${t.tilts.length} tilts` : '';
      grid.append(h('div', { class: 'tile', title: t.ts, onclick: () => go(`tomograms/${enc(t.ts)}`) },
        frame(t, false), h('div', { class: 'cap' }, h('span', {}, t.label), h('span', { class: 'n' }, n))));
    }
    main.append(bar, grid);
  }
  function tomogram(name) {
    const i = S.tomograms.findIndex((t) => t.ts === name);
    if (i < 0) { go('tomograms'); return; }
    const t = S.tomograms[i];
    const prev = S.tomograms[i - 1], next = S.tomograms[i + 1];
    const redraw = () => { clear(); tomogram(name); };
    const head = h('div', { class: 'detail-head' },
      h('button', { class: 'btn', onclick: () => go('tomograms') }, '← all tomograms'),
      h('span', { class: 'name' }, t.label), h('span', { class: 'ts' }, t.ts),
      h('span', { style: 'margin-left:auto' }),
      h('button', { class: 'btn', disabled: !prev, onclick: () => prev && go(`tomograms/${enc(prev.ts)}`) }, '‹ previous'),
      h('button', { class: 'btn', disabled: !next, onclick: () => next && go(`tomograms/${enc(next.ts)}`) }, 'next ›'));
    const src = sourceOf(t);
    const side = h('div', { class: 'side' },
      src ? h('div', { class: 'muted', style: 'margin-bottom:6px' }, src.caption) : null,
      t.picks.length ? kv(t.picks.map((p) => [(layerById[p.layer] || {}).label || p.layer, `${p.n.toLocaleString()} picks`])) : null,
      t.tilts.length ? h('div', { class: 'muted', style: 'margin-top:6px' },
        `${t.tilts.length} tilts, ${t.n_excluded} excluded by the tilt filter`) : null);
    const top = h('div', { class: 'card' }, toolbar(redraw), h('div', { class: 'detail-top' }, frame(t, true), side));
    main.append(head, top);
    if (t.tilts.length) {
      const strip = h('div', { class: 'strip' }, t.tilts.map((x) => {
        const cls = 'tilt' + (x.excluded ? ' excluded' : '') + (x.dark ? ' dark' : '');
        const img = x.img ? h('img', { src: x.img, loading: 'lazy', alt: x.frame }) : h('img', { alt: '' });
        return h('div', { class: cls, title: x.tip }, img,
          h('div', { class: 'a' }, `${x.angle.toFixed(1)}° · ${x.n}${x.excluded ? ' · out' : ''}`));
      }));
      main.append(h('div', { class: 'card' }, h('h2', {}, 'Tilts'), strip));
    }
    const box = h('div', { class: 'card' }, h('h2', {}, 'Per-tilt measurements'));
    main.append(box);
    if (t.note) box.append(h('div', { class: 'note' }, t.note));
    if (!t.part) { box.append(h('div', { class: 'muted' }, 'No per-tilt measurements for this tilt series.')); return; }
    const wait = h('div', { class: 'muted' }, 'reading…');
    box.append(wait);
    loadPart(t.part).then(() => {
      wait.remove();
      for (const sec of window.SNAPSHOT_TS[t.ts] || []) {
        box.append(...[h('h3', {}, sec.title, h('span', { class: 'job' }, sec.job)), stats(sec.stats), plotGrid(sec.charts)]
          .filter(Boolean));
      }
    }, (e) => { wait.textContent = e.message; });
  }

  // ── particles ────────────────────────────────────────────────────────────
  function particles() {
    const list = S.species.filter((s) => s.readouts.length || (s.lists && s.lists.length));
    if (!list.length) { main.append(h('div', { class: 'card muted' }, 'No particle results in this project.')); return; }
    for (const s of list) {
      const card = h('div', { class: 'card' }, h('h2', {}, h('span', { style: `color:${s.color}` }, '● '), s.name));
      if (s.id) {
        card.append(stats([['Picks', (s.n_picks || 0).toLocaleString()], ['Kept', (s.n_kept || 0).toLocaleString()],
          ['Tomograms with picks', String(s.n_tomos_with_picks || 0)]]));
      }
      for (const r of s.readouts) {
        card.append(h('h3', {}, r.name));
        if (r.note) card.append(h('div', { class: 'note' }, r.note));
        if (r.facts && r.facts.length) card.append(stats(r.facts));
        if (r.maps && r.maps.length) {
          card.append(h('div', { class: 'maps' }, r.maps.map((m) =>
            h('figure', {}, h('img', { src: m.img, alt: m.label }), h('figcaption', {}, m.label)))));
        }
        if (r.charts && r.charts.length) card.append(plotGrid(r.charts));
      }
      if (s.lists && s.lists.length) {
        card.append(h('h3', {}, 'Pick lists'), h('table', { class: 'list' },
          h('tr', {}, ['tomogram', 'list', 'type', 'picks', 'kept'].map((x) => h('th', {}, x))),
          s.lists.map((l) => h('tr', {}, h('td', {}, l.tomo), h('td', {}, l.label), h('td', {}, l.type),
            h('td', { class: 'num' }, l.count.toLocaleString()), h('td', { class: 'num' }, l.kept == null ? '—' : l.kept.toLocaleString())))));
      }
      main.append(card);
    }
  }

  // ── routing ──────────────────────────────────────────────────────────────
  const VIEWS = [
    ['overview', 'Overview', null],
    ['jobs', 'Jobs', S.jobs.length],
    ['tomograms', 'Tomograms', S.tomograms.length],
    ['particles', 'Particles', S.species.reduce((n, s) => n + s.readouts.length, 0) || null],
  ];
  function go(route) { location.hash = '#' + route; }
  function show() {
    const [view, ...rest] = location.hash.replace(/^#/, '').split('/');
    const target = rest.length ? decodeURIComponent(rest.join('/')) : null;
    const name = VIEWS.some(([k]) => k === view) ? view : 'overview';
    rail.querySelectorAll('a').forEach((a) => a.classList.toggle('on', a.dataset.view === name));
    clear();
    if (name === 'jobs') jobs(target);
    else if (name === 'tomograms') (target ? tomogram(target) : wall());
    else if (name === 'particles') particles();
    else overview();
  }
  for (const [key, label, n] of VIEWS) {
    rail.append(h('a', { href: `#${key}`, 'data-view': key }, label, n ? h('span', { class: 'count' }, n) : null));
  }
  document.addEventListener('keydown', (e) => {
    const [view, ...rest] = location.hash.replace(/^#/, '').split('/');
    if (view !== 'tomograms' || !rest.length || e.target.closest('input, select')) return;
    const i = S.tomograms.findIndex((t) => t.ts === decodeURIComponent(rest.join('/')));
    if (e.key === 'Escape') go('tomograms');
    else if (e.key === 'ArrowRight' && S.tomograms[i + 1]) go(`tomograms/${enc(S.tomograms[i + 1].ts)}`);
    else if (e.key === 'ArrowLeft' && S.tomograms[i - 1]) go(`tomograms/${enc(S.tomograms[i - 1].ts)}`);
  });
  document.getElementById('title').textContent = S.overview.project;
  document.getElementById('sub').textContent = `snapshot of ${S.overview.snapshot_at.replace('T', ' ')} · read-only`;
  document.title = `${S.overview.project} · CryoBoost snapshot`;
  window.addEventListener('hashchange', show);
  show();
})();
