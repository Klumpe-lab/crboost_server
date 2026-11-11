// A frozen page of a project snapshot (ui/snapshot/): the workspace as it was captured, read-only.
// Brings the charts back from their options, and makes the parts that move between views go to the
// captured page of that view: the rail's icons, the job roster's rows, the Journey's tilt series,
// the wall's tiles, the segmented tabs. Everything else is inert — nothing here can run or change.
(function () {
  'use strict';
  const F = window.FROZEN;
  document.body.classList.add('cb-frozen');

  // ── charts ────────────────────────────────────────────────────────────────
  // NiceGUI evaluates option keys that start with ':' as JavaScript; so does the frozen page.
  function live(o) {
    if (Array.isArray(o)) return o.map(live);
    if (o === null || typeof o !== 'object') return o;
    const out = {};
    for (const [k, v] of Object.entries(o)) {
      if (k.startsWith(':')) {
        try { out[k.slice(1)] = new Function('return ' + v)(); } catch (e) { /* a broken formatter: ECharts' default */ }
      } else out[k] = live(v);
    }
    return out;
  }
  const charts = [];
  if (window.echarts) {
    for (const el of document.querySelectorAll('[data-frozen-chart]')) {
      const opts = F.charts[el.getAttribute('data-frozen-chart')];
      if (!opts) continue;
      const c = window.echarts.init(el);
      c.setOption(live(opts));
      charts.push(c);
    }
  }
  window.addEventListener('resize', () => charts.forEach((c) => c.resize()));

  // ── navigation ────────────────────────────────────────────────────────────
  const mark = (el, file, title) => {
    if (!el || !file) return;
    el.setAttribute('data-cb-go', file);
    if (title) el.setAttribute('title', title);
  };
  document.querySelectorAll('[data-cb-rail]').forEach((el) => mark(el, F.rail[el.getAttribute('data-cb-rail')]));
  document.querySelectorAll('[data-cb-job]').forEach((el) => mark(el, F.jobs[el.getAttribute('data-cb-job')]));
  const journeyOf = Object.fromEntries(F.journey.map((j) => [j.ts, j]));
  document.querySelectorAll('[data-cb-ts]').forEach((el) => {
    const j = journeyOf[el.getAttribute('data-cb-ts')];
    if (j) mark(el, j.file, `${j.title}: open its Journey`);
  });
  const segs = F.seg[F.scene] || {};
  document.querySelectorAll('[data-cb-seg]').forEach((el) => mark(el, segs[el.getAttribute('data-cb-seg')]));

  // The Journey's tilt-series switch: the list of every captured tilt series, and ‹ › beside its count.
  const i = F.journey.findIndex((j) => j.file === F.files[F.scene]);
  if (i >= 0) {
    const sw = document.querySelector('.cb-ts-switch');
    if (sw) {
      sw.setAttribute('data-cb-go', '#list');
      sw.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); toggleList(sw); }, true);
    }
    const idx = document.querySelector('.cb-ts-idx');
    if (idx) {
      const step = (d, text) => {
        const j = F.journey[i + d];
        const a = document.createElement('a');
        a.className = 'cb-frozen-step' + (j ? '' : ' off');
        a.textContent = text;
        if (j) { a.href = j.file; a.title = j.title; }
        return a;
      };
      idx.before(step(-1, '‹'));
      idx.after(step(1, '›'));
    }
    document.addEventListener('keydown', (e) => {
      const j = F.journey[i + (e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0)];
      if (j && j !== F.journey[i]) location.href = j.file;
    });
  }
  let list = null;
  function toggleList(anchor) {
    if (list) { list.remove(); list = null; return; }
    list = document.createElement('div');
    list.className = 'cb-frozen-list';
    const r = anchor.getBoundingClientRect();
    list.style.left = `${r.left}px`;
    list.style.top = `${r.bottom + 3}px`;
    for (const j of F.journey) {
      const a = document.createElement('a');
      a.href = j.file;
      a.textContent = j.title;
      a.title = j.ts;
      if (j === F.journey[i]) a.className = 'on';
      list.append(a);
    }
    document.body.append(list);
    list.querySelector('.on')?.scrollIntoView({ block: 'center' });
  }

  // A click goes where its element was marked to, or nowhere: the live page's handlers are gone.
  document.addEventListener('click', (e) => {
    if (list && !list.contains(e.target)) { list.remove(); list = null; }
    if (e.target.closest('.cb-frozen-list, .cb-frozen-step, .cb-frozen-tag')) return;
    const go = e.target.closest('[data-cb-go]');
    e.preventDefault();
    e.stopPropagation();
    if (go && go.getAttribute('data-cb-go') !== '#list') location.href = go.getAttribute('data-cb-go');
  }, true);
  for (const el of document.querySelectorAll('input, textarea')) el.readOnly = true;
  for (const el of document.querySelectorAll('select, button')) el.disabled = true;

  // Which snapshot this is, and the way back to its first page.
  const tag = document.createElement('a');
  tag.className = 'cb-frozen-tag';
  tag.href = 'index.html';
  tag.textContent = `${F.project} · snapshot · read-only`;
  document.body.append(tag);
})();
