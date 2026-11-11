// A project snapshot's capture (ui/snapshot/routes.py): show every scene of the plan in the iframe — the
// real workspace — wait for the view to settle, freeze it (its DOM, the page's styles, its charts'
// options, every file it names) and post it to the server, which zips them (services/snapshot/bundle.py).
(async function () {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const frame = $('frame');
  const source = new URLSearchParams(location.search).get('source') || '';
  const SETTLE_QUIET_MS = 1500;
  const SETTLE_MAX_MS = 60000;
  // Still drawing: a picture rendering, a spinner, a view "reading".
  const BUSY = '.cb-rslot--run, .q-spinner, .q-inner-loading, .q-skeleton';

  const say = (text) => { $('status').textContent = text; };
  const progress = (f) => { $('bar').firstElementChild.style.width = `${Math.round(f * 100)}%`; };
  const fail = (text) => { $('error').textContent = text; say('Stopped.'); document.title = 'Snapshot failed'; };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  // ── driving the workspace ─────────────────────────────────────────────────
  function load(path) {
    return new Promise((resolve) => {
      frame.onload = () => resolve();
      frame.src = path;
    });
  }
  async function until(test, ms) {
    const t0 = Date.now();
    while (Date.now() - t0 < ms) {
      if (test()) return true;
      await sleep(200);
    }
    return false;
  }
  // NiceGUI's app: a top-level `let` of its script, a global but not a property of the window.
  function appOf(win) {
    try { return win.eval('typeof mounted_app === "undefined" ? undefined : mounted_app'); } catch (e) { return undefined; }
  }
  function paneOf(doc, name) {
    return doc.getElementById(name === 'roster' ? 'cb-roster-panel' : `cb-view-${name}`);
  }
  const shows = (el) => !!el && el.getClientRects().length > 0 && getComputedStyle(el).display !== 'none';
  function openPanes(doc) {
    const out = [];
    if (shows(doc.getElementById('cb-roster-panel'))) out.push('roster');
    doc.querySelectorAll('#cb-view-main > [id^="cb-view-"]').forEach((el) => {
      if (shows(el)) out.push(el.id.slice('cb-view-'.length));
    });
    return out;
  }
  // The workspace's own routing: the History API and a popstate, as Back does (ui/routing.py).
  async function go(path) {
    const win = frame.contentWindow;
    win.history.pushState(null, '', path);
    win.dispatchEvent(new PopStateEvent('popstate'));
  }
  // Panes the scene does not show are closed as their cross would (the workspace's Esc), so the
  // server is not keeping views no one captures.
  function closeOthers(solo) {
    const win = frame.contentWindow;
    for (const name of openPanes(win.document)) {
      if (!solo.includes(name) && !(name === 'pipeline' && solo.includes('roster'))) {
        win.emitEvent('cb_view_escape', name === 'pipeline' ? 'roster' : name);
      }
    }
  }
  async function settle(solo) {
    const doc = frame.contentWindow.document;
    const panes = () => solo.map((n) => paneOf(doc, n));
    if (!(await until(() => panes().every(shows), SETTLE_MAX_MS))) return false;
    let last = Date.now();
    const obs = new MutationObserver(() => { last = Date.now(); });
    obs.observe(doc.body, { subtree: true, childList: true, attributes: true, characterData: true });
    const busy = () => panes().some((p) => [...p.querySelectorAll(BUSY)].some(shows));
    const ok = await until(() => Date.now() - last > SETTLE_QUIET_MS && !busy(), SETTLE_MAX_MS);
    obs.disconnect();
    return ok;
  }

  // What a reader would open: folded sections (a job's Results, its Log) and, where a view offers it,
  // its own "Expand all" (the Tilts tab's series). True when something was opened.
  function expandAll(solo) {
    const doc = frame.contentWindow.document;
    let opened = false;
    for (const pane of solo.map((n) => paneOf(doc, n)).filter(Boolean)) {
      pane.querySelectorAll('.q-expansion-item--collapsed > .q-expansion-item__container > .q-item').forEach((h) => {
        h.click();
        opened = true;
      });
      // A job page's folded sections (job_tab_component `_render_lazy_section`), all but Files: a
      // directory listing no snapshot can browse.
      pane.querySelectorAll('.cb-jsec-fold:not([id$="-files"])').forEach((sec) => {
        const head = sec.firstElementChild;
        if (head && head.textContent.trim().startsWith('▸')) {
          head.click();
          opened = true;
        }
      });
      const all = [...pane.querySelectorAll('.q-btn, button')].find((b) => b.textContent.trim() === 'Expand all');
      if (all) { all.click(); opened = true; }
    }
    return opened;
  }

  // ── freezing ──────────────────────────────────────────────────────────────
  function hash(text) {
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++) { h ^= text.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
    return h.toString(36) + text.length.toString(36);
  }
  const URL_IN_CSS = /url\(\s*["']?([^"')]+)["']?\s*\)/g;
  // Every stylesheet the page has, @imports inlined in their layers, url()s made absolute.
  function pageCss(doc, assets) {
    const walk = (sheet) => {
      let rules;
      try { rules = sheet.cssRules; } catch (e) { return ''; }
      const base = sheet.href || doc.location.href;
      let css = '';
      for (const r of rules) {
        if (r instanceof doc.defaultView.CSSImportRule) {
          const inner = r.styleSheet ? walk(r.styleSheet) : '';
          css += r.layerName != null ? `@layer ${r.layerName} {\n${inner}\n}\n` : inner;
          continue;
        }
        css += r.cssText.replace(URL_IN_CSS, (m, u) => {
          if (u.startsWith('data:') || u.startsWith('#')) return m;
          const a = new URL(u, base).href;
          assets.add(a);
          return `url("${a}")`;
        }) + '\n';
      }
      return css;
    };
    return [...doc.styleSheets].map(walk).join('');
  }
  function freeze(scene) {
    const win = frame.contentWindow;
    const doc = win.document;
    const origin = win.location.origin;
    const assets = new Set();
    const css = pageCss(doc, assets);
    const absolute = (u) => {
      const a = new URL(u, win.location.href).href;
      if (a.startsWith(origin)) assets.add(a);
      return a;
    };

    // How wide each picture shows (CSS px), so the bundle need not carry one many times that size: an
    // <img> by its box, a sprite by its background-size. The widest use wins; 0 is "not known".
    const shown = {};
    const seen = (u, w) => {
      if (!u || u.startsWith('data:') || !(w > 0)) return;
      const a = new URL(u, win.location.href).href;
      shown[a] = Math.max(shown[a] || 0, Math.ceil(w));
    };
    const panesLive = scene.solo.map((n) => paneOf(doc, n)).filter(Boolean);
    for (const pane of panesLive) {
      pane.querySelectorAll('img[src]').forEach((i) => seen(i.getAttribute('src'), i.getBoundingClientRect().width));
      pane.querySelectorAll('[style*="url("]').forEach((n) => {
        const u = /url\(\s*["']?([^"')]+)/.exec(n.style.backgroundImage || '');
        const w = parseFloat((n.style.backgroundSize || '').split(' ')[0]);
        if (u && /px$/.test((n.style.backgroundSize || '').split(' ')[0])) seen(u[1], w);
      });
    }

    // What a clone loses: values typed or set as properties, and what is drawn on a canvas.
    const live = [...doc.body.querySelectorAll('input, textarea, select, canvas')];
    const clone = doc.body.cloneNode(true);
    const copies = [...clone.querySelectorAll('input, textarea, select, canvas')];
    live.forEach((el, i) => {
      const c = copies[i];
      if (!c || c.tagName !== el.tagName) return;
      if (el.tagName === 'INPUT') {
        if (el.type === 'checkbox' || el.type === 'radio') { if (el.checked) c.setAttribute('checked', ''); }
        else c.setAttribute('value', el.value);
      } else if (el.tagName === 'TEXTAREA') c.textContent = el.value;
      else if (el.tagName === 'SELECT') [...c.options].forEach((o, j) => o.toggleAttribute('selected', el.options[j]?.selected));
      else if (el.tagName === 'CANVAS' && !el.closest('[_echarts_instance_]')) {
        try {
          const img = doc.createElement('img');
          img.src = el.toDataURL();
          img.style.cssText = el.style.cssText;
          img.width = el.width; img.height = el.height;
          c.replaceWith(img);
        } catch (e) { /* a tainted canvas stays blank */ }
      }
    });

    // Only the scene's panes: the others are dropped (the workspace keeps them, hidden).
    const keep = new Set(scene.solo);
    if (!keep.has('roster')) clone.querySelector('#cb-roster-panel')?.remove();
    clone.querySelectorAll('#cb-roster-resizer').forEach((n) => { if (!keep.has('roster')) n.remove(); });
    clone.querySelectorAll('#cb-view-main > [id^="cb-view-"]').forEach((el) => {
      const name = el.id.slice('cb-view-'.length);
      if (keep.has(name)) el.style.setProperty('display', 'flex');
      else el.remove();
    });
    clone.querySelectorAll('script, style, #popup, .q-tooltip, .q-menu, .q-notifications, .q-dialog').forEach((n) => n.remove());

    // Charts come back from their options (NiceGUI's `options` prop, `:`-keys still JavaScript).
    const charts = {};
    for (const [id, el] of Object.entries(appOf(win).elements)) {
      const opts = el && el.props && el.props.options;
      if (!opts || !/echart/i.test((el.component && el.component.name) || el.tag || '')) continue;
      const node = clone.querySelector(`#c${id}`);
      if (!node) continue;
      node.replaceChildren();
      node.removeAttribute('_echarts_instance_');
      node.setAttribute('data-frozen-chart', `c${id}`);
      charts[`c${id}`] = opts;
    }
    let chartsJson = JSON.stringify(charts).replace(/"(\/(?:api|_nicegui|static)\/[^"]*)"/g, (m, u) => `"${absolute(u)}"`);
    chartsJson = chartsJson.replace(/(src=\\?["'])(\/(?:api|_nicegui|static)\/[^"'\\]*)/g, (m, pre, u) => pre + absolute(u));

    // Files the DOM names: pictures, inline backgrounds, SVG images.
    clone.querySelectorAll('img[src]').forEach((i) => {
      const s = i.getAttribute('src');
      if (!s.startsWith('data:')) i.setAttribute('src', absolute(s));
    });
    clone.querySelectorAll('[style*="url("]').forEach((n) => {
      n.setAttribute('style', n.getAttribute('style').replace(URL_IN_CSS, (m, u) => (u.startsWith('data:') ? m : `url("${absolute(u)}")`)));
    });
    clone.querySelectorAll('image[href]').forEach((n) => n.setAttribute('href', absolute(n.getAttribute('href'))));

    return {
      key: scene.key,
      title: scene.title,
      solo: scene.solo,
      html: clone.outerHTML,
      html_class: doc.documentElement.className,
      css: hash(css),
      css_text: css,
      charts: JSON.parse(chartsJson),
      assets: [...assets],
      shown,
    };
  }

  async function post(url, body) {
    const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const data = await r.json().catch(() => ({ error: `${r.status} ${r.statusText}` }));
    if (!r.ok || data.error) throw new Error(data.error || `${r.status}`);
    return data;
  }

  // ── the run ───────────────────────────────────────────────────────────────
  let plan;
  try {
    const r = await fetch(`/api/snapshot/plan?source=${encodeURIComponent(source)}`);
    plan = await r.json();
    if (!r.ok) throw new Error(plan.error || `${r.status}`);
  } catch (e) {
    fail(`Could not plan the snapshot: ${e.message}`);
    return;
  }
  $('title').textContent = `Taking a snapshot of ${plan.project}`;
  document.title = `Snapshot · ${plan.project}`;
  const sentCss = new Set();
  const slow = [];
  const n = plan.scenes.length;
  try {
    for (let i = 0; i < n; i++) {
      const s = plan.scenes[i];
      say(`${i + 1} / ${n} · ${s.title}  (keep this window open)`);
      progress(i / (n + 1));
      if (i === 0) await load(s.path);
      else await go(s.path);
      if (!(await until(() => appOf(frame.contentWindow), SETTLE_MAX_MS))) throw new Error('the workspace did not load');
      await sleep(300);
      closeOthers(s.solo);
      if (s.seg) {
        await settle(s.solo);
        const seg = paneOf(frame.contentWindow.document, s.solo[0])?.querySelector(`[data-cb-seg="${s.seg}"]`);
        if (seg && !seg.classList.contains('active')) seg.click();
      }
      if (!(await settle(s.solo))) slow.push(s.title);
      if (expandAll(s.solo) && !(await settle(s.solo))) slow.push(s.title);
      const frozen = freeze(s);
      if (sentCss.has(frozen.css)) delete frozen.css_text;
      let res = await post(`/api/snapshot/scene?session=${plan.session}`, frozen);
      if (res.need_css) res = await post(`/api/snapshot/scene?session=${plan.session}`, { ...freeze(s) });
      sentCss.add(frozen.css);
    }
    say('Writing the zip…');
    progress(n / (n + 1));
    const done = await post(`/api/snapshot/finish?session=${plan.session}`, {});
    progress(1);
    say(`Done: ${done.n_scenes} views, ${(done.size / 1e6).toFixed(1)} MB. Downloading ${done.name}.`);
    const a = Object.assign(document.createElement('a'), { href: done.url, download: done.name });
    document.body.append(a);
    a.click();
    $('done').innerHTML = '';
    $('done').append('Not downloading? ', Object.assign(document.createElement('a'), { href: done.url, textContent: done.name }),
      done.missing ? ` · ${done.missing} file(s) could not be included (listed in missing.json)` : '',
      slow.length ? ` · still drawing when captured: ${slow.join(', ')}` : '');
    document.title = 'Snapshot ready';
    frame.remove();
    $('stage').remove();
  } catch (e) {
    fail(`The snapshot stopped: ${e.message}`);
  }
})();
