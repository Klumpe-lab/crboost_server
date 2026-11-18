"""A long path in the width it is given: cut in its middle, never at its end.

As much of the path's start as fits, an ellipsis, then its last part (`/name`) whole. Only when the name alone does
not fit is the name itself cut in its middle, its end (the extension) kept. Inside the project the path is written
from the project folder, outside it whole. The cut is made in the browser, in characters (the face is monospaced; one
character's width is measured in the element itself, and again once the page's fonts are ready), and made again
whenever the element's width changes, so a path only loses characters when its space does.

A click copies the whole absolute path. Hovering lays a lit copy over the path, anchored at the ellipsis, that unrolls:
the hidden middle comes back a few characters a frame from both sides of it, then the project folder comes in on the
left, until the absolute path shows (~230 ms whatever its length). The copy closes when the pointer leaves, or when a
scroll moves the path; a log scrolling elsewhere, or a scroll event that moved nothing, does not close it.

The script (`PATH_JS`) and the styles (`PATH_CSS`) are delivered per client by `ui/dashboard/css.py`
(`ensure_assets_loaded`).
"""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from ui.components.copyable import copy_on_click

PATH_CSS = """
.cb-path { display: block; width: 100%; min-width: 0; overflow: hidden; white-space: nowrap; cursor: pointer;
  font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; line-height: 14px; color: #5b7089; }
.cb-path .n { color: #1e293b; }
.cb-path .e { color: #94a3b8; }
.cb-path.cb-copy-path:hover { text-decoration: none; color: #5b7089 !important; }
.cb-unfurl { position: fixed; z-index: 9000; pointer-events: none; white-space: pre; padding: 2px 6px;
  font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; line-height: 14px; color: #94a3b8;
  background: #0f1d2e; border-radius: 2px;
  box-shadow: 0 0 0 1px rgba(74, 143, 212, 0.55), 0 0 16px 2px rgba(74, 143, 212, 0.35);
  opacity: 0; transition: opacity 80ms ease-out; }
.cb-unfurl.on { opacity: 1; }
.cb-unfurl .n { color: #ffffff; }
.cb-unfurl .e { color: #5b7089; }
"""

PATH_JS = r"""
(() => {
  if (window.cbPath) { window.cbPath.scan(); return; }
  // One character's width, measured in the element itself; measured again, and every path cut again, once the
  // page's fonts are ready (a measure taken before the face loads is a fallback face's).
  const widths = new Map();
  const charW = (el) => {
    const font = getComputedStyle(el).font;
    if (!widths.has(font)) {
      const probe = document.createElement('span');
      probe.textContent = '0'.repeat(40);
      probe.style.cssText = 'position:absolute;visibility:hidden;white-space:pre;';
      el.appendChild(probe);
      widths.set(font, probe.getBoundingClientRect().width / 40);
      probe.remove();
    }
    return widths.get(font);
  };
  const esc = (t) => t.replace(/&/g, '&amp;').replace(/</g, '&lt;');
  // The cut of s into cap characters, as indices: shown = s[hs:he] + '…' + s[ts:].
  const cut = (s, cap) => {
    if (s.length <= cap) return { hs: 0, he: s.length, ts: s.length, cut: false };
    cap = Math.max(cap, 4);
    const slash = s.lastIndexOf('/');
    const tail = slash >= 0 ? slash : 0;
    if (s.length - tail + 2 <= cap) return { hs: 0, he: cap - 1 - (s.length - tail), ts: tail, cut: true };
    const name = slash + 1, room = cap - 1, keep = Math.ceil(room * 0.6);
    return { hs: name, he: name + (room - keep), ts: s.length - keep, cut: true };
  };
  // s[a:b] as HTML, the file name's characters in the name's ink.
  const part = (s, a, b, nameAt) => {
    if (b <= a) return '';
    if (b <= nameAt) return esc(s.slice(a, b));
    if (a >= nameAt) return `<span class="n">${esc(s.slice(a, b))}</span>`;
    return esc(s.slice(a, nameAt)) + `<span class="n">${esc(s.slice(nameAt, b))}</span>`;
  };
  const draw = (el) => {
    const s = el.dataset.path || '';
    if (el.clientWidth === 0) return;
    const w = charW(el);
    const cap = Math.floor(el.clientWidth / w);
    if (cap === el._cap && el._shown === s && el.firstChild) return;
    el._cap = cap;
    el._shown = s;
    const c = cut(s, cap);
    el._cut = c;
    const nameAt = s.lastIndexOf('/') + 1;
    el.innerHTML = c.cut
      ? part(s, c.hs, c.he, nameAt) + '<span class="e">…</span>' + part(s, c.ts, s.length, nameAt)
      : part(s, 0, s.length, nameAt);
  };
  let live = null;
  const close = () => {
    if (!live) return;
    cancelAnimationFrame(live.raf);
    const o = live.o;
    o.classList.remove('on');
    setTimeout(() => o.remove(), 90);
    live = null;
  };
  const unfurl = (el) => {
    close();
    draw(el);
    const s = el.dataset.path || '', full = el.dataset.full || s;
    const c = el._cut || cut(s, s.length);
    const prefix = full.endsWith(s) ? full.slice(0, full.length - s.length) : '';
    if (!c.cut && !prefix) return;
    const w = charW(el), r = el.getBoundingClientRect();
    // Where the ellipsis stands, or the text's left edge: the point the copy unrolls from.
    const ax = r.left + (c.cut ? (c.he - c.hs) * w : 0);
    const o = document.createElement('div');
    o.className = 'cb-unfurl';
    document.body.appendChild(o);
    let left = prefix + s.slice(0, c.hs);   // still hidden, comes in on the left
    let mid = s.slice(c.he, c.ts);          // still hidden, comes in from both sides of the ellipsis
    let L = s.slice(c.hs, c.he), R = s.slice(c.ts);
    const total = left.length + mid.length;
    const per = Math.max(1, Math.ceil(total / 14));
    const pad = 6;
    const nameAt = full.lastIndexOf('/') + 1;
    const render = () => {
      // L is full[left.length : left.length + L.length], R the end of full; the ellipsis stands for what is
      // still hidden between them.
      const a = left.length;
      o.innerHTML = part(full, a, a + L.length, nameAt) + (mid.length ? '<span class="e">…</span>' : '')
        + part(full, full.length - R.length, full.length, nameAt);
      const n = L.length + (mid.length ? 1 : 0) + R.length;
      let x = ax - L.length * w - pad;
      x = Math.max(6, Math.min(x, window.innerWidth - 6 - (n * w + 2 * pad)));
      o.style.left = `${x}px`;
      o.style.top = `${r.top - 2}px`;
    };
    const step = () => {
      if (!live || live.o !== o) return;
      for (let k = 0; k < per; k++) {
        if (mid.length) {
          if (k % 2 === 0) { L += mid[0]; mid = mid.slice(1); }
          else { R = mid[mid.length - 1] + R; mid = mid.slice(0, -1); }
        } else if (left.length) {
          L = left[left.length - 1] + L; left = left.slice(0, -1);
        }
      }
      render();
      if (mid.length || left.length) live.raf = requestAnimationFrame(step);
    };
    live = { o, el, raf: 0, top: r.top, left: r.left };
    render();
    requestAnimationFrame(() => o.classList.add('on'));
    live.raf = requestAnimationFrame(step);
  };
  const seen = new WeakSet();
  const ro = new ResizeObserver((entries) => entries.forEach((e) => draw(e.target)));
  const scan = () => {
    document.querySelectorAll('.cb-path').forEach((el) => {
      if (seen.has(el)) { if (el._shown !== el.dataset.path) draw(el); return; }
      seen.add(el);
      draw(el);
      ro.observe(el);
      el.addEventListener('mouseenter', () => unfurl(el));
      el.addEventListener('mouseleave', close);
    });
    // A path element the page removed while its copy showed takes the copy with it.
    if (live && !live.el.isConnected) close();
  };
  let queued = false;
  new MutationObserver(() => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => { queued = false; scan(); });
  }).observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['data-path'] });
  window.addEventListener('scroll', () => {
    if (!live) return;
    const r = live.el.getBoundingClientRect();
    if (Math.abs(r.top - live.top) > 1 || Math.abs(r.left - live.left) > 1) close();
  }, { passive: true, capture: true });
  window.cbPath = { scan };
  scan();
  document.fonts.ready.then(() => {
    widths.clear();
    document.querySelectorAll('.cb-path').forEach((el) => { el._cap = null; draw(el); });
  });
})();
"""


def display_path(path: str | Path, project: str | Path | None) -> str:
    """What a path shows as: from the project folder when inside it, whole otherwise."""
    text = str(path)
    if project is not None:
        try:
            return str(Path(text).relative_to(Path(project)))
        except ValueError:
            pass
    return text


def absolute_path(path: str | Path, project: str | Path | None) -> str:
    """The path a click copies and the hover ends on: a relative one is taken from the project folder. A folder's
    trailing slash goes, so its own name is the part kept whole."""
    p = Path(str(path).rstrip("/") or "/")
    if not p.is_absolute() and project is not None:
        p = Path(project) / p
    return str(p)


def path_text(path: str | Path, project: str | Path | None = None, *, style: str = "") -> ui.element:
    """A path cut in its middle to the width it is given; a click copies the absolute path, hovering unrolls it.

    The element is a block that fills its parent's width: give it a parent with a width (a grid cell, a flex child
    with `flex: 1 1 0; min-width: 0`). It has no children of its own: the script writes its text."""
    el = ui.element("div").classes("cb-path")
    _point(el, path, project)
    if style:
        el.style(style)
    copy_on_click(el, lambda: el.props["data-full"])
    return el


def set_path(el: ui.element, path: str | Path, project: str | Path | None = None) -> None:
    """Point an existing `path_text` at another path; the script cuts it again and a click copies the new one."""
    _point(el, path, project)
    el.update()


def _point(el: ui.element, path: str | Path, project: str | Path | None) -> None:
    full = absolute_path(path, project)
    el.props["data-path"] = display_path(full, project)
    el.props["data-full"] = full
