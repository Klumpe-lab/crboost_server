"""Header ladders, long paths and checkboxes for the job-page prototypes.

A ladder is the set of headers one look draws, from the most to the least important:

    page     the job's own title line
    section  Parameters, Inputs & Outputs, SLURM resources, Tasks, Files
    accent   a section that asks to be looked at (a failed job's Log, Results)
    group    a group inside a section: Angles & offsets, Reference & mask

today  Harbor Mono now: the pale blue-grey band, the 2 px accent on its left, the hairline under it.
rule   Oxide's console: mono caps title, a hairline running on from it to the right edge; no fill, no corners.
tab    the title in a flat inverted tab sitting on a full-width ink rule.
band   a flat band, square, ruled in ink along its top instead of on its left.

The look is picked per panel (`use`), so one page can show every ladder side by side.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from nicegui import ui

from ui.components.copyable import copy_on_click
from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE

MONO = "font-family: var(--cb-mono, 'IBM Plex Mono', monospace);"
SANS = "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif);"
INK_1 = "#12324f"  # --cb-ink-1: titles
INK_2 = "#1e293b"
INK_3 = "#5b7089"
INK_4 = "#94a3b8"
RULE = "#d6e0ea"  # --cb-panel-line
ACCENT = "#2f6fb3"  # one step darker than --cb-pick, so white text on it reads
ACCENT_SOFT = "#a9c6e4"
CAPS = "text-transform: uppercase; letter-spacing: .09em;"

LOOKS = ("today", "rule", "tab", "band")
_look = {"name": "today"}


def use(name: str) -> None:
    _look["name"] = name if name in LOOKS else "today"


def look() -> str:
    return _look["name"]


def _meta(text: str) -> None:
    if text:
        ui.label(text).style(f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; margin-left: auto; white-space: nowrap;")


def _head(title: str, right: str, accent: bool) -> None:
    name = look()
    if name == "rule":
        with ui.element("div").style("display: flex; align-items: center; gap: 8px; min-height: 18px;"):
            if accent:
                ui.element("div").style(f"width: 6px; height: 6px; background: {ACCENT}; flex-shrink: 0;")
            ui.label(title).style(
                f"{MONO} font-size: 9.5px; font-weight: 600; {CAPS} color: {ACCENT if accent else INK_1}; "
                "white-space: nowrap;"
            )
            ui.element("div").style(f"flex: 1; height: 1px; background: {ACCENT_SOFT if accent else RULE};")
            if right:
                ui.label(right).style(f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; white-space: nowrap;")
        return
    if name == "tab":
        ink = ACCENT if accent else INK_1
        with ui.element("div").style(
            f"display: flex; align-items: flex-end; gap: 8px; border-bottom: 1px solid {ink}; min-height: 18px;"
        ):
            ui.label(title).style(
                f"{MONO} font-size: 8.5px; font-weight: 500; {CAPS} color: #fff; background: {ink}; "
                "padding: 3px 8px 2px; line-height: 11px; white-space: nowrap;"
            )
            if right:
                ui.label(right).style(
                    f"{MONO} font-size: 8.5px; {CAPS} color: {INK_4}; margin-left: auto; padding-bottom: 2px; "
                    "white-space: nowrap;"
                )
        return
    if name == "band":
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 8px; padding: 4px 8px; min-height: 22px; "
            "box-sizing: border-box; "
            f"background: {'#eef4fb' if accent else '#f5f8fb'}; "
            f"border-top: {'2px solid ' + ACCENT if accent else '1px solid ' + INK_1};"
        ):
            ui.label(title).style(
                f"{MONO} font-size: 9px; font-weight: 600; {CAPS} color: {ACCENT if accent else INK_1}; "
                "white-space: nowrap;"
            )
            _meta(right)
        return
    # today
    accent_css = f"box-shadow: 0 1px 0 #dbe3ec, inset 2px 0 0 {'#d97706' if accent else '#4a8fd4'};"
    head_css = f"{PANEL_HEAD_STYLE} {accent_css if accent else ''} display: flex; align-items: center;"
    with ui.element("div").style(head_css):
        ui.label(title).style(PANEL_TITLE_STYLE)
        if right:
            ui.label(right).style(f"{SANS} font-size: 9.5px; color: {INK_4}; margin-left: auto;")


@contextmanager
def section(title: str, *, right: str = "", accent: bool = False, extra: str = ""):
    """A section: its header, then its content, one box deep."""
    if look() == "today":
        box = ui.column().classes("w-full cbjp-sec").style(f"gap: 6px; {PANEL_STYLE} {extra}")
    else:
        box = ui.column().classes("w-full cbjp-sec").style(f"gap: 7px; {extra}")
    with box:
        _head(title, right, accent)
        yield box


def group_title(text: str) -> None:
    """The header of a group inside a section: one step under the section's, never a box."""
    name = look()
    if name == "rule":
        ui.label(text).style(f"{SANS} font-size: 10.5px; font-weight: 600; color: {INK_2}; margin: 3px 0 2px;")
    elif name == "tab":
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; margin: 3px 0 2px;"):
            ui.element("div").style(f"width: 5px; height: 5px; border: 1px solid {INK_1}; flex-shrink: 0;")
            ui.label(text).style(f"{MONO} font-size: 8.5px; font-weight: 600; {CAPS} color: {INK_1};")
    elif name == "band":
        ui.label(text).style(
            f"{SANS} font-size: 10.5px; font-weight: 600; color: {INK_1}; margin: 3px 0 2px; padding-bottom: 2px; "
            f"border-bottom: 1px dotted {RULE};"
        )
    else:
        ui.label(text).style(f"{SANS} font-size: 10.5px; font-weight: 600; color: #0f172a; margin: 2px 0 3px;")


def page_title(text: str, meta: str = "") -> None:
    """The specimen's page level: the job's own title line, as large in every look."""
    with ui.element("div").style("display: flex; align-items: baseline; gap: 8px;"):
        ui.label(text).style(f"{SANS} font-size: 13px; font-weight: 600; color: {INK_1};")
        if meta:
            ui.label(meta).style(f"{MONO} font-size: 9.5px; color: {INK_4};")


# ── long paths ───────────────────────────────────────────────────────────────
#
# A path is cut in its middle, never at its end: as much of its start as fits, an ellipsis, then its last part
# (`/name`) whole. Only when the name alone does not fit is the name itself cut in its middle, its end (the extension)
# kept. The cut is made in the browser, in characters (the face is monospaced), and made again whenever the element's
# width changes. A click copies the whole path. Hovering lays a lit copy over it that unrolls: the hidden middle comes
# back a few characters a frame from both sides of the ellipsis, and the project folder in front of a relative path
# comes in on the left, until the whole absolute path shows.

MPATH_CSS = """
.cbjp-mpath { display: block; width: 100%; min-width: 0; overflow: hidden; white-space: nowrap; cursor: pointer;
  font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; line-height: 14px; color: #5b7089; }
.cbjp-mpath .n { color: #1e293b; }
.cbjp-mpath .e { color: #94a3b8; }
.cbjp-unfurl { position: fixed; z-index: 9000; pointer-events: none; white-space: pre; padding: 2px 6px;
  font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; line-height: 14px; color: #94a3b8;
  background: #0f1d2e; border-radius: 2px;
  box-shadow: 0 0 0 1px rgba(74, 143, 212, 0.55), 0 0 16px 2px rgba(74, 143, 212, 0.35);
  opacity: 0; transition: opacity 80ms ease-out; }
.cbjp-unfurl.on { opacity: 1; }
.cbjp-unfurl .n { color: #ffffff; }
.cbjp-unfurl .e { color: #5b7089; }
"""

MPATH_JS = r"""
(() => {
  if (window.cbMpath) { window.cbMpath.scan(); return; }
  // One character's width, measured in the element itself (the face may load after the first cut: the widths are
  // measured again, and every path cut again, once the page's fonts are ready).
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
    const w = charW(el);
    const cap = Math.floor(el.clientWidth / w);
    if (cap === el._cap && el.firstChild) return;
    el._cap = cap;
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
    const s = el.dataset.path || '', full = el.dataset.full || s;
    const c = el._cut || cut(s, s.length);
    const prefix = full.endsWith(s) ? full.slice(0, full.length - s.length) : '';
    if (!c.cut && !prefix) return;
    const w = charW(el), r = el.getBoundingClientRect();
    // Where the ellipsis stands, or the text's left edge: the point the copy unrolls from.
    const ax = r.left + (c.cut ? (c.he - c.hs) * w : 0);
    const o = document.createElement('div');
    o.className = 'cbjp-unfurl';
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
    document.querySelectorAll('.cbjp-mpath').forEach((el) => {
      if (seen.has(el)) return;
      seen.add(el);
      draw(el);
      ro.observe(el);
      el.addEventListener('mouseenter', () => unfurl(el));
      el.addEventListener('mouseleave', close);
    });
  };
  let queued = false;
  new MutationObserver(() => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => { queued = false; scan(); });
  }).observe(document.body, { childList: true, subtree: true });
  // Only a scroll that moves the hovered path closes its copy: a log scrolling elsewhere does not, nor a scroll
  // event that moved nothing.
  window.addEventListener('scroll', () => {
    if (!live) return;
    const r = live.el.getBoundingClientRect();
    if (Math.abs(r.top - live.top) > 1 || Math.abs(r.left - live.left) > 1) close();
  }, { passive: true, capture: true });
  window.cbMpath = { scan };
  scan();
  document.fonts.ready.then(() => {
    widths.clear();
    document.querySelectorAll('.cbjp-mpath').forEach((el) => { el._cap = null; draw(el); });
  });
})();
"""


def display_path(path_: str, project: Path | None) -> str:
    """What a path shows as: from the project folder when inside it, whole otherwise."""
    if project is not None:
        try:
            return str(Path(path_).relative_to(project))
        except ValueError:
            pass
    return path_


def mpath(path_: str, project: Path | None) -> ui.element:
    """A path cut in its middle to the width it is given; a click copies it, hovering unrolls it whole."""
    el = ui.element("div").classes("cbjp-mpath")
    el.props["data-path"] = display_path(path_, project)
    el.props["data-full"] = path_
    copy_on_click(el, path_)
    return el


# ── checkboxes ───────────────────────────────────────────────────────────────

CHECK_CSS = """
.cbjp-hcheck { display: inline-flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none; }
.cbjp-hcheck .b { position: relative; width: 10px; height: 10px; box-sizing: border-box; border: 1px solid #5b7089;
  border-radius: 1px; flex-shrink: 0; display: inline-block; }
/* The tick is out of flow, so the box holds no text and its baseline is its bottom edge. */
.cbjp-hcheck.on .b { background: #334155; border-color: #334155; }
.cbjp-hcheck.on .b::before { content: '✓'; position: absolute; inset: 0; color: #fff; font-size: 8px; line-height: 8px;
  text-align: center; }
.cbjp-hcheck .t { font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif); font-size: 10px; line-height: 14px;
  color: #334155; }
/* The F panel's guide: a red line along the bottom of each box, out under its label. */
.cbjp-guide .q-checkbox__bg::after, .cbjp-guide .b::after { content: ''; position: absolute; left: -2px;
  bottom: -1px; width: 150px; border-top: 1px solid rgba(220, 38, 38, 0.6); }
"""


def house_check(label: str, on: bool) -> None:
    """A checkbox drawn by us: a 10 px box whose bottom edge is its label's baseline (a box with no text has its
    bottom as its baseline, and the row aligns baselines), so the text sits on the box's line, not above it."""
    el = ui.html(f"<span class='b'></span><span class='t'>{label}</span>", sanitize=False).classes(
        f"cbjp-hcheck {'on' if on else ''}"
    )
    el.on("click", js_handler="(e) => e.currentTarget.classList.toggle('on')")
