"""The prototypes' look: Harbor Mono's variables (with today's values as fallbacks, as everywhere in the app), the
lights and marks every variant shares, and the page's few hover and animation rules. In a build the rules move to
ui/dashboard/css.py; on a dev page they are added with the page."""

from __future__ import annotations

from ui.branch_map.style import tok

MONO = "font-family: var(--cb-mono, 'IBM Plex Mono', monospace);"
SANS = "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif);"

INK = tok("ink-1", "#12324f")
BODY = tok("ink-2", "#1e293b")
SOFT = tok("ink-3", "#5b7089")
MUTED = tok("ink-4", "#94a3b8")
LINE = tok("line", "#e4eaf1")
PANEL_LINE = tok("panel-line", "#d6e0ea")
HEAD = tok("head", "#f2f6fb")
HOVER = tok("hover", "#eaf1f8")
PICK = tok("pick", "#4a8fd4")
GLYPH = "#5b7089"

# A tilt series' mark: its fill and the word its hover says. Waiting in the queue is amber, a job not submitted yet
# leaves its marks grey.
MARK = {
    # A muted green: a magazine is mostly done marks, and the app's status green at that area shouts.
    "ok": ("#5a9c80", "done"),
    "fail": ("var(--cb-bad-dot, #ef4444)", "failed"),
    "running": ("var(--cb-run-dot, #3b82f6)", "running"),
    "pending": ("var(--cb-warn-dot, #f59e0b)", "waiting in the queue"),
    "skip": ("#b8c4d2", "skipped: muted or nothing to do"),
    "unrun": ("var(--cb-wait-dot, #dbe3ec)", "not run yet"),
}
LIGHT = {
    "done": "var(--cb-ok-dot, #10b981)",
    "running": "var(--cb-run-dot, #3b82f6)",
    "failed": "var(--cb-bad-dot, #ef4444)",
    "queued": "var(--cb-que-dot, #60a5fa)",
    "scheduled": "var(--cb-off-dot, #c5d3e2)",
}
STATUS_WORD = {"done": "done", "running": "running", "failed": "failed", "queued": "queued", "scheduled": "not run yet"}


def light(status: str, size: int = 7) -> str:
    """A job's status light: a flat square pip, a ring while queued, blinking while running."""
    color = LIGHT.get(status, LIGHT["scheduled"])
    cls = ' class="cbji-run"' if status == "running" else ""
    if status == "queued":
        return (
            f'<span{cls} style="display:inline-block;width:{size}px;height:{size}px;box-sizing:border-box;'
            f'border:1.5px solid {color};border-radius:var(--cb-pip-radius, 50%);flex-shrink:0;"></span>'
        )
    return (
        f'<span{cls} style="display:inline-block;width:{size}px;height:{size}px;background:{color};'
        f'border-radius:var(--cb-pip-radius, 50%);flex-shrink:0;"></span>'
    )


def tally(counts: dict[str, int], status: str) -> tuple[str, str]:
    """The row's tally, `ok/total`, and its ink: green when all done, red with failures, blue while running."""
    total = sum(counts.values())
    ok = counts.get("ok", 0)
    if status == "scheduled" or not total:
        return f"–/{total}", MUTED
    if counts.get("fail"):
        return f"{ok}/{total}", "var(--cb-bad-ink, #a61b1b)"
    if status in ("running", "queued"):
        return f"{ok}/{total}", "var(--cb-run-ink, #1d4fa0)"
    if ok + counts.get("skip", 0) == total:
        return f"{ok}/{total}", "var(--cb-ok-ink, #0f6b4c)"
    return f"{ok}/{total}", SOFT


def micro_magazine(states: list[str], *, width: int = 64, height: int = 8) -> str:
    """The magazine squeezed into a row: one column per tilt series (two rows when there are many), as SVG."""
    n = len(states)
    if not n:
        return ""
    rows = 2 if n > 40 else 1
    per = -(-n // rows)
    bw = width / per
    bh = (height - (rows - 1)) / rows
    parts = []
    for i, s in enumerate(states):
        x, y = (i % per) * bw, (i // per) * (bh + 1)
        fill = MARK.get(s, MARK["unrun"])[0]
        cls = ' class="cbji-run"' if s == "running" else ""
        parts.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{max(bw - 0.6, 0.6):.2f}" height="{bh:.2f}" fill="{fill}"{cls}/>'
        )
    return f'<svg width="{width}" height="{height}" style="display:block;flex-shrink:0;">{"".join(parts)}</svg>'


CSS = """
@keyframes cbji-pulse { 0%, 100% { opacity: 1; } 50% { opacity: .32; } }
.cbji-run { animation: cbji-pulse 1.3s ease-in-out infinite; }
.cbji-row { cursor: pointer; }
.cbji-row:hover { background: var(--cb-hover, #eaf1f8); }
.cbji-row .cbji-x { opacity: 0; }
.cbji-row:hover .cbji-x { opacity: .7; }
.cbji-row .cbji-x:hover { opacity: 1; color: #a61b1b !important; }
.cbji-tally { border-radius: 2px; padding: 0 3px; }
.cbji-tally:hover { background: #dbe7f3; box-shadow: inset 0 0 0 1px #a9c6e4; }
.cbji-ghost { cursor: pointer; }
.cbji-ghost .cbji-ghost-add { opacity: 0; }
.cbji-ghost:hover { background: var(--cb-hover, #eaf1f8); }
.cbji-ghost:hover .cbji-ghost-add { opacity: 1; }
.cbji-add { cursor: pointer; color: #94a3b8; border-radius: 2px; }
.cbji-add:hover { color: #12324f !important; background: #dbe7f3; }
.cbji-band:hover .cbji-hover-add { opacity: 1 !important; }
.cbji-band:hover .cbji-hover-glyph { opacity: 0 !important; }
.cbji-mag [data-ts] { cursor: pointer; }
.cbji-mag [data-ts]:hover { outline: 1.5px solid #12324f; outline-offset: 1px; }
.cbji-mag [data-ts].cbji-on { outline: 1.5px solid #12324f; outline-offset: 1px; }
.cbji-node { cursor: pointer; }
.cbji-node:hover { filter: brightness(.94); }
.cbji-seg { cursor: pointer; }
.cbji-x-btn { cursor: pointer; color: #8aa0b8; }
.cbji-x-btn:hover { color: #12324f; }
.cbji-link { cursor: pointer; color: var(--cb-ink-3, #5b7089); }
.cbji-link:hover { color: #12324f; text-decoration: underline; }
.cbji-pop { animation: cbji-in .09s ease-out; }
@keyframes cbji-in { from { opacity: 0; transform: translateX(-3px); } to { opacity: 1; transform: none; } }

/* Toasts (section D): rules over every ui.notify, by class. `body` in front outranks the app's own toast rule
 * (ui/dashboard/css.py), which ties with a plain `.q-notification.x` and then wins or loses on load order. The kind
 * (its word and its pip) comes from Quasar's type class, so no call site changes. */
body .q-notification.cbji-t { --cbji-pip: #94a3b8; --cbji-kind: "Note"; }
body .q-notification.cbji-t.bg-positive { --cbji-pip: #10b981; --cbji-kind: "Done"; }
body .q-notification.cbji-t.bg-negative { --cbji-pip: #ef4444; --cbji-kind: "Error"; }
body .q-notification.cbji-t.bg-warning { --cbji-pip: #f59e0b; --cbji-kind: "Warning"; }
body .q-notification.cbji-t.bg-info { --cbji-pip: #4a8fd4; --cbji-kind: "Note"; }
body .q-notification.cbji-t.cbji-k-copied { --cbji-pip: #4a8fd4; --cbji-kind: "Copied"; }
body .q-notification.cbji-t {
    background: #ffffff !important; color: #1e293b !important; border: 1px solid #c9d6e3 !important;
    border-radius: 2px !important; box-shadow: 0 6px 18px rgba(16, 42, 67, .10) !important; padding: 0 !important;
    min-height: 0 !important; font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif) !important;
}
body .q-notification.cbji-t .q-notification__wrapper { min-height: 0; }
body .q-notification.cbji-t .q-notification__content { padding: 0 !important; min-height: 0; }
body .q-notification.cbji-t .q-notification__icon { display: none !important; }
body .q-notification.cbji-t .q-notification__message {
    color: #1e293b !important; font-size: 10.5px !important; line-height: 1.4; white-space: pre-line;
    overflow-wrap: anywhere;
}
body .q-notification.cbji-t .q-notification__message::before {
    content: var(--cbji-kind); color: #12324f; text-transform: uppercase; letter-spacing: .07em;
    font: 500 8.5px var(--cb-mono, 'IBM Plex Mono', monospace);
    background: linear-gradient(var(--cbji-pip), var(--cbji-pip)) 0 50% / 7px 7px no-repeat;
}
/* Quasar's colour classes (.bg-positive {background ... !important}) sit in the `quasar_importants` cascade layer,
 * and for !important a layered rule beats any unlayered one, whatever its specificity: the app's own toast rule loses
 * to them today. NiceGUI declares `theme` first, and for !important the first layer wins: the colours go there. */
@layer theme {
    body .q-notification.cbji-t { background: #ffffff !important; color: #1e293b !important; }
    body .q-notification.cbji-t3 { background: #f2f6fb !important; }
    body .q-notification.cbji-t .q-notification__message { color: #1e293b !important; }
}
/* T1 card: a title stripe with the pip and the kind, the message under it. */
body .q-notification.cbji-t1, body .q-notification.cbji-t2 { min-width: 240px; max-width: 460px !important; }
body .q-notification.cbji-t1 .q-notification__content > div,
body .q-notification.cbji-t1 .q-notification__message { width: 100%; }
body .q-notification.cbji-t1 .q-notification__message { padding: 0 10px 7px !important; }
body .q-notification.cbji-t1 .q-notification__message::before {
    display: block; margin: 0 -10px 6px; padding: 4px 10px 4px 24px; background-color: #f2f6fb;
    background-position: 10px 50%; border-bottom: 1px solid #e4eaf1;
}
/* T2 line: one row as tall as a roster row: the pip, the kind, the message. */
body .q-notification.cbji-t2 .q-notification__message { padding: 4px 10px 4px 9px !important; }
body .q-notification.cbji-t2 .q-notification__message::before {
    display: inline-block; padding-left: 13px; margin-right: 9px; line-height: 14px;
}
/* T3 status bar: the line across the foot of the window, in the title-stripe tint. */
body .q-notification.cbji-t3 {
    background: #f2f6fb !important; border: none !important; border-top: 1px solid #d6e0ea !important;
    border-radius: 0 !important; box-shadow: none !important; margin: 0 !important; width: 100vw;
    max-width: 100vw !important;
}
body .q-notification.cbji-t3 .q-notification__message { padding: 4px 14px !important; }
body .q-notification.cbji-t3 .q-notification__message::before {
    display: inline-block; padding-left: 13px; margin-right: 10px; line-height: 14px;
}
.q-notifications__list--bottom:has(.cbji-t3) { margin: 0 !important; padding: 0 !important; bottom: 0 !important; }
/* T0: a copy says so on the path itself: the path flashes, a COPIED tag sits at its end, nothing floats. */
.cbji-copied-flash { animation: cbji-flash .9s ease-out; }
@keyframes cbji-flash { 0% { background: #dbe7f3; } 100% { background: transparent; } }
.cbji-copied {
    position: fixed; z-index: 9999; pointer-events: none; padding: 0 4px; line-height: 13px; border-radius: 2px;
    background: #ffffff; border: 1px solid #b5e3cd; color: #0f6b4c;
    font: 500 8px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase; letter-spacing: .06em;
    animation: cbji-tag 1.1s ease-out forwards;
}
@keyframes cbji-tag { 0% { opacity: 0; } 12% { opacity: 1; } 80% { opacity: 1; } 100% { opacity: 0; } }
"""
