"""The look and the browser half of /dev/map-ideas. Everything the cards need is a class here, so a card is a short
HTML string; the cables, the lineage, the find bar and the walk by arrow keys run in the browser over the graph the
page sends once (`cbmi.setGraph`), so selecting, finding and drawing cost the server nothing."""

CSS = r"""
.cbmi-page { padding: 18px 26px 60px; display: flex; flex-direction: column; gap: 8px; width: 100%;
  box-sizing: border-box; align-self: stretch; }
.cbmi-h1 { font: 600 16px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--cb-ink-1, #0f172a); }
.cbmi-meta { font: 400 9.5px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--cb-ink-2, #334155); }
.cbmi-note { font: 400 10.5px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--cb-ink-3, #64748b);
  line-height: 1.45; max-width: 1100px; }
.cbmi-note b { color: var(--cb-ink-2, #334155); font-weight: 600; }
.cbmi-controls { display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: center; }
.cbmi-ctl { display: flex; align-items: center; gap: 6px; }
.cbmi-ctl-name { font: 400 8.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase;
  letter-spacing: .08em; color: var(--cb-ink-3, #64748b); }
.cbmi-seg { display: inline-flex; border: 1px solid var(--cb-btn-line, #b8cce0); border-radius: var(--cb-r-sm, 3px);
  overflow: hidden; }
.cbmi-seg > * { font: 400 8.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase;
  letter-spacing: .07em; padding: 2px 8px; line-height: 14px; color: var(--cb-ink-3, #64748b); background: #fff;
  cursor: pointer; user-select: none; }
.cbmi-seg > * + * { border-left: 1px solid var(--cb-btn-line, #b8cce0); }
.cbmi-seg > .on { color: var(--cb-ink-1, #0f172a); background: var(--cb-rail-on, #dbe7f3); }
.cbmi-seg > :hover:not(.on) { background: #f1f5f9; }

/* The frame: the roster and the Pipeline view, as the workspace shows them. */
.cbmi-frame { display: flex; gap: 10px; height: 78vh; min-height: 560px;
  --acc: var(--cb-accent, #4a8fd4); --ink1: var(--cb-ink-1, #0f172a); --ink2: var(--cb-ink-2, #334155);
  --ink3: var(--cb-ink-3, #64748b); --ink4: var(--cb-ink-4, #94a3b8); --hair: var(--cb-bm-hair, #e9eef3);
  --band: var(--cb-bm-band, #f8fafc); --cline: var(--cb-bm-card-line, #d6dee8); --cable: var(--cb-bm-cable, #94a3b8);
  --port: var(--cb-bm-port, #64748b); }
.cbmi-panel { background: #fff; border: 1px solid var(--cb-line, #e2e8f0); border-radius: var(--cb-r-md, 6px);
  display: flex; flex-direction: column; min-height: 0; min-width: 0; }
.cbmi-panel-head { display: flex; align-items: center; gap: 8px; height: 30px; flex: 0 0 auto; padding: 0 10px;
  border-bottom: 1px solid var(--hair); }
.cbmi-panel-title { font: 600 12px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink1); }
.cbmi-headtext { font: 400 9.5px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink3); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; }
#cbmi-linchip:not(:empty) { color: var(--ink1); background: var(--cb-head, #edf4fb); padding: 1px 6px;
  border-radius: 2px; box-shadow: inset 2px 0 0 var(--acc); }
.cbmi-roster { width: 268px; flex: 0 0 268px; }
.cbmi-roster .cbmi-rlist-wrap { flex: 1 1 0; overflow-y: auto; min-height: 0; }
.cbmi-rgroup { display: flex; gap: 6px; align-items: baseline; padding: 8px 10px 2px;
  font: 400 8.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase; letter-spacing: .07em;
  color: var(--ink3); }
.cbmi-rgroup span { color: var(--ink4); }
.cbmi-rrow { display: flex; align-items: center; gap: 7px; height: 21px; padding: 0 10px 0 12px; cursor: pointer;
  transition: opacity .15s; }
.cbmi-rrow:hover { background: #f1f5f9; }
.cbmi-rrow .t { font: 400 11px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink1); flex: 1 1 0;
  min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cbmi-rrow .n { font: 400 9.5px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink4); }
.cbmi-rrow .s { font: 400 7.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase;
  letter-spacing: .05em; color: var(--ink3); border: 1px solid var(--cb-tag-line, #cfd8e3); border-radius: 2px;
  padding: 0 3px; max-width: 74px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cbmi-pipe { flex: 1 1 0; }
.cbmi-scroll { flex: 1 1 0; min-height: 0; overflow: auto; position: relative;
  border-radius: 0 0 var(--cb-r-md, 6px) var(--cb-r-md, 6px);
  background: var(--cb-bm-paper, #f1f4f8);
  background-image: radial-gradient(var(--cb-bm-paper-dot, #c3cbd6) 1px, transparent 1.2px);
  background-size: 16px 16px; }
.cbmi-world { position: relative; width: 100%; min-height: 100%; box-sizing: border-box; padding: 26px 30px 70px; }
.cbmi-cables { position: absolute; left: 0; top: 0; pointer-events: none; z-index: 0; line-height: 0; }
.cbmi-cables svg { overflow: visible; }
.cbmi-cables path { fill: none; stroke: var(--cable); stroke-width: 1.5; transition: opacity .15s; }
.cbmi-cables path.k-side { stroke-dasharray: 4 3; stroke-width: 1.2; opacity: .6; }
.cbmi-frame[data-side="sel"] .cbmi-cables path.k-side:not(.up):not(.down) { display: none; }

/* The layouts. */
.cbmi-forest { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 40px 52px; }
.cbmi-sub { display: flex; flex-direction: column; align-items: center; }
.cbmi-kids { display: flex; align-items: flex-start; gap: 26px; margin-top: 38px; }
.cbmi-lanes .cbmi-sub { align-items: flex-start; }
.cbmi-lanes .cbmi-kids { gap: 30px; margin-top: 34px; }
.cbmi-headrow { display: flex; align-items: flex-start; gap: 26px; }
.cbmi-stack { position: relative; display: flex; flex-direction: column; gap: 10px; padding-left: 16px; }
.cbmi-layers { display: flex; flex-direction: column; gap: 42px; }
.cbmi-layer { display: flex; flex-wrap: wrap; justify-content: center; align-items: flex-start; gap: 18px 26px; }
.cbmi-grid { display: grid; grid-template-columns: repeat(auto-fill, 236px); grid-auto-flow: row dense;
  justify-content: center; gap: 22px 18px; }
.cbmi-grid .cbmi-node.large { grid-column: span 2; }
.cbmi-grid .cbmi-node.large .cbmi-card { width: 100%; }
.cbmi-band { display: flex; align-items: center; gap: 10px; margin: 6px 0 16px; }
.cbmi-band + * { margin-bottom: 30px; }
.cbmi-band .t { font: 400 9.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase;
  letter-spacing: .08em; color: var(--ink1); white-space: nowrap; }
.cbmi-band .r { flex: 1 1 auto; border-top: 1px solid var(--cb-rule, #cfd8e3); }
.cbmi-band .m { font: 400 9px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink3); white-space: nowrap; }
.cbmi-grid-wrap .cbmi-cables path:not(.up):not(.down) { display: none; }

/* A node: the escort above the card (a set's tab, the magazine), then the card. */
.cbmi-node { position: relative; z-index: 1; display: flex; flex-direction: column; align-items: flex-start;
  transition: opacity .15s; }
.cbmi-strip { line-height: 0; margin-bottom: 6px; }
.cbmi-tab { display: flex; align-items: center; gap: 5px; height: 16px; padding: 0 7px; margin: 0 0 -1px 10px;
  background: #fff; border: 1px solid var(--cline); border-bottom: none; border-radius: 4px 4px 0 0;
  font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink2); max-width: 210px;
  white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; position: relative; z-index: 2; }
.cbmi-tab .k, .cbmi-src .k { font: 400 7.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase;
  letter-spacing: .07em; color: var(--ink4); }
.cbmi-src { display: flex; flex-direction: column; gap: 1px; padding: 5px 12px 6px; min-width: 150px;
  max-width: 236px;
  background: #fff; border: 1px dashed var(--port); border-radius: 14px; position: relative; cursor: pointer;
  transition: opacity .15s, outline-color .15s; }
.cbmi-src .cbmi-title { font-size: 11px; }
.cbmi-src .d { font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink3); white-space: nowrap; }
.cbmi-src::after { content: ""; position: absolute; left: calc(50% - 4px); bottom: -5px; width: 8px; height: 8px;
  box-sizing: border-box; background: #fff; border: 2px solid var(--port); border-radius: 2px; }

/* The card: the branch map's small card; large, its parameters and results under the inputs. */
.cbmi-card { position: relative; width: 236px; box-sizing: border-box; background: #fff;
  border: 1px solid var(--cline);
  border-radius: var(--cb-bm-card-radius, 8px); padding-bottom: 6px; cursor: pointer;
  box-shadow: var(--cb-bm-card-shadow, 0 1px 2px rgba(15,23,42,0.05), 0 6px 16px rgba(15,23,42,0.05));
  transition: opacity .15s, box-shadow .15s, outline-color .15s; }
.cbmi-card:hover { box-shadow: var(--cb-bm-card-hover, 0 1px 2px rgba(15,23,42,0.06),
  0 10px 24px rgba(15,23,42,0.10)); }
.cbmi-node.large .cbmi-card { width: 600px; }
.cbmi-card.off { opacity: .55; }
.cbmi-card::before, .cbmi-card::after { content: ""; position: absolute; left: calc(50% - 4px); width: 8px;
  height: 8px;
  box-sizing: border-box; background: #fff; border: 2px solid var(--port); border-radius: 2px; z-index: 3; }
.cbmi-card::before { top: -5px; }
.cbmi-card::after { bottom: -5px; }
.cbmi-card.root::before { display: none; }
.cbmi-stack .cbmi-card::before { left: -5px; top: 9px; }
.cbmi-head { display: flex; align-items: center; height: 26px; padding: 0 9px 0 13px; background: var(--band);
  border-bottom: 1px solid var(--hair); border-radius: 7px 7px 0 0; box-sizing: border-box; }
.cbmi-light { width: 8px; height: 8px; flex: 0 0 auto; border-radius: var(--cb-pip-radius, 50%);
  background: var(--cb-off-dot, #cbd5e1); margin-right: 7px; box-sizing: border-box; }
.cbmi-light.done { background: var(--cb-ok-dot, #10b981); }
.cbmi-light.running { background: var(--cb-run-dot, #3b82f6);
  animation: var(--cb-run-blink, cb-bm-pulse 1.6s ease-out infinite); }
.cbmi-light.failed { background: var(--cb-bad-dot, #ef4444); }
.cbmi-light.queued { background: var(--cb-que-dot, #60a5fa); }
.cbmi-title { font: 600 12px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink1); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; letter-spacing: -0.01em; min-width: 0; flex: 0 1 auto;
  margin-right: 7px; }
.cbmi-badge { flex: 0 0 auto; height: 14px; line-height: 12px; padding: 0 5px; box-sizing: border-box;
  border: 1px solid var(--cb-tag-line, #cfd8e3); background: var(--cb-tag, #f1f5f9);
  border-radius: var(--cb-tag-radius, 3px);
  font-family: var(--cb-tag-font, 'IBM Plex Sans', sans-serif); font-size: var(--cb-tag-size, 9px);
  font-weight: var(--cb-tag-weight, 600); text-transform: var(--cb-tag-case, none);
  letter-spacing: var(--cb-tag-track, 0.01em);
  color: var(--cb-tag-ink, var(--ink2)); white-space: nowrap; }
.cbmi-fill { flex: 1 1 0; }
.cbmi-num { flex: 0 0 auto; font: 400 10px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink4);
  margin-left: 4px; }
.cbmi-size { font-size: 11px !important; color: var(--ink4); margin-left: 6px; opacity: .3; cursor: pointer;
  transition: opacity .12s; }
.cbmi-card:hover .cbmi-size { opacity: 1; }
.cbmi-size:hover { color: var(--ink1); }
.cbmi-ltag { position: absolute; right: 8px; bottom: -13px; pointer-events: none; white-space: nowrap;
  font: 400 7.5px var(--cb-mono, 'IBM Plex Mono', monospace); text-transform: uppercase; letter-spacing: .07em; }
.cbmi-row { position: relative; display: flex; align-items: center; gap: 6px; height: 17px; padding: 0 10px 0 27px; }
.cbmi-lbl { width: 70px; flex: 0 0 auto; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  font-family: var(--cb-label-font, 'IBM Plex Sans', sans-serif); font-size: var(--cb-label-size, 8px);
  font-weight: var(--cb-label-weight, 600); text-transform: var(--cb-label-case, uppercase);
  letter-spacing: var(--cb-label-track, 0.05em); color: var(--cb-label-ink, var(--ink4)); }
.cbmi-val { flex: 1 1 0; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  font: 400 10px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink1); }
.cbmi-val.v-explicit { color: var(--cb-pick, #2563eb); font-weight: 600; }
.cbmi-val.v-named { color: var(--ink2); }
.cbmi-val.v-ambiguous { color: var(--cb-warn-dot, #d97706); font-weight: 600; }
.cbmi-val.v-missing { color: var(--cb-bad-dot, #ef4444); }
.cbmi-val.v-optional { color: var(--ink4); }
.cbmi-row.side::before { content: ""; position: absolute; left: -5px; top: 4.5px; width: 8px; height: 8px;
  box-sizing: border-box; background: var(--port); border: 2px solid var(--port); border-radius: 2px; }
.cbmi-foot { padding: 3px 10px 0 27px; font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink3);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cbmi-sec { margin: 6px 12px 0; padding-top: 5px; border-top: 1px solid var(--hair); }
.cbmi-sec-h { font: 400 8px var(--cb-mono, 'IBM Plex Mono', monospace); letter-spacing: .06em; color: var(--ink4);
  margin-bottom: 4px; }
.cbmi-params { display: grid; grid-template-columns: 1fr 1fr; column-gap: 14px; }
.cbmi-params .p { display: flex; gap: 8px; height: 17px; align-items: center; min-width: 0; }
.cbmi-params .p.wide { grid-column: 1 / -1; }
.cbmi-params .pl { width: 96px; flex: 0 0 auto; font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif);
  color: var(--ink3); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cbmi-params .pv { font: 400 10px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink1); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; min-width: 0; }
.cbmi-tiles { display: flex; gap: 20px; margin-bottom: 4px; }
.cbmi-tiles .tl { display: block; font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink3); }
.cbmi-tiles .tv { display: block; font: 400 11px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--ink1); }
.cbmi-pics { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 3px; }
.cbmi-pics img { height: 88px; width: auto; max-width: 100%; border-radius: var(--cb-img-radius, 5px);
  display: block; }
.cbmi-pics .arrow { color: var(--ink4); font-size: 12px; }
.cbmi-cap { font: 400 9px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: var(--ink3); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; line-height: 13px; }
.cbmi-cap.bad { color: var(--cb-bad-dot, #ef4444); }
.cbmi-plot { line-height: 0; margin: 2px 0; }

/* Selected, and its lineage: three ways to show it. */
.cbmi-node[data-l="sel"] .cbmi-card, .cbmi-node[data-l="sel"] .cbmi-src { outline: 2px solid var(--acc);
  outline-offset: 2px; }
.cbmi-rrow[data-l="sel"] { background: var(--cb-head, #edf4fb); box-shadow: inset 2px 0 0 var(--acc); }
.cbmi-frame[data-has-sel] .cbmi-cables path { opacity: .28; }
.cbmi-frame[data-has-sel] .cbmi-cables path.up, .cbmi-frame[data-has-sel] .cbmi-cables path.down { opacity: 1;
  stroke-width: 2.2; }
.cbmi-cables path.up { stroke: var(--ink2); }
.cbmi-cables path.down { stroke: var(--acc); }
.cbmi-frame[data-has-sel] .cbmi-cables path.k-side.up, .cbmi-frame[data-has-sel] .cbmi-cables path.k-side.down {
  stroke-width: 1.3; opacity: .85; }
.cbmi-frame[data-has-sel] .cbmi-cables path.k-side.up { stroke: var(--ink3); }

.cbmi-frame[data-lin="dim"][data-has-sel] .cbmi-node:not([data-l]) { opacity: .22; }
.cbmi-frame[data-lin="dim"][data-has-sel] .cbmi-rrow:not([data-l]) { opacity: .35; }
.cbmi-frame[data-lin="dim"] .cbmi-node[data-l="up"] :is(.cbmi-card, .cbmi-src),
.cbmi-frame[data-lin="dim"] .cbmi-node[data-l="down"] :is(.cbmi-card, .cbmi-src) {
  outline: 1px solid color-mix(in srgb, var(--acc) 60%, transparent); outline-offset: 2px; }

.cbmi-frame[data-lin="updown"][data-has-sel] .cbmi-node:not([data-l]) { opacity: .45; }
.cbmi-frame[data-lin="updown"][data-has-sel] .cbmi-rrow:not([data-l]) { opacity: .5; }
.cbmi-frame[data-lin="updown"] .cbmi-node[data-l="up"] :is(.cbmi-card,
  .cbmi-src) { outline: 1.5px solid var(--ink2); outline-offset: 2px; }
.cbmi-frame[data-lin="updown"] .cbmi-node[data-l="down"] :is(.cbmi-card,
  .cbmi-src) { outline: 1.5px dashed var(--acc); outline-offset: 2px; }
.cbmi-frame[data-lin="updown"] .cbmi-node[data-l="up"] .cbmi-ltag::before { content: "↑ input"; color: var(--ink2); }
.cbmi-frame[data-lin="updown"] .cbmi-node[data-l="down"] .cbmi-ltag::before { content: "↓ made from it";
  color: var(--acc); }
.cbmi-frame[data-lin="updown"] .cbmi-rrow[data-l="up"] { box-shadow: inset 2px 0 0 var(--ink2); }
.cbmi-frame[data-lin="updown"] .cbmi-rrow[data-l="down"] { box-shadow: inset 2px 0 0 var(--acc); }
.cbmi-frame[data-lin="updown"] .cbmi-rrow[data-l="up"] .n::after { content: " ↑"; color: var(--ink2); }
.cbmi-frame[data-lin="updown"] .cbmi-rrow[data-l="down"] .n::after { content: " ↓"; color: var(--acc); }

.cbmi-frame[data-lin="trace"][data-has-sel] .cbmi-cables path:not(.up):not(.down) { opacity: .5; }
.cbmi-frame[data-lin="trace"] .cbmi-node[data-l] .cbmi-card { box-shadow: inset 3px 0 0 var(--acc),
  var(--cb-bm-card-shadow, 0 1px 2px rgba(15,23,42,0.05), 0 6px 16px rgba(15,23,42,0.05)); }
.cbmi-frame[data-lin="trace"] .cbmi-node[data-l="sel"] .cbmi-card { box-shadow: inset 3px 0 0 var(--acc);
  outline-width: 2px; }
.cbmi-frame[data-lin="trace"] .cbmi-cables :is(path.up, path.down) { stroke: var(--acc); stroke-width: 2.4;
  stroke-dasharray: 7 5; animation: cbmi-march 0.9s linear infinite; }
.cbmi-frame[data-lin="trace"] .cbmi-rrow[data-l] { box-shadow: inset 2px 0 0 var(--acc); }
@keyframes cbmi-march { to { stroke-dashoffset: -24; } }

/* Find: matches stay, the rest fades; the matched letters are marked where they show. */
.cbmi-frame[data-finding] .cbmi-node:not([data-f]) { opacity: .16; }
.cbmi-frame[data-finding] .cbmi-rrow:not([data-f]) { opacity: .28; }
.cbmi-frame[data-finding] .cbmi-cables path:not(.hit) { opacity: .12; }
.cbmi-node[data-f] :is(.cbmi-card, .cbmi-src) { outline: 1.5px solid var(--acc); outline-offset: 2px; }
.cbmi-node[data-f="cur"] :is(.cbmi-card, .cbmi-src) { outline-width: 2.5px; outline-offset: 3px;
  box-shadow: 0 0 0 7px color-mix(in srgb, var(--acc) 16%, transparent), 0 10px 24px rgba(15,23,42,.10); }
.cbmi-rrow[data-f="cur"] { background: var(--cb-head, #edf4fb); }
::highlight(cbmi-find) { background-color: #fde68a; color: #0f172a; }

/* The find bar: frosted glass over the view, nothing else on the page changes but the highlights. */
.cbmi-find { position: fixed; z-index: 9000; display: none; box-sizing: border-box;
  background: rgba(248, 250, 252, 0.58); -webkit-backdrop-filter: blur(14px) saturate(160%);
  backdrop-filter: blur(14px) saturate(160%); border: 1px solid rgba(148, 163, 184, 0.45); border-radius: 10px;
  box-shadow: 0 18px 50px rgba(15, 23, 42, 0.18), inset 0 1px 0 rgba(255, 255, 255, 0.7); }
.cbmi-find-bar { display: flex; align-items: center; gap: 10px; height: 46px; padding: 0 14px; }
.cbmi-find-bar .material-icons { color: var(--cb-ink-3, #64748b); font-size: 18px; }
.cbmi-find input { all: unset; flex: 1 1 auto; min-width: 0;
  font: 500 15px var(--cb-sans, 'IBM Plex Sans', sans-serif);
  color: #0f172a; caret-color: var(--cb-accent, #4a8fd4); }
.cbmi-find input::placeholder { color: #94a3b8; font-weight: 400; }
.cbmi-find-count { font: 400 10px var(--cb-mono, 'IBM Plex Mono', monospace); color: #475569; white-space: nowrap; }
.cbmi-find-x { cursor: pointer; font-size: 15px !important; opacity: .6; }
.cbmi-find-x:hover { opacity: 1; }
.cbmi-find-list { display: none; border-top: 1px solid rgba(148, 163, 184, 0.35); padding: 4px 0; max-height: 300px;
  overflow-y: auto; }
.cbmi-find[data-mode="palette"] .cbmi-find-list:not(:empty) { display: block; }
.cbmi-find-item { display: flex; align-items: center; gap: 8px; height: 26px; padding: 0 14px; cursor: pointer; }
.cbmi-find-item.on { background: rgba(74, 143, 212, 0.14); }
.cbmi-find-item .t { font: 500 12px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: #0f172a; }
.cbmi-find-item .n { font: 400 10px var(--cb-mono, 'IBM Plex Mono', monospace); color: #64748b; }
.cbmi-find-item .c { font: 400 10px var(--cb-sans, 'IBM Plex Sans', sans-serif); color: #64748b; flex: 1 1 0;
  min-width: 0;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: right; }
.cbmi-find-hint { font: 400 9px var(--cb-mono, 'IBM Plex Mono', monospace); color: #64748b; padding: 0 14px 8px 42px;
  margin-top: -6px; }
.cbmi-find[data-mode="palette"] .cbmi-find-hint { margin-top: 0; padding-top: 6px;
  border-top: 1px solid rgba(148, 163, 184, 0.35); }
.cbmi-find[data-mode="dock"] { border-radius: 8px; }
.cbmi-find[data-mode="dock"] .cbmi-find-bar { height: 34px; padding: 0 10px; gap: 8px; }
.cbmi-find[data-mode="dock"] input { font-size: 13px; }
.cbmi-find[data-mode="dock"] .cbmi-find-hint { display: none; }
.cbmi-perf { font: 400 9.5px var(--cb-mono, 'IBM Plex Mono', monospace); color: var(--cb-ink-2, #334155);
  line-height: 1.6; }
.cbmi-perf td { padding: 0 14px 0 0; white-space: nowrap; }
.cbmi-perf th { text-align: left; font-weight: 400; color: var(--cb-ink-3, #64748b); padding: 0 14px 2px 0; }
"""

JS = r"""
(function () {
  if (window.cbmi) return;
  const M = (window.cbmi = {
    g: null, up: {}, down: {}, mainUp: {}, mainDown: {}, sel: null, lin: 'dim', find: 'spotlight', fit: false,
    side: 'all', q: '', hits: [], cur: -1, open: false,
  });
  const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
  const frame = () => document.querySelector('.cbmi-frame');
  const world = () => document.querySelector('.cbmi-world');
  const q = (s) => (window.CSS && CSS.escape ? CSS.escape(s) : s);
  const attr = (s) => String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  const nodeEl = (nid) => document.querySelector(`.cbmi-world .cbmi-node[data-nid="${q(nid)}"]`);
  const boxOf = (nid) => { const n = nodeEl(nid); return n && (n.querySelector('.cbmi-card, .cbmi-src') || n); };
  const CENTRE = { block: 'center', inline: 'center', behavior: 'smooth' };
  const numOf = (nid) => { const it = M.g && M.g.nodes[nid]; return it && it.num ? parseInt(it.num, 10) : 1e9; };

  M.setGraph = (g) => {
    M.g = g; M.up = {}; M.down = {}; M.mainUp = {}; M.mainDown = {};
    for (const e of g.edges) {
      (M.up[e.b] = M.up[e.b] || []).push(e.a);
      (M.down[e.a] = M.down[e.a] || []).push(e.b);
      if (e.k === 'main') { M.mainUp[e.b] = e.a; (M.mainDown[e.a] = M.mainDown[e.a] || []).push(e.b); }
    }
    if (M.sel && !g.nodes[M.sel]) M.sel = null;
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
  function applyFit(W) {
    W.style.zoom = '';
    if (!M.fit) return;
    const have = W.parentElement.clientWidth, need = W.scrollWidth;
    const z = Math.max(0.45, Math.min(1, have / need));
    if (z < 0.999) W.style.zoom = String(z);
  }
  M.draw = () => {
    const W = world();
    if (!W || !M.g) return;
    const svg = W.querySelector('.cbmi-cables svg');
    if (!svg) return;
    applyFit(W);
    const z = parseFloat(W.style.zoom) || 1;
    const wr = W.getBoundingClientRect();
    svg.setAttribute('width', Math.ceil(W.scrollWidth));
    svg.setAttribute('height', Math.ceil(W.scrollHeight));
    const out = [];
    for (const e of M.g.edges) {
      const A = boxOf(e.a), B = boxOf(e.b);
      if (!A || !B) continue;
      const a = rel(A, wr, z), b = rel(B, wr, z);
      const x0 = a.x + a.w / 2, y0 = a.y + a.h;
      let d;
      const stack = e.k === 'main' && B.closest('.cbmi-stack');
      if (stack && stack.classList.contains('cbmi-beside')) {
        // Beside its parent: out of the parent's right edge, along the stack's left edge to the card's header.
        const S = rel(stack, wr, z);
        const xa = a.x + a.w, ya = a.y + 13, xb = S.x + 3, yi = b.y + 13;
        d = `M ${xa} ${ya} L ${xb} ${ya} L ${xb} ${yi} L ${b.x} ${yi}`;
      } else if (stack) {
        // Into a stack: down to the stack, then along its left edge to the card's header.
        const S = rel(stack, wr, z);
        const xb = S.x + 3, ym = S.y - 12, yi = b.y + 13, k = Math.max(8, (ym - y0) / 2);
        d = `M ${x0} ${y0} C ${x0} ${y0 + k}, ${xb} ${ym - k}, ${xb} ${ym} `
          + `L ${xb} ${yi - 5} Q ${xb} ${yi} ${xb + 5} ${yi} L ${b.x} ${yi}`;
      } else if (e.k === 'side') {
        const row = B.querySelector(`.cbmi-row[data-slot="${q(e.s)}"]`);
        const r = row ? rel(row, wr, z) : { y: b.y, h: 26 };
        const x1 = b.x, y1 = r.y + r.h / 2;
        if (y1 > y0 + 16) {
          const k = Math.max(26, (y1 - y0) * 0.5);
          d = `M ${x0} ${y0} C ${x0} ${y0 + k}, ${x1 - 52} ${y1}, ${x1} ${y1}`;
        } else {
          // The reader sits beside or above: out of the left edge, round the left, into the row's port.
          const xa = a.x, ya = a.y + 13, xl = Math.min(xa, x1) - 46;
          d = `M ${xa} ${ya} C ${xl} ${ya}, ${xl} ${y1}, ${x1} ${y1}`;
        }
      } else if (b.y >= y0 + 8) {
        const x1 = b.x + b.w / 2, y1 = b.y, k = Math.max(14, (y1 - y0) / 2);
        d = `M ${x0} ${y0} C ${x0} ${y0 + k}, ${x1} ${y1 - k}, ${x1} ${y1}`;
      } else {
        // The reader sits beside or above: out of the right edge, into the left edge of its header.
        const xa = a.x + a.w, ya = a.y + 13, xb = b.x, yb = b.y + 13, k = Math.max(24, Math.abs(xb - xa) / 2);
        d = `M ${xa} ${ya} C ${xa + k} ${ya}, ${xb - k} ${yb}, ${xb} ${yb}`;
      }
      out.push(`<path d="${d}" class="k-${e.k}" data-a="${attr(e.a)}" data-b="${attr(e.b)}"/>`);
    }
    svg.innerHTML = out.join('');
    M.paint();
  };

  // ── selection, lineage, matches: attributes the stylesheet reads ──
  M.paint = () => {
    const F = frame();
    if (!F) return;
    F.dataset.lin = M.lin;
    F.dataset.side = M.side;
    const sel = M.sel;
    const up = sel ? reach(sel, M.up) : new Set(), down = sel ? reach(sel, M.down) : new Set();
    F.toggleAttribute('data-has-sel', !!sel);
    const finding = M.q.trim().length > 0;
    F.toggleAttribute('data-finding', finding);
    const hits = new Set(M.hits), cur = M.hits[M.cur];
    for (const el of $$('[data-nid]', F)) {
      const n = el.dataset.nid;
      const l = n === sel ? 'sel' : up.has(n) ? 'up' : down.has(n) ? 'down' : '';
      if (l) el.dataset.l = l; else delete el.dataset.l;
      if (finding && hits.has(n)) el.dataset.f = n === cur ? 'cur' : 'hit'; else delete el.dataset.f;
    }
    for (const p of $$('.cbmi-cables path', F)) {
      const a = p.dataset.a, b = p.dataset.b;
      const isUp = !!sel && up.has(a) && (b === sel || up.has(b));
      const isDown = !!sel && (a === sel || down.has(a)) && down.has(b);
      p.classList.toggle('up', isUp);
      p.classList.toggle('down', isDown);
      p.classList.toggle('hit', finding && hits.has(a) && hits.has(b));
    }
    const chip = document.getElementById('cbmi-linchip');
    if (chip) {
      const it = sel && M.g && M.g.nodes[sel];
      chip.textContent = it ? `${it.name} ${it.num || ''} · ${up.size} above · ${down.size} below · esc` : '';
    }
    marks();
  };
  M.select = (nid, scroll) => {
    M.sel = nid && M.g && M.g.nodes[nid] ? nid : null;
    M.paint();
    if (M.sel && scroll) { const b = boxOf(M.sel); if (b) b.scrollIntoView(CENTRE); }
  };
  M.set = (key, value) => {
    M[key] = value;
    if (key === 'find' && M.open) {
      const box = document.querySelector('.cbmi-find');
      box.dataset.mode = value;
      place(box);
      M.search(M.q);
    }
    if (key === 'fit') M.later(); else M.paint();
  };
  M.walk = (key) => {
    const s = M.sel;
    let t = null;
    if (key === 'ArrowUp') t = M.mainUp[s] || (M.up[s] || [])[0];
    else if (key === 'ArrowDown') t = (M.mainDown[s] || M.down[s] || [])[0];
    else {
      const p = M.mainUp[s];
      const sib = (p ? M.mainDown[p] : null) || [];
      const at = (n) => { const b = boxOf(n); return b ? b.getBoundingClientRect() : { left: 0, top: 0 }; };
      const order = sib.slice().sort((x, y) => at(x).left - at(y).left || at(x).top - at(y).top);
      const i = order.indexOf(s);
      if (i >= 0 && order.length > 1) t = order[(i + (key === 'ArrowRight' ? 1 : -1) + order.length) % order.length];
    }
    if (t) M.select(t, true);
  };

  // ── find ──
  const subseq = (t, s) => { let i = 0; for (const c of s) if (c === t[i]) i++; return i === t.length; };
  // A word of the type's name ranks over the species, status, tool and the job's own words, which rank over the
  // name of the set or file it reads; a letter run inside a word, and the letters of the kind in order, come last.
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
  M.search = (text) => {
    M.q = text;
    const toks = text.toLowerCase().trim().split(/\s+/).filter(Boolean);
    let scored = [];
    if (toks.length && M.g) for (const [nid, it] of Object.entries(M.g.nodes)) {
      const [s, weakest] = score(it, toks);
      if (s > 0) scored.push([s, nid, weakest]);
    }
    // Once something matches by its name, matches only through a set's name or a letter run drop out.
    const top = Math.max(0, ...scored.map((x) => x[2]));
    if (top >= 80) scored = scored.filter((x) => x[2] >= 50);
    scored.sort((x, y) => y[0] - x[0] || numOf(x[1]) - numOf(y[1]));
    M.hits = scored.map((x) => x[1]);
    M.cur = M.hits.length ? 0 : -1;
    findUi();
    M.paint();
    if (M.hits.length) {
      const b = boxOf(M.hits[0]), sc = document.querySelector('.cbmi-scroll');
      if (b && sc) {
        const r = b.getBoundingClientRect(), v = sc.getBoundingClientRect();
        if (r.bottom < v.top || r.top > v.bottom || r.right < v.left || r.left > v.right) b.scrollIntoView(CENTRE);
      }
    }
  };
  M.step = (d) => {
    if (!M.hits.length) return;
    M.cur = (M.cur + d + M.hits.length) % M.hits.length;
    findUi();
    M.paint();
    const b = boxOf(M.hits[M.cur]);
    if (b) b.scrollIntoView(CENTRE);
  };
  function marks() {
    if (!(window.CSS && CSS.highlights && window.Highlight)) return;
    CSS.highlights.delete('cbmi-find');
    const toks = M.q.toLowerCase().trim().split(/\s+/).filter((t) => t && !/^(#|job)?\d+$/.test(t));
    if (!toks.length) return;
    const ranges = [];
    for (const el of $$('[data-f] .cbmi-title, [data-f] .cbmi-tab, .cbmi-rrow[data-f] .t, .cbmi-find-item .t')) {
      const node = el.firstChild && el.firstChild.nodeType === 3 ? el.firstChild : null;
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
    if (ranges.length) CSS.highlights.set('cbmi-find', new Highlight(...ranges));
  }
  function findBox() {
    let box = document.querySelector('.cbmi-find');
    if (box) return box;
    box = document.createElement('div');
    box.className = 'cbmi-find';
    box.innerHTML = '<div class="cbmi-find-bar"><i class="material-icons">search</i>'
      + '<input type="text" spellcheck="false" autocomplete="off" '
      + 'placeholder="find a job: a type, a number, a species, a status">'
      + '<span class="cbmi-find-count"></span><i class="material-icons cbmi-find-x" title="Close (esc)">close</i></div>'
      + '<div class="cbmi-find-list"></div><div class="cbmi-find-hint"></div>';
    document.body.appendChild(box);
    const inp = box.querySelector('input');
    inp.addEventListener('input', () => M.search(inp.value));
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); M.closeFind(true); return; }
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        M.step(e.key === 'ArrowDown' ? 1 : -1);
        return;
      }
      if (e.key === 'Enter') {
        e.preventDefault();
        if (M.find === 'palette') { const n = M.hits[M.cur]; M.closeFind(true); if (n) M.select(n, true); return; }
        M.step(e.shiftKey ? -1 : 1);
      }
    });
    box.querySelector('.cbmi-find-x').addEventListener('click', () => M.closeFind(true));
    box.querySelector('.cbmi-find-list').addEventListener('click', (e) => {
      const it = e.target.closest('.cbmi-find-item');
      if (!it) return;
      M.closeFind(true);
      M.select(it.dataset.nid, true);
    });
    window.addEventListener('resize', () => { if (M.open) place(box); });
    return box;
  }
  function place(box) {
    const sc = document.querySelector('.cbmi-scroll');
    if (!sc) return;
    const r = sc.getBoundingClientRect();
    if (M.find === 'dock') {
      box.style.width = '380px';
      box.style.left = (r.right - 380 - 14) + 'px';
      box.style.top = (r.top + 10) + 'px';
    } else {
      const w = Math.min(580, r.width - 48);
      box.style.width = w + 'px';
      box.style.left = (r.left + (r.width - w) / 2) + 'px';
      box.style.top = (r.top + Math.max(24, r.height * 0.16)) + 'px';
    }
  }
  function findUi() {
    const box = document.querySelector('.cbmi-find');
    if (!box) return;
    const count = box.querySelector('.cbmi-find-count');
    const n = M.hits.length;
    count.textContent = !M.q.trim() ? '' : n ? (M.find === 'palette' ? `${n} found` : `${M.cur + 1} of ${n}`) : 'none';
    const hint = box.querySelector('.cbmi-find-hint');
    hint.textContent = M.find === 'palette' ? '↑ ↓ choose · ↵ select the job and show its lineage · esc'
      : '↵ next · ⇧↵ previous · ↑ ↓ too · esc closes and clears';
    const list = box.querySelector('.cbmi-find-list');
    if (M.find !== 'palette' || !n) { list.innerHTML = ''; return; }
    list.innerHTML = M.hits.slice(0, 40).map((nid, i) => {
      const it = M.g.nodes[nid];
      return `<div class="cbmi-find-item${i === M.cur ? ' on' : ''}" data-nid="${attr(nid)}">`
        + `<span class="cbmi-light ${attr(it.status)}"></span><span class="t">${attr(it.name)}</span>`
        + `<span class="n">${attr(it.num || '')}</span>`
        + `<span class="c">${attr([it.sp, it.chips].filter(Boolean).join(' · '))}</span></div>`;
    }).join('');
    const on = list.querySelector('.on');
    if (on) on.scrollIntoView({ block: 'nearest' });
  }
  M.openFind = () => {
    const box = findBox();
    box.dataset.mode = M.find;
    box.style.display = 'block';
    place(box);
    M.open = true;
    const inp = box.querySelector('input');
    inp.value = M.q;
    inp.focus();
    inp.select();
    M.search(M.q);
  };
  M.closeFind = (clear) => {
    const box = document.querySelector('.cbmi-find');
    if (box) box.style.display = 'none';
    M.open = false;
    if (clear) { M.q = ''; M.hits = []; M.cur = -1; }
    M.paint();
  };

  // ── the browser's events ──
  document.addEventListener('click', (e) => {
    const F = frame();
    if (!F || !F.contains(e.target)) return;
    const size = e.target.closest('.cbmi-size');
    if (size) {
      const n = size.closest('.cbmi-node');
      if (n) emitEvent('cbmi_size', { nid: n.dataset.nid });
      return;
    }
    const row = e.target.closest('.cbmi-rrow');
    if (row) { M.select(row.dataset.nid, true); return; }
    const node = e.target.closest('.cbmi-world .cbmi-node');
    if (node) {
      if (M.open && M.find !== 'dock') M.closeFind(true);
      M.select(node.dataset.nid, false);
      return;
    }
    if (e.target.closest('.cbmi-scroll')) M.select(null);
  });
  document.addEventListener('dblclick', (e) => {
    const n = e.target.closest && e.target.closest('.cbmi-frame .cbmi-node, .cbmi-frame .cbmi-rrow');
    if (n && !e.target.closest('.cbmi-size')) emitEvent('cbmi_open', { nid: n.dataset.nid });
  });
  document.addEventListener('pointerdown', (e) => {
    const F = frame();
    if (!F || (e.target.closest && e.target.closest('.cbmi-find'))) return;
    F.toggleAttribute('data-cb-focus', F.contains(e.target));
  }, true);
  document.addEventListener('keydown', (e) => {
    const F = frame();
    if (!F || (e.target.closest && e.target.closest('.cbmi-find'))) return;
    const a = document.activeElement;
    const fields = 'input, textarea, select, [contenteditable="true"]';
    const typing = a && a !== document.body && a.closest && a.closest(fields);
    if (e.key === 'Escape') {
      if (M.open || M.q) M.closeFind(true);
      else M.select(null);
      return;
    }
    if (typing || e.ctrlKey || e.metaKey || e.altKey || e.repeat) return;
    if ((e.key === 'f' || e.key === '/') && F.hasAttribute('data-cb-focus')) {
      e.preventDefault();
      M.openFind();
      return;
    }
    if (M.sel && ['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(e.key)) {
      e.preventDefault();
      M.walk(e.key);
    }
  });
  window.addEventListener('resize', () => M.later());

  // A card that grows or a layout that changes moves the others: the cables follow.
  M.mount = () => {
    const W = world();
    if (!W) return;
    if (!W.__cbmi) {
      W.__cbmi = new ResizeObserver(() => M.later());
      new MutationObserver((ms) => {
        if (ms.some((m) => !(m.target.closest && m.target.closest('.cbmi-cables')))) { M.watchNodes(); M.later(); }
      }).observe(W, { childList: true, subtree: true });
    }
    M.watchNodes();
    M.later();
  };
  M.watchNodes = () => {
    const W = world();
    if (!W || !W.__cbmi) return;
    W.__cbmi.observe(W);
    for (const n of $$('.cbmi-node', W)) if (!n.__cbmiSeen) { n.__cbmiSeen = true; W.__cbmi.observe(n); }
  };

  // What each update from the server weighs: the readout on the Pipeline title line.
  M.hookPerf = () => {
    if (M.hooked || !window.socket) return;
    M.hooked = true;
    let total = 0;
    window.socket.onAny((ev, ...args) => {
      if (ev !== 'update') return;
      let n = 0;
      try { n = JSON.stringify(args).length; } catch (x) { n = 0; }
      total += n;
      const el = document.getElementById('cbmi-live');
      const kb = (b) => (b < 1024 ? b + ' B' : (b / 1024).toFixed(1) + ' KB');
      if (el) el.textContent = `last update ${kb(n)} · ${kb(total)} since load`;
    });
  };
})();
"""
