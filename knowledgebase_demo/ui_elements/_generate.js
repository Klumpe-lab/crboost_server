/* Renders CryoBoost roster UI elements to transparent high-res PNGs.
   Geometry + colors are lifted verbatim from ui/pipeline_builder/pipeline_roster.py
   and ui/status_indicator.py so these match the running app. */
const { Resvg } = require('@resvg/resvg-js');
const fs = require('fs'), path = require('path');

const OUT = process.argv[2];
const SCALE = 5;                       // 5x -> ~360 dpi at slide scale
fs.mkdirSync(OUT, { recursive: true });

// ── design tokens (from the source) ─────────────────────────────────────────
const DOT = { scheduled:'#fbbf24', queued:'#a855f7', running:'#3b82f6',
              succeeded:'#10b981', failed:'#ef4444', unknown:'#9ca3af',
              orphaned:'#f97316' };
const TS  = { ok:'#16a34a', fail:'#dc2626', running:'#2563eb', pending:'#d1d5db' };
const MONO='IBM Plex Mono', SANS='IBM Plex Sans';
const NAME_COLOR='#1e293b', SUB_COLOR='#64748b', MUTED='#94a3b8';

const wMono = (s, size) => s.length * size * 0.600;
const wSans = (s, size, ls=0) => s.length * (size * 0.545 + ls);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

// material icon paths (24x24)
const ICON = {
  check_circle:'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z',
  error:'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z',
  sync:'M12 4V1L8 5l4 4V6c3.31 0 6 2.69 6 6 0 1.01-.25 1.97-.7 2.8l1.46 1.46C19.54 15.03 20 13.57 20 12c0-4.42-3.58-8-8-8zm0 14c-3.31 0-6-2.69-6-6 0-1.01.25-1.97.7-2.8L5.24 7.74C4.46 8.97 4 10.43 4 12c0 4.42 3.58 8 8 8v3l4-4-4-4v3z',
  radio_button_unchecked:'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z',
  layers:'M11.99 18.54l-7.37-5.73L3 14.07l9 7 9-7-1.63-1.27-7.38 5.74zM12 16l7.36-5.73L21 9l-9-7-9 7 1.63 1.27L12 16z',
  circle_dot:'M7.5.877a6.62 6.62 0 100 13.246A6.62 6.62 0 007.5.877zM1.827 7.5a5.673 5.673 0 1111.346 0 5.673 5.673 0 01-11.346 0zm6.546 0a.873.873 0 11-1.746 0 .873.873 0 011.746 0zm.95 0a1.823 1.823 0 11-3.646 0 1.823 1.823 0 013.646 0z',
};
const icon = (name, x, y, px, color) => {
  const s = px / (name === 'circle_dot' ? 15 : 24);
  return `<g transform="translate(${x} ${y}) scale(${s})"><path d="${ICON[name]}" fill="${color}" fill-rule="evenodd" clip-rule="evenodd"/></g>`;
};
const text = (s, x, y, { f=MONO, size=11, w=400, fill=NAME_COLOR, ls=null, anchor=null } = {}) =>
  `<text x="${x}" y="${y}" font-family="${f}" font-size="${size}" font-weight="${w}" fill="${fill}"` +
  (ls !== null ? ` letter-spacing="${ls}"` : '') + (anchor ? ` text-anchor="${anchor}"` : '') +
  ` xml:space="preserve">${esc(s)}</text>`;
const rect = (x,y,w,h,fill,extra='') => `<rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${fill}" ${extra}/>`;

// ── one roster job row ──────────────────────────────────────────────────────
// opts: {name, status, chips:[{t,color,weight}], width, rail:true, active:false, indent}
const ROW_H = 22;
function jobRow(o) {
  const indent = o.indent ?? 10, W = o.width, y0 = o.y ?? 0;
  const bg = o.active ? '#f0f4f8' : '#ffffff';
  const rail = o.active ? '#475569' : '#e5e7eb';
  let s = '';
  if (o.rail !== false) { s += rect(0, y0, W, ROW_H, bg); s += rect(0, y0, 2, ROW_H, rail); }
  const cy = y0 + ROW_H/2;
  let x = indent;
  s += `<circle cx="${x+4}" cy="${cy}" r="4" fill="${DOT[o.status]}"/>`;   // 8px dot
  x += 8 + 6;
  s += text(o.name, x, cy + 3.9, { size: 11, w: o.active ? 600 : 400,
                                   fill: o.status === 'unselected' ? '#9ca3af' : NAME_COLOR });
  // right-aligned chips
  let rx = W - (o.rail === false ? 0 : 8);
  for (const c of [...(o.chips || [])].reverse()) {
    const cw = wMono(c.t, 9);
    const gw = c.glyph ? 7 : 0;
    rx -= cw + gw;
    if (c.glyph === "play") s += `<path d="M${rx} ${cy-3.6} L${rx+4.6} ${cy} L${rx} ${cy+3.6} Z" fill="${c.color}"/>`;
    if (c.glyph === "skip") s += `<circle cx="${rx+3.4}" cy="${cy}" r="3.3" fill="none" stroke="${c.color}" stroke-width="1.1"/>`
                              + `<line x1="${rx+1.2}" y1="${cy+2.2}" x2="${rx+5.6}" y2="${cy-2.2}" stroke="${c.color}" stroke-width="1.1"/>`;
    s += text(c.t, rx + gw, cy + 3.2, { size: 9, w: c.weight || 600, fill: c.color });
    rx -= 5;
  }
  return s;
}
function jobRowWidth(o) {
  const indent = o.indent ?? 10;
  let w = indent + 8 + 6 + wMono(o.name, 11);
  for (const c of (o.chips || [])) w += 5 + wMono(c.t, 9) + (c.glyph ? 7 : 0);
  return Math.ceil(w + 10);
}

// ── per-tilt-series sub-rows ────────────────────────────────────────────────
const TS_H = 17;
function tsRows(items, W, x0, y0) {
  let s = rect(x0, y0, 2, items.length * TS_H, '#e2e8f0');     // left rail
  items.forEach((it, i) => {
    const y = y0 + i * TS_H, cy = y + TS_H/2;
    s += rect(x0, y + TS_H - 1, W - x0, 1, '#f1f5f9');          // row divider
    s += icon(it.st === 'ok' ? 'check_circle' : it.st === 'fail' ? 'error'
            : it.st === 'running' ? 'sync' : 'radio_button_unchecked',
            x0 + 8, cy - 5.5, 11, TS[it.st]);
    s += text(it.name, x0 + 8 + 11 + 5, cy + 3.5, { size: 10, fill: SUB_COLOR });
    const lbl = it.st.toUpperCase();
    s += text(lbl, W - 6 - wMono(lbl, 8), cy + 3, { size: 8, w: 600, fill: TS[it.st], ls: 0.3 });
  });
  return s;
}

// ── phase header ────────────────────────────────────────────────────────────
const PH_H = 20;
function phaseHeader(label, W, y0, glyph) {
  let s = rect(0, y0, W, PH_H, '#f1f5f9') + rect(0, y0 + PH_H - 1, W, 1, '#e5e7eb');
  s += icon(glyph, 10, y0 + 4, 12, MUTED);
  s += text(label.toUpperCase(), 10 + 12 + 5, y0 + PH_H/2 + 3.2,
            { f: SANS, size: 9, w: 700, fill: MUTED, ls: 0.63 });
  return s;
}

// ── emit ────────────────────────────────────────────────────────────────────
const FONTS = ['ttf/PlexSans-Regular.ttf','ttf/PlexSans-Medium.ttf','ttf/PlexSans-SemiBold.ttf',
               'ttf/PlexMono-Regular.ttf','ttf/PlexMono-SemiBold.ttf'].map(p => path.resolve(__dirname, p));
let n = 0;
function emit(name, W, H, body) {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">${body}</svg>`;
  fs.writeFileSync(path.join(OUT, name + '.svg'), svg);
  const png = new Resvg(svg, { fitTo:{ mode:'zoom', value:SCALE }, background:'rgba(0,0,0,0)',
      font:{ fontFiles:FONTS, defaultFontFamily:'IBM Plex Sans', loadSystemFonts:false } }).render();
  fs.writeFileSync(path.join(OUT, name + '.png'), png.asPng());
  console.log(`  ${name}.png  ${png.width}x${png.height}`);
  n++;
}

// 1 ── per-stage "done" chips (no rail — drop-in next to a figure box)
const STAGES = [
  ['Import','importmovies','1/1'], ['Motion & CTF','fsMotionAndCtf','28/28'],
  ['Tilt Filter','tiltFilter',null], ['Alignment','aligntiltsWarp','28/28'],
  ['Miss Align','missAlign',null], ['TS CTF','tsCtf','28/28'],
  ['Reconstruct','tsReconstruct','28/28'], ['Denoise Train','denoisetrain',null],
  ['Denoise Predict','denoisepredict','28/28'], ['Template Match','templatematching','28/28'],
  ['Pick candidates','tmextractcand','28/28'], ['Subtomo Extraction','subtomoExtraction','28/28'],
  ['Reconstruct Particle','reconstructParticle',null], ['Class 3D','class3d',null],
];
console.log('stage chips (done):');
for (const [label, slug, prog] of STAGES) {
  const o = { name: label, status: 'succeeded', indent: 0, rail: false,
              chips: prog ? [{ t: prog, color: TS.ok, weight: 600 }] : [] };
  const W = jobRowWidth(o);
  emit('stage-' + slug, W, ROW_H, jobRow({ ...o, width: W }));
}

// 2 ── status dot legend
{
  const states = [['Scheduled','scheduled'],['Queued','queued'],['Running','running'],
                  ['Succeeded','succeeded'],['Failed','failed'],['Orphaned','orphaned']];
  const CW = 108, W = CW * states.length, H = 22;
  let s = '';
  states.forEach(([lab, k], i) => {
    const x = i * CW;
    s += `<circle cx="${x+6}" cy="${H/2}" r="4" fill="${DOT[k]}"/>`;
    s += text(lab, x + 16, H/2 + 3.9, { size: 11, fill: NAME_COLOR });
  });
  console.log('legend:'); emit('status-dot-legend', W, H, s);
}

// 3 ── full roster, everything done
{
  const W = 300;
  const pre = ['Import','Motion & CTF','Tilt Filter','Alignment','TS CTF','Reconstruct','Denoise Train','Denoise Predict'];
  const par = ['Template Match','Pick candidates','Subtomo Extraction','Reconstruct Particle','Class 3D'];
  const prog = { 'Motion & CTF':'28/28','Alignment':'28/28','TS CTF':'28/28','Reconstruct':'28/28',
                 'Denoise Predict':'28/28','Template Match':'28/28','Pick candidates':'28/28','Subtomo Extraction':'28/28' };
  let y = 0, s = '';
  s += phaseHeader('Preprocessing', W, y, 'layers'); y += PH_H;
  for (const nm of pre) { s += jobRow({ name:nm, status:'succeeded', width:W, y,
      chips: prog[nm] ? [{ t:prog[nm], color:TS.ok }] : [] }); y += ROW_H; }
  s += phaseHeader('Particles', W, y, 'circle_dot'); y += PH_H;
  for (const nm of par) { s += jobRow({ name:nm, status:'succeeded', width:W, y,
      chips: prog[nm] ? [{ t:prog[nm], color:TS.ok }] : [] }); y += ROW_H; }
  console.log('full roster:'); emit('roster-all-done', W, y, s);
}

// 4 ── full roster, mixed live states
{
  const W = 300;
  const rows = [
    ['Import','succeeded',[['1/1',TS.ok]]],
    ['Motion & CTF','succeeded',[['28/28',TS.ok]]],
    ['Tilt Filter','succeeded',[]],
    ['Alignment','succeeded',[['27/28',TS.ok],['1!','#dc2626',700]]],
    ['TS CTF','running',[['4','#2563eb',700,'play'],['12/28','#2563eb']]],
    ['Reconstruct','queued',[]],
    ['Denoise Train','scheduled',[]],
    ['Denoise Predict','scheduled',[]],
  ];
  const par = [['Template Match','scheduled',[]],['Pick candidates','scheduled',[]],
               ['Subtomo Extraction','scheduled',[]]];
  let y = 0, s = '';
  s += phaseHeader('Preprocessing', W, y, 'layers'); y += PH_H;
  for (const [nm, st, ch] of rows) { s += jobRow({ name:nm, status:st, width:W, y, active: st === 'running',
      chips: ch.map(([t,c,w,g]) => ({ t, color:c, weight:w||600, glyph:g })) }); y += ROW_H; }
  s += phaseHeader('Particles', W, y, 'circle_dot'); y += PH_H;
  for (const [nm, st, ch] of par) { s += jobRow({ name:nm, status:st, width:W, y, chips:ch }); y += ROW_H; }
  console.log('mixed roster:'); emit('roster-mixed-live', W, y, s);
}

// 5 ── array job expanded: running per-tilt-series tasks  (@ui, doc 04)
{
  const W = 300;
  const mk = (i, st) => ({ name: `TS_${String(i).padStart(2,'0')}`, st });
  const items = [ mk(1,'ok'), mk(2,'ok'), mk(3,'ok'), mk(4,'ok'), mk(5,'ok'), mk(6,'ok'),
                  mk(7,'running'), mk(8,'running'), mk(9,'running'), mk(10,'running'),
                  mk(11,'pending'), mk(12,'pending'), mk(13,'pending'), mk(14,'pending') ];
  let y = 0, s = '';
  s += jobRow({ name:'Reconstruct', status:'running', width:W, y, active:true,
                chips:[{t:'4',color:'#2563eb',weight:700,glyph:'play'},{t:'6/20',color:'#2563eb'}] });
  y += ROW_H;
  s += tsRows(items, W, 14, y); y += items.length * TS_H;
  console.log('array expanded:'); emit('roster-array-running', W, y, s);
  // the task block on its own
  let s2 = tsRows(items, W, 0, 0);
  emit('ts-tasks-running', W, items.length * TS_H, s2);
}

// 6 ── a compact "20 tasks, 4 running" strip for the dispatch figure
{
  const N = 20, BOX = 13, GAP = 3, PER = 10;
  const cols = PER, rowsN = Math.ceil(N / PER);
  const W = cols * (BOX + GAP) - GAP, H = rowsN * (BOX + GAP) - GAP;
  const st = i => i < 6 ? 'ok' : i < 10 ? 'running' : 'pending';
  let s = '';
  for (let i = 0; i < N; i++) {
    const x = (i % PER) * (BOX + GAP), y = Math.floor(i / PER) * (BOX + GAP);
    const k = st(i);
    s += `<rect x="${x}" y="${y}" width="${BOX}" height="${BOX}" rx="2.5" fill="${k==='pending'?'#f1f5f9':TS[k]}" ${k==='pending'?'stroke="#e2e8f0" stroke-width="1"':''}/>`;
  }
  console.log('task grid:'); emit('array-task-grid-20', W, H, s);
}

console.log(`\n${n} figures -> ${OUT}`);
