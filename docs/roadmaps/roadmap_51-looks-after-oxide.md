# Roadmap 51 — Looks after Oxide

**Status:** scoped 2026-11-11. S0 built the same day: `/dev/looks` shows Harbor beside three looks after Oxide
Computer's console and site, on one mock workspace (`ui/looks_preview.py`), checked in headless Chromium.
2026-11-12: Harbor Mono taken (D1), and the maintainer asked for less nesting: one box deep (D5, §5). The three new
looks on `/dev/looks` are flat now, and the rule was previewed on the real UI through two project snapshots (§5).
The same day the branch map was made to take a look from variables and shown as the Jobs view will be once it is in
(§6, `/dev/io-ideas/branch?frame=app`). Nothing in the app itself changed, and nothing goes into main until the
maintainer says so. Then: S1–S6 on main's code (§7, §8).

**In one line.** Oxide's UI reads as stylized and dense at the same time because the stylization lives in a type
voice and a few drawn things, never in heavier chrome. Harbor Mono carries that into CryoBoost with Harbor's palette,
and one box deep takes the boxes inside boxes out.

**Not in this roadmap:** layout or flow changes (the views, the roster and the job page keep their shape); the
Journey's own quieting (roadmap 35); features seen on Oxide's console that CryoBoost lacks (a command palette,
breadcrumbs, a jump-to box).

## 1. What Oxide does (survey 2026-11-11)

Read in headless Chromium and from their published stylesheets: oxide.computer (home, compute, specifications,
HPC, principles, blog), the console's public preview (console-preview.oxide.computer: instances, one instance's
storage and metrics, system utilization, sleds), the RFD site (rfd.shared.oxide.computer) in both themes, and the
design-system tokens (github.com/oxidecomputer/design-system, `color-gen/themes.json`).

- **Two voices of type.** Suisse Intl (light 300, regular 400, medium 500) for names, values and prose. GT
  America Mono 400 for everything that is a label: column heads, the keys of key/value tables, tabs, buttons,
  badges, figure captions, section names in the sidebar. The mono is always in capitals, tracked +0.04 rem, at
  10, 11, 12 or 14 px, with stylistic sets on and contextual alternates off. The sans gets positive tracking at
  small sizes (+0.042 em at 12 px, +0.03 em at 14 px). This voice is most of the stylized feel, and it costs no
  space.
- **Colour.** Neutrals are OKLCH hue 260 at almost no chroma (dark ground #080F11, light ground #FCFCFF), so
  there is no blue cast. One accent, green 800 = oklch(77 % 0.19 164) ≈ #00d892, is both state (running,
  attached, published) and the primary action (NEW INSTANCE), and marks the open item in the sidebar.
  Blue-violet marks work in progress (STARTING, with a spinner), red 800 failure, yellow notice. Everything else
  is grey.
- **Badges, not dots.** A state is a 16 px box: tinted fill (accent 200 on dark, accent 1200 on light), a 1 px
  border in the same hue, the word in mono capitals (RUNNING, FAILED, STARTING). Neutral states (STOPPED,
  DISTRIBUTED) are a grey outline.
- **Geometry.** Corner radii are 1–2 px. Borders are hairlines, often drawn as box-shadow rings; panels have no
  drop shadows. Tables are fully ruled (rows and columns), with the header row on a raised surface. A page is one
  surface: its tables and property lists sit on it, never in a card inside a card.
- **Numbers.** A unit or a decimal sits a step back from its number: "2 vCPUs" with the unit dimmer, "29.17%"
  with ".17%" dimmer, a date's time dimmer than its day.
- **Key/value tables** for an object's properties: a mono-capitals key column and a value column, ruled, two
  pairs per row when there is room.
- **Density.** The console's body text is 14 px and its rows are 44 px, so it is not denser than CryoBoost. It
  looks dense because of the small mono labels and the ruling.
- **Drawn things, small and exact.** ASCII isometric line drawings of racks and sleds with one coloured detail
  (a red "STATUS ERROR" inside), dithered renders, terminal blocks with a `~>` prompt, boxed "FIG. 1" captions, a
  grid of squares for instance occupancy, a strip of square chips (S0 S1 … +24) for attestations, stacked bars
  captioned PROVISIONED / QUOTA, faint grid paper behind a heading. Each is a toy made of the UI's own parts.

Two things do not carry over: the marketing site's large type and generous spacing, and the console's 14 px
body text. CryoBoost keeps its 10–11 px.

## 2. The looks (S0, built)

`/dev/looks` (route in `ui/main_ui.py`, module `ui/looks_preview.py`) draws one mock workspace and one sheet of
pieces in four looks from one markup. The workspace has the rail, an 18-job roster, Reconstruct job024 running
(Parameters, Inputs / Outputs, SLURM, Log), and a Journey slice (stat tiles, a tomogram with its X/Z slab, two
per-tilt plots, the tilt strip). The sheet has the projects table, a run dialog, the empty Jobs view, the
tomogram wall and the controls. A look is a set of `--lk-*` variables on its root plus a few rules of its own.
`?look=harbor-mono` shows one look; the buttons at the top switch looks without a reload. The data is mock;
nothing reads a project.

| | Harbor (today) | Console | Datasheet | Harbor Mono (taken) |
|---|---|---|---|---|
| Ground | cool paper #f6f8fb, desk #e9eef4 | near-black #07090b, cards #0d1014 | paper #fcfcff, desk #f1f1f2 | Harbor's desk, white sheets |
| Faces | Plex Sans, Plex Mono, Plex Sans Condensed | Geist, Geist Mono | Host Grotesk, Fragment Mono | Plex Sans, Plex Mono |
| Labels | sans 10 px, mixed case | mono capitals 8.5 px, +0.07 em | same | same |
| Boxes | white panels on a pale sheet inside each card | one box deep (§5) | same | same |
| Panel head | light-blue stripe, blue tick, glint | raised grey, hairline | raised grey, hairline | paler stripe, tick kept, no glint |
| Corners, shadows | 4–6 px, soft shadows | 2 px, none | 2 px, none | 2 px, none |
| State | round light with a halo, pill badges | square pip, boxed mono badge | same, mint / lavender / rose | same, Harbor's hues |
| The one action | navy fill | green on dark green | mint, dark green ink | navy fill |
| Units, decimals | 9 px grey suffix | a step dimmer than the number | same | same |
| Drawn things | none | ASCII figure in the empty view, FIG captions, dot-matrix loading tile, `~>` prompt in the log | same | same |

Shared by the three new looks: a finished job is quiet (an outline badge, a plain pip), a running one blinks, a
failed one is the only filled red, a job not run yet is dashed. Labels and controls speak in mono capitals;
names (jobs, tilt series, projects) and values stay in the sans and the mono respectively, never capitals. The
branch map's magazine strip already uses the square language, so it needs no change.

To look in a browser: open `/dev/looks` on a server started from this tree. Without one:

    ~/.local/share/crboost-dev/pw/bin/python scripts/dev_screenshot.py --panels --width 1500 --height 1000 --settle 2500 /dev/looks

That writes eight PNGs to `notes/screens/` (each look's workspace and pieces).

## 3. What each look costs

- **Harbor Mono** changes tokens and a handful of shared constants. Every view changes at once, because labels,
  buttons and badges are already central (`LABEL_STYLE`, `house_button`, `_badge_html`). The smallest step.
- **Datasheet** is the same work plus the palette. Harbor's colours live in `--cb-*` variables and their
  fallbacks, but also as hard-coded hex: 1,555 occurrences in 84 files under `ui/`, most of them Harbor's slate
  and navy. A light look can leave most of those, since they read as neutral greys beside it, and sweep the ones
  that clash.
- **Console** needs every hard-coded light colour behind a token (the same 1,555), dark themes for the charts
  (`JOURNEY_THEME`, `ui/dashboard/figures.py:90`, and the Plotly hover rules in `ui/dashboard/css.py`), a dark
  glass for the tooltips, and a look at every picture frame. Pictures (tomograms, slabs, cutouts) read best on
  it. It gets its own roadmap (S7).

## 4. Decisions

- **D1 — the look: Harbor Mono** (2026-11-12, "Let's go with harbor mono").
- **D2 — faces: IBM Plex,** as Harbor Mono has them; it follows from D1. Geist and Fragment Mono stay on the mock
  only.
- **D3 — drawn things: as Harbor Mono shows them** (the ASCII figure in the empty Jobs view, numbered captions on
  pictures, the dot-matrix loading tile, the `~>` prompt in logs). Taken with D1, not asked on its own; the user
  check at the end of S6 can drop any of them.
- **D4 — dark: not now.** Harbor Mono is light; S7 stays unscheduled.
- **D5 — one box deep** (2026-11-12). The maintainer finds the panels nested in panels too much and asked for a
  pass over every view: where a panel holds something that "doesn't need separation from its outer container",
  its borders and outlines go. §5 is the rule and what it changes.

## 5. One box deep (D5)

**The rule.** The desk holds the cards: the job roster, each view, a dialog, the landing page's sheet. Inside a
card nothing draws a box again. A section is its title stripe and its content on the card's white, sections a
gap apart. What is a thing of its own keeps its edge: a field, a button, a badge, a tile, a picture, the log
well, a table that scrolls, a menu or popover. A warning keeps its tint and loses its box.

**The stripe stays.** A panel-less open sheet was tried and turned down on 2026-10-22. What made sections read
apart then was the box; now it is the title stripe (`PANEL_HEAD_STYLE`, in Harbor Mono's paler colours), so a
section still reads as one block without a frame around it.

**What is nested today** (read from two real projects' snapshots, 59 views, and from main's code):

- Every view is a card (`_CARD_EDGE`) on the desk with a pale sheet (`--cb-page` #f6f8fb) inside, and on it white
  panels (`PANEL_STYLE`: fill, outline ring, shadow, title stripe): about 390 of them across the snapshot pages.
  Job pages, the Particles registry tabs, the analytics, the Protocols view, the landing form and the dialogs
  all use it.
- The job page's folded sections (Log, Files, Tasks) open into a second ringed box (`outline: var(--cb-box-line)`)
  inside the ringed section; in Tasks the dark log well sits inside that: box, box, box.
- The Journey's sections are ringed cards (`.cb-journey .cb-main > .cb-section-card`) inside the view card; the
  Tilts tab's groups are ringed (`.cb-tp-group`) with ringed tilt cards inside.
- The Particles registry's rail is a white panel beside the pale sheet; its tabs' sections are panels.
- Three dialogs carry a warning as a Quasar card inside the dialog card.

**What changes**, with anchors on main (98524dd):

- `PANEL_STYLE` (`ui/job_plugins/_field_styles.py:73`): its ring, fill and shadow read `--cb-section-ring`,
  `--cb-section-fill` and `--cb-section-shadow` (§6), transparent inside a card or a page's sheet (the views, the
  job roster, dialogs, the landing page's two views), so no call site changes. The stripe (`PANEL_HEAD_STYLE`) and
  the padding stay. The padding goes in as longhands (`padding-top` and so on), not the
  `padding` shorthand: Chromium serializes the other longhands of a `var()` shorthand as empty once one is set
  (`fold_panel` sets `padding-bottom`), so a snapshot's frozen page loses those panels' padding and their stripes
  overhang.
- `_THEME_CSS` (`ui/dashboard/css.py:2119`): `--cb-page` #ffffff, so a view is one white sheet (charts, tables and
  fields sit on white as they did on the panels). `--cb-panel-line` stays: it is the cards' edge.
- The job page's fold box (`ui/pipeline_builder/job_tab_component.py:84`): no outline; its resize grip stays.
- The Journey's sections (`ui/dashboard/css.py:1782`): no outline, no fill. The pick viewer's root and empty card
  (`.cb-section-card`, `ui/dashboard/css.py:109`, used by `ui/particles/pick_viewer.py:1584`, `:1837`, `:1860`):
  no border.
- The Tilts tab's groups (`ui/dashboard/css.py:1374`): no outline, no fill; the selected group keeps its accent
  ring and tick, which are state.
- The species rail (`ui/species/page.py:112`): a column with a hairline on its right (`--cb-panel-line`), no fill,
  no radius, no margin.
- The landing page: the projects roster's panel (`ui/projects_overview.py:402`) loses its outline; its border
  stays transparent until deletion or tagging colours it, which is state. The new-project form's sections follow
  `PANEL_STYLE`; the dataset table (`ui/dataset_overview_panel.py:150`) keeps its frame.
- The warnings in the delete dialogs (`ui/pipeline_builder/job_tab_component.py:493`,
  `ui/pipeline_builder/pipeline_roster.py:825`, `:897`): a tinted `div`, no border, no card shadow.

**Left as they are:** the desk and the cards (roster, views, dialogs), the view title lines, fields, buttons,
badges, tiles and their captions, pictures, the log well, menus and popovers, the merge dialog's records and the
task tray's rows (items in a list).

**Preview.** The rule was tried as an extra stylesheet on the frozen snapshots of two projects
(`<project>/Snapshots/*.zip`, made by the snapshot window from main): eight before/after pairs in
`notes/screens/denest-*.png` (job page with Tasks, a template-matching job page, the Journey, the Tilts tab, the
tomogram wall, and the Particles registry's overview, picks and analytics). The snapshot's padding loss (above)
was patched in both halves. On `/dev/looks` the three new looks are one box deep, Harbor is left as today.

## 6. How it is built across the app: variables, voices, context

The look is data. Three layers, each in one place, so a look is one block of values and a new view gets it without
knowing it exists.

1. **Variables.** Every colour, line, radius, shadow and type choice a look decides is a CSS custom property
   `--cb-<name>`, defined once in `_THEME_CSS` (`ui/dashboard/css.py`). The families: surfaces (desk, sheet, card,
   field, band, paper); lines (card edge, rule, hairline); the ink scale `--cb-ink-1` … `--cb-ink-4`; the states
   `--cb-{ok,run,que,bad,warn,off}-{dot,ink,bg,line}`; the one action and the pick (what is chosen); the voices
   `--cb-label-*`, `--cb-tag-*`, `--cb-ctl-*` (buttons, tabs), `--cb-badge-*`, each `font`, `size`, `weight`, `case`,
   `track`, `ink`; geometry (`--cb-radius`, `--cb-field-radius`, `--cb-pip-radius`, the shadows). Harbor Mono is
   one block of values; a dark look (S7) is a second.
2. **Voices and primitives.** A component never spells a font, a size, a case or a colour: it names a voice or a
   state. One helper per voice returns inline CSS that reads the variables (`_voice(kind, …)`, `_label(…)` in the
   branch map are the pattern; in the app they sit in `ui/job_plugins/_field_styles.py`, and `LABEL_STYLE` becomes
   a call to one). The house primitives (`house_button`, the house fields, `panel_head` / `config_section`,
   `_badge_html`, `_dot_html`) read the same variables; a rule that needs a hover or a pseudo-element goes in
   `ui/dashboard/css.py` on a class, still reading variables. Every inline use keeps today's value as its var()
   fallback, so converting a component changes nothing on screen until a look sets the variable: conversion and
   look are separate steps, and a conversion is proven by a pixel diff of the views before and after.
3. **Context decides nesting.** One box deep is a cascade rule, not an edit per site: a section draws its ring and
   fill from `--cb-section-*`, which are transparent inside a card or a sheet and set only where a section stands
   on the bare page. The branch map's own boxes do the same (`--cb-bm-box-*`, `--cb-bm-paper-*`: boxes on the
   mock page, strips in the app frame).

**Keeping it so.** A check beside `check_boundaries.py`: no hex colour, `font-family` or `text-transform` literal
in `ui/` outside the theme block and the voice helpers, with an allow-list that shrinks as files convert. The
1,555 literals of §3 are that list's starting point; when it is empty, a second look costs only its values.

**Worked example: the branch map** (2026-11-12). `ui/branch_map_preview.py`'s colours, label voice, tool tags,
lights, ports, picker and paper read `--cb-*` variables with today's values as fallbacks (`_tok`, `_voice`,
`_label`); screenshots of `/dev/io-ideas/branch` before and after differ only by a blinking mark caught at another
moment. `HARBOR_MONO` in the same module is the look as values plus two rules (a running light blinks instead of
pulsing; stand-ins for the house buttons and the stat labels until S1 converts them). `?frame=app` draws the Jobs
view as roadmap 32 §15.8 plans it: the rail, the job roster, the pipeline frame showing the map with no job open,
one box deep (`APP_CSS`: the picker and the controls are strips across the view, the paper fills it). `&look=harbor`
shows the same frame in today's look. Screens: `notes/screens/final-*.png` (the view, a job being added, the same
in today's look, every card large, one large card at 100 % in both looks).

## 7. Stages

Built back to back on main's code and checked once at the end, the way Harbor's controls sweep was. The real UI on
main moved about 50 files under `ui/` past this branch's base (among them `ui/dashboard/css.py`,
`ui/job_plugins/_field_styles.py`, `ui/workspace_page.py`, `ui/pipeline_builder/job_tab_component.py`,
`ui/pipeline_builder/logs_tab.py`, `ui/projects_overview.py`, `ui/template_workbench.py`), so the build happens in
a tree on main, not on this branch.

- **S1 — variables and voices,** with today's values (§6). The families into `_THEME_CSS`; the voice helpers into
  `ui/job_plugins/_field_styles.py`; labels, column heads, tabs, buttons and badges read them: `LABEL_STYLE`,
  `PANEL_TITLE_STYLE`, `PANEL_HEAD_STYLE`, `VIEW_TITLE_STYLE`, `SECTION_HEADER_STYLE`
  (`ui/job_plugins/_field_styles.py`); `_BASE_STYLE`, `_KIND_STYLE`, `_ICON_STYLE` (`ui/components/buttons.py`);
  `render_segmented` (`ui/components/segmented.py`) and `.cb-seg-btn` / `.cb-tb-btn` / `.cb-chip` in
  `ui/dashboard/css.py`; the house fields (`ui/components/fields.py`); the projects overview's column titles
  (`.cb-proj-colhead`). Checked by a pixel diff of a snapshot's views: nothing moves. The literal check of §6
  starts here with its allow-list.
- **S2 — one box deep,** as §5 lists it, through the context variables. The rule then goes into CLAUDE.md's UI
  conventions, beside "Sections separate by whitespace".
- **S3 — the look.** Harbor Mono's values into `_THEME_CSS`: every converted label, tab, button and badge changes
  voice at once; radius 2 px, no shadows. Field label width moves from 110 px to what the mono needs (104 px in the
  mock). Fonts through `_TITLE_FONT_LINK` (the per-client channel), never the `main_ui.py` shell. The branch map's
  `HARBOR_MONO` block and its stand-ins go: the app's variables now carry it.
- **S4 — state.** `_dot_html`, `_ring_html`, `_running_led_html` and `_badge_html` (`ui/status_indicator.py`)
  draw the square pip and the boxed badge; done quiet, running blinking (CSS, no timer), failed filled, not run
  dashed. The roster's active row (`_row_paint`, `ui/pipeline_builder/pipeline_roster.py`) and the job page's
  title line (`_JOB_HEAD_STYLE`, `ui/workspace_page.py`) take the look's selection colours.
- **S5 — numbers.** One helper that renders a value with its unit and decimals a step back, used by the stat
  tiles (`.cb-stat-value`), the projects table's size column, the I/O rows and the roster's running cell.
- **S6 — drawn things,** as D3 says: the empty Jobs view (`_refs["empty_title"]`, `ui/workspace_page.py`),
  captions on the tomogram wall's tiles, the loading look (`@keyframes cb-img-wait`, `ui/dashboard/css.py`), the
  prompt in the Log panel.
- **S7 — dark (Console only).** The hard-coded colours behind tokens, file by file; chart themes; tooltips;
  picture frames. Its own roadmap if D4 changes.

User check at the end: the real workspace on a real project (roster, a running job's page, the Journey, the
tomogram wall, the Particles registry, the landing page, two dialogs) against the Harbor Mono panels of
`/dev/looks`, and a fresh snapshot against the right halves of `notes/screens/denest-*.png`.

## 8. Pickup

Nothing goes into main until the maintainer says so (2026-11-12: "don't merge anything in yet. i just want to take
a look at it"). To look: `/dev/looks` (`?look=harbor-mono`), `/dev/io-ideas/branch?frame=app` (the Jobs view with
the map; `&look=harbor` for today's look, `&add=denoisepredict&after=job024`, `&large=all`, `&ts=<id>` as on the
mock page), or the screens in `notes/screens/` (`dev-looks--*`, `denest-*`, `final-*`). When it is time: a session
in a tree on main (not `branch_map`) reads §5–§7 and builds S1–S6 in one pass; the anchors above are main's at
98524dd, so check them against the tree first. `/dev/looks` and `ui/looks_preview.py` go when the look is in the
app, with the other `/dev` pages (roadmap 32 §15.8 S5).
