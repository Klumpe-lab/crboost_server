# Roadmap 60: the Particles registry, brought into line with the rest of the app

The maintainer, 2026-11-19: the Particles registry is poor in layout, colour and usability. The colours of the chips
that go with particles must change. The Jobs tab is not needed at all. Picks & curation is just a list of tomograms,
each with "Curate picks". Templates & masks looks bad and is laid out badly. Improve all of it, and fold together or
cut the functions these tabs repeat.

Nothing is built yet. §1 records the page as it is (seen from the running server, 2026-11-19), §2 what it repeats,
§3 the target, §4 the decisions it needs, §5 the stages.

## 1. The page today

Seen on `AGG412_20260311_MyrGFP_Grid2_rerun` (two species, 412_particle and dm_ovary_ribosome) at a 1700 px window
with the roster open. That leaves the registry about 1270 px, its detail pane about 1060 px.

The frame is `ui/species/page.py`: the species rail (200 px, `rail.py`) beside a detail pane. The species mark and six
tabs sit on the view's title line: Overview, Templates & masks, Picks & curation, Analytics, STA kitchen, Jobs. Each
tab is built once per species and cached; switching only flips visibility.

**Rail** (`ui/species/rail.py`). One row per species: the grey glyph, the name, "1 tpl · 0 lists". "Lists" counts
`state.pick_lists`, the hand-made, imported and merged lists, so a species with 2,064 template-matching picks reads
"0 lists". The count says nothing a user means by "picks".

**Overview** (`overview_tab.py`, 667 lines).
- **Species info:** the colour swatch, the name in a large box, diameter, symmetry and notes.
- **Template matching files:** the selected template and mask, each with a polarity or method chip. This repeats the
  Templates & masks tab.
- **Extraction geometry:** box, binning and crop, three empty fields under a two-line explanation, then "not set".
- **Status:** one muted sentence ("2064 picks on 45 tomograms · 2064 kept · 0 lists extracted").
- **Actions:** Delete species, and Publish to catalog, at the far right edge.

The block fills the top 480 px and leaves the rest of the pane empty.

**Templates & masks** (`templates_tab.py` mounts `ui/template_workbench.py`, 1,982 lines).
- **Templates and Masks:** two panels in a CSS auto-fit grid (`minmax(560px, 1fr)`). At this width they stack, so
  each fixed-height (186 px) list leaves 100–150 px of white under one or two rows.
- **Source:** under Templates, four always-open creation forms (Shape, PDB / EMDB, Import, Edit current). Under
  Masks, a second Source with three (From template, Sphere, Import).
- **Viewer:** molstar, 380 px, starting at y ≈ 940, below the fold; the Activity log is under it.
- **Selection:** a selected row is ringed in indigo for templates and purple for masks (`_INDIGO`, `_PURPLE`). The
  house rules allow no purple for state.
- **Chips:** BLACK is a near-black chip, WHITE orange on cream, SPHERICAL purple (`ui/components/chip.py`,
  `render_polarity_chip`, `_METHOD_PALETTE`).
- **Size badge:** turns orange or red for large files.

The page reads as three generations of design stacked on top of each other.

**Picks & curation** (`picks_tab.py`, 983 lines). One group per tomogram, 48 of them for this species, each about
130 px tall:
- the tomogram name, a copy icon and "1 list";
- Curate picks, a viewer icon and a Journey icon, on the right;
- a table head (LIST · SOURCE · ORIGIN · PICKS · EXTRACTED · JOB) over one row: a vivid yellow species dot,
  "candidates", "tm", the template's file name and the count;
- an "import a .coords list by path" row.

That is 6,000 px of scrolling to say, 48 times, "this tomogram has one candidates list of N picks". The Watcher footer
is at the very bottom. Refresh and Extract all pending sit on the section head.

**Analytics** (`analytics_tab.py`, 618 lines).
- **Scores in σ:** a source picker, stat tiles, the σ ladder, the cut with Make subset, the histogram and the survival
  curve.
- **Where the picks sit:** the funnel, the depth chart and yield against quality, and overlap with another species.
- **Tomograms:** a marker summary and a full per-tomogram table (σ, N, FP=1, top, peaks, picks, the ≥ 8–12σ counts,
  last, slab, edge, NN, extracted, min, node, markers).
- **Particle sets** with their lineage.
- **Classes and maps:** each Class 3D and Reconstruct particle job's results. Its tooltip already says "each job's
  Results tab shows the same".

This is the most useful tab, but it is long and mixes the particle stage with the refinement results.

**STA kitchen** (`ui/sta_kitchen/`). It takes the whole pane (the rail hides), with its own tool rows and the tile
wall. It is the youngest tab and the closest to the house look; this roadmap leaves it alone except where §2 sends
work to it.

**Jobs** (`jobs_tab.py`, 158 lines). This species' particle-phase jobs (type, instance id, status, "open ↗"), drift
chips when a job's snapshot differs from the species (symmetry, Ø, template, mask), and "Add for this species:" with
ten buttons. The roster already lists these jobs with their species tags, and its "+" already asks for the species
(`ui/pipeline_builder/species_picker.py`).

**Colour.** Species carry an overlay colour from `SPECIES_OVERLAY_COLORS` (`services/models_base.py:354`):
saturated primaries chosen to stand out on a greyscale tomogram. The registry also uses it as chrome: the swatch, and
the dot on every pick-list row. That breaks the rule in `ui/components/species_pill.py` ("Species colors stay on the
canvas overlays they key, never on names"). The swatch picker in the workbench draws its own 36-colour Material
palette (`COLOR_PALETTE`, `template_workbench.py:127`), a second palette for the same field.

## 2. What the tabs repeat

| Function | Where it is now | Where it goes |
|---|---|---|
| A species' jobs, their status, opening one | Jobs tab; the roster | The roster only |
| Adding a job for a species | Jobs tab's ten buttons; the roster's "+" with the species picker | The roster's "+" only |
| Drift (a job's snapshot ≠ the species) | Jobs tab's chips; the job page's species-group tooltips | The job page's tooltips and a mark on the roster's species tag (D4) |
| The selected template and mask | Overview's "Template matching files"; Templates & masks | Templates & masks; Overview keeps one line linking there |
| One row per tomogram | Picks & curation (a group each); Analytics' Tomograms table | One table, on the Tomograms tab (§3) |
| Making a set from picks | Analytics' Make subset (σ cut dialog); the kitchen's Make set | Decided in D5 |
| The list of particle sets | Analytics' Particle sets; the kitchen's set chips and sources | Decided in D5 |
| Class 3D / Reconstruct results | Analytics' Classes and maps; each job's Results tab | The job's Results tab only |
| Opening a tomogram in a viewer | Picks & curation's group icons; the tomogram gallery; the Journey; the viewer chips | The Tomograms row keeps one viewer chip set (`ui/components/viewer_chips.py`), as everywhere else |
| Curating picks | Curate picks (ArtiaX); the napari companion's particle layers, saved to `Curation/napari/` and not read back (roadmap 55 S5) | One Curate action per tomogram; which viewer it opens is D6 |

## 3. The target

Four tabs: **Species · Templates & masks · Tomograms · Scores**, then **STA kitchen**. Jobs is gone.

**The rail.** Each row shows the glyph, the name and "n picks", the species' total from the in-memory overview.
Templates show as a count only when a species has none, marked as missing ("no template").

**Species** (today's Overview, trimmed to what defines the species):
- Name, overlay colour (a 10 px square beside the field, the one place the colour is drawn in chrome), diameter,
  symmetry and notes, in the house field geometry (`ui/job_plugins/_field_styles.py`, the `.cb-select` scale with
  the 132 px label column, the same rows as a job page).
- One line: "template run_it015…_black.mrc · mask …_sphere_d575_s5.mrc → Templates & masks".
- Extraction geometry as three fields of the same geometry. The explanation moves into the section's tooltip; "not
  set" stays as the fields' state.
- The status sentence.
- Delete and Publish as a `[Delete species] [Publish to catalog]` pair, under the sections and with them, not at the
  far right edge.

**Templates & masks**:
- **Two tables:** templates and masks, one above the other at full width, sized to their rows (no fixed 186 px), in
  the job page's Table look. The columns are file (`path_text`), polarity or method, header (Å/px · box · lp),
  stats, size, and the eye and × actions.
- **Selection:** the roster's hairline ring.
- **Viewer:** to the right of the tables in a wide pane, beneath them in a narrow one. It is drawn only once
  something is loaded; an empty viewer is a "no volume loaded" line, not a 380 px black box.
- **Creating:** "New template…" and "New mask…" on the tables' heads open a dialog with the existing forms (Shape,
  PDB / EMDB, Import, Edit current; From template, Sphere, Import). The forms leave the page; the code stays.
- **Activity log:** a folded line under everything.

**Tomograms** (today's Picks & curation, with Analytics' table merged in):
- One table, one row per tomogram, about 22 px high, sorted by position by default and sortable by any column.
- Columns: tomogram (`path_text`), picks, kept, top σ, ≥ 10σ, extracted (✓ / count / –), markers (outlier, no peak,
  edge max, slow, as Analytics names them), the viewer chips, then **Curate**.
- A tomogram with more than one list (candidates plus a hand-made list, a merge) opens to its lists as indented
  sub-rows with today's list columns and actions. A tomogram with one list shows no sub-row.
- **Import picks…** goes on the section head, once: choose files; each `.coords` is matched to its tomogram by name
  and the unmatched ones are listed, never guessed. It replaces 48 import rows.
- **Watcher:** a one-line count on the head ("3 saves read · 1 not attributed") that opens to today's footer.
- Refresh and Extract all pending stay on the head.

**Scores** (today's Analytics without its table and without Classes and maps): Scores in σ, Where the picks sit, and
the overlap count. Particle sets: see D5.

**Colours** (D1):
- Polarity is plain text with a small half-filled square glyph (■ black, □ white) in ink and slate. Method is plain
  mono text.
- Size badges stay grey; the large-file warning keeps its tooltip and gets an amber dot.
- No species dot on rows. The species' overlay colour shows only on canvases (tomogram overlays, the pick viewer,
  napari, the gallery) and in its one swatch.
- The workbench's 36-colour picker goes; the swatch offers `SPECIES_OVERLAY_COLORS` like everywhere else.

## 4. Decisions

**Taken (2026-11-19).**
- D1: (b), one palette everywhere, overlays included, muted from neon but built on strong hues: **Cockpit** (a glass
  cockpit's display colours), picked from four candidates (Cockpit, Reactor, Terminal, Industrial). Slot for slot
  with today's `SPECIES_OVERLAY_COLORS` (red, cyan, yellow, magenta, green, blue, orange, pink):
  `#e8504a #2cc8dc #e3cf3e #d65ad6 #4cc96a #4f8ff0 #f0a030 #ef7aa6`.
- D2: as written. D3: (a), two tables stacked. D4: the roster mark, as M2 (a corner notch on the species tag).
  D5: (a). D8: as written.
- D6: Curate stays ArtiaX; napari sits beside it, disabled, its tooltip saying napari's curation is in development.
- D7: round 1's line glyphs (round 2's bead and density molecules were turned down), the **icosahedral capsid as
  the default** for every species, the other nine to pick from, the nine hues as drawn. The assets are in
  `static/icons/species/` (below).
- The five places S0 differed from §3 ("S0 as built") were not answered one by one; the maintainer called the
  prototype that had them "great" twice. Build them as prototyped.

- **D1. Species colour in the chrome.**
  - (a) Keep the vivid overlay palette for canvases, where it must stand out on grey, and take colour out of the
    chrome: no dots on rows, polarity and method chips neutral, the one swatch on the Species tab.
  - (b) Change the palette itself to muted tones like roadmap 58's class palette (`--cb-class-1..8`), everywhere,
    overlays included.

  Recommended: (a). A muted dot on a greyscale tomogram is hard to see, and the complaint is about the chips, not
  the overlays.
- **D2. Tab names.** Species · Templates & masks · Tomograms · Scores · STA kitchen. Alternatives: keep "Overview",
  and "Picks" for Tomograms. Recommended as written: the tab is a table of tomograms, and "Picks" collides with the
  pick viewer (`View.PICKS`).
- **D3. Templates & masks layout.**
  - (a) Tables stacked, viewer beside them (§3).
  - (b) Templates and masks side by side, viewer full width beneath.
  - (c) One table holding both, masks indented under the template they were made from.

  Recommended: prototype (a) and (c) in S0 and choose by eye, as roadmap 59 did.
- **D4. Where drift shows once Jobs is gone.** On the job page it already does: the species-group tooltip names the
  species default. On the roster: a small slate mark on the species tag of a job whose snapshot differs, the
  differences in its tooltip. CLAUDE.md's "Job parameter tabs" section names the Jobs-tab drift chips as the other
  half of that story; it is rewritten to name the roster mark. Recommended: the roster mark.
- **D5. Sets.**
  - (a) Particle sets are listed in the kitchen only; Scores' σ cut makes a set through the same dialog it opens
    today, which then shows up in the kitchen.
  - (b) Scores keeps its read-only Particle sets list.

  Recommended: (a). One place lists sets, and the cut stays where its histogram is.
- **D6. What Curate opens.** ArtiaX today. The napari companion also curates (particle layers), but its saves are
  not read back until roadmap 55 S5. Until then Curate stays ArtiaX, and the napari chip among the viewer chips
  says in its tooltip that its picks are not read back. After 55 S5: Curate offers both. Recommended: as written;
  this roadmap does not build 55 S5.
- **D7. The species glyph** (asked 2026-11-19: a new default glyph, and a set a species can pick from with a hue).
  Every species shows the default glyph, the icosahedral capsid, in the theme's grey. A species may pick another of
  the set and a muted hue. The glyph and its hue are chrome; the overlay colour stays on the canvases, and the two
  are set apart on the Species tab. The registry's own icon in the workspace rail keeps today's hexagon
  (`static/icons/particle.svg`).
  - The set: `static/icons/species/<key>.svg`, 16-unit grid, one ink (`currentColor`, which `load_icon_svg`
    replaces), keys `capsid` (the default), `ribosome`, `enveloped`, `spike`, `membrane`, `filament`, `tube`,
    `barrel`, `pore`, `lattice`; their names: Icosahedral capsid, Ribosome, Enveloped virus, Spike / trimer on a
    stalk, Membrane protein, Filament (actin), Tube (microtubule), Barrel (proteasome), Ring / pore, Lattice.
  - The hues (key, name, ink): `grey` #94a3b8 (the default, today's glyph grey), `slate` #5f6f86, `steel` steel blue
    #4f7aa8, `verdigris` #3f8a78, `olive` #6f7f3a, `ochre` #b08a2e, `sand` #a8916a, `rust` #b0643a, `oxide` oxide
    red #a14d4a (roadmap 58's muted palette).
  - The picker: a menu from the Species tab's Glyph row (beside Overlay colour): the glyphs in a 6-wide grid of
    36 px tiles, the default first, the current one ringed in the accent; under them the hues as 14 px squares. A
    click sets it at once. The row shows the glyph at 14 px, its name ("icosahedral capsid (default)") and the hue.
  - Model: `ParticleSpecies.glyph` and `.glyph_hue`, "" meaning the default and grey. `render_species_mark`
    (`ui/components/species_pill.py`) draws them, so the rail, the registry's title line and a job page's species
    line follow; the roster's species tag gains the glyph in front of the name; the wall's Layers menu shows it
    beside the overlay colour. CLAUDE.md and `species_pill.py`'s docstring say the mark may carry a chosen muted hue.
- **D8. Picks on the tomogram picture** (asked 2026-11-19: the dots are far too big; with 300 ribosomes a picture
  is mostly dots). The wall and its zoom draw every pick as a filled dot of a fixed size (radius 1.1 px on a tile,
  2 px on the zoom) with a white edge. The wall's Layers menu gains the look, held per user: the mark (dot, ring,
  ring as wide as the species' diameter, cross, point), its size, the white edge on or off, opacity; species on or
  off is already there. Default: a 3 px ring, no white edge, 90 % opacity, which marks a pick without covering the
  density in it. As prototyped: the size is the zoom's radius, 0.5 to 4 px (a tile draws 0.55 of it, as today's
  1.1 against 2); "to scale" is a ring of the species' diameter in the picture's pixels (`diameter_ang / 2 / apix ×
  picture width / tomogram width`); a ring or cross is 1 px on the zoom, 0.8 on a tile; the wall still draws at most
  260 picks per species per tile (`_WALL_DOTS`). The pick viewer's dots (`.cb-pick-ghost`) are not part of this.

## 5. Stages

Each stage is one commit and leaves the page working. Every stage is checked by screenshots of the real page
(`scripts/dev_screenshot.py --url`, or the headless WebEngine route used for §1), before and after, on
`AGG412_20260311_MyrGFP_Grid2_rerun` and on a project with a hand-made list and a merge.

**S0. Prototypes** (`/dev/registry-ideas`, read-only on real projects, as `/dev/job-page-ideas`). Templates & masks
in D3's (a) and (c); the Tomograms table at 48 rows, with one tomogram holding three lists; the colour rules of D1
on both. The maintainer chooses; nothing in the page changes.

**S1. Colours.**
- `ui/components/chip.py`: polarity and method as neutral text tags.
- `ui/template_workbench.py`: the selection ring becomes the roster's `cb-sel`; `_INDIGO` / `_PURPLE` and
  `COLOR_PALETTE` go; the swatch is `ui/components/color_swatch.py`.
- `ui/species/picks_tab.py`: no species dot on rows.
- The size badge gets the amber dot.
- The palette picked under D1 (Cockpit, §4) replaces `SPECIES_OVERLAY_COLORS`. A species' stored colour moves to
  the same slot of the new palette, once, by a migration, so no species changes hue family; the swatch, the gallery,
  the pick viewer, napari's layers and the kitchen read the new colours.

Check: no saturated colour left in the registry outside the one swatch and the canvases; each species keeps its hue
family across the migration.

**S2. Jobs tab out.**
- `ui/species/jobs_tab.py` deleted.
- `TABS` (`page.py`) and `SPECIES_TABS` (`services/routes.py`) lose it, and `/species/<id>/jobs` lands on the Species
  tab.
- The drift messages (`services/particles/species_jobs.py`) feed the roster's species tag (D4).
- Links that sent people to the Jobs tab (the Analytics empty state "add them from the Jobs tab", `species_select_tab`
  callers) point at the roster instead.
- CLAUDE.md's "Job parameter tabs" text is updated.

Check: every particle job's tag shows its drift mark, matching what the Jobs tab showed before the change (compare
on a project where TM symmetry ≠ species symmetry).

**S3. Species tab (Overview trimmed).**
- The house field geometry for the identity editor and extraction geometry.
- "Template matching files" becomes the one-line link.
- The actions move under the sections.
- The rail's meta becomes "n picks".

Check: an edit saves exactly as today (`set_species`, debounced); the rail count equals the status sentence's.

**S4. Templates & masks (D3).**
- The tables in the Table look.
- "New template…" and "New mask…" open the creation forms in a dialog, moved from the page and otherwise unchanged.
- The viewer beside the tables (or as chosen), drawn on demand.
- The log folded.

Keep the workbench's invariants: one `TemplateWorkbench` per species, never rebuilt (the molstar iframe and its
message listener). Check: make a template from a shape and from a PDB, a mask from a template, delete one, and load
each in the viewer, on both species.

**S5. Tomograms.**
- One table from `picks_tab._compute` joined with Analytics' per-tomogram rows (`analytics_tab._compute` already reads
  them off the loop). The join is a pure function in `services/particles/`, so the API's species reads can use it.
- Sub-rows for tomograms with several lists.
- Import picks… with matching by name.
- The Watcher folded.
- Analytics' table goes.
- Each row ends "curate in [ArtiaX] [napari]": ArtiaX opens the tomogram with the species' picks; napari is
  disabled, its tooltip saying its curation is in development (D6). The viewer chips keep napari for looking.

Check: every per-list action of today (curate, import, extract, re-extract, delete, dedup inspect, logs) still works
from a sub-row; a 48-tomogram species fits in one screen; Extract all pending submits the same lists as before.

**S6. Scores.** Analytics renamed. Classes and maps removed (the job Results tabs keep it). Particle sets per D5.
Check: the σ cut and Make subset produce the same set as before.

**S7. The species glyph (D7).** The glyph set is already in `static/icons/species/`; `glyph` and `glyph_hue` on
`ParticleSpecies`, written through `set_species` (and so
the API's `set_species`); the Glyph row and its picker on the Species tab; `render_species_mark` and the roster's
species tag draw them. Check: pick a glyph and a hue on one species and see them in the rail, the title line, the
roster and a job page; a species that never picked shows the default in grey.

**S8. Pick marks on the wall (D8).** The look in the wall's Layers menu, held in the user's prefs; `_dots_svg`
(`ui/tomo_gallery.py`) draws the mark asked for, on the tiles at 0.55 of the zoom's size as today. Check: Grid2's
Position_6 (1,321 picks) and a tomogram with about 300, at each mark.

### Pickup for the build (2026-11-19)

Every decision is taken (§4 "Taken"); S1–S8 are the build, in that order, back to back, one commit each, checked
once at the end. Read §1–§5 first, then CLAUDE.md's UI rules.
- **Where.** Other sessions keep uncommitted work in the main tree, some of it in files these stages touch
  (`ui/pipeline_builder/pipeline_roster.py`, `species_picker.py`, `ui/workspace_page.py` on 2026-11-19). Run
  `git status` first; build in a worktree of its own if those files are still dirty.
- **S1** carries the palette: Cockpit replaces `SPECIES_OVERLAY_COLORS` (`services/models_base.py`), and a one-off
  migration moves each species' stored `color` to the same slot (a colour outside the old palette stays as it is).
  Everything that draws the overlay colour reads the new values: the gallery, the pick viewer, napari's layers
  (`services/visualization/napari_bundle.py`), the kitchen, the Species tab's swatch.
- **S2** puts drift on the roster as M2, a corner notch on the species tag (the prototype's `.cbri-tag.notch`: a
  5 px slate triangle in the tag's top-right corner), the differences in the tag's hover; CLAUDE.md's "Job parameter
  tabs" text changes with it.
- **S4** is D3 (a). **S5** ends each row with "curate in [ArtiaX] [napari]", napari disabled (D6).
- **S7** adds `glyph` and `glyph_hue` to the API's `set_species` as well (the API mirrors every UI action), and
  `species_pill.py`'s docstring and CLAUDE.md's species-mark text say the mark may carry a chosen muted hue.
- **Check** with screenshots of the real page, before and after (`scripts/dev_screenshot.py --url`, or
  `~/.crboost/ui_shots/shoot.sh`), on `AGG412_20260311_MyrGFP_Grid2_rerun`, `AGG412_STA` (drift) and
  `MysteryLattice` (one tomogram with six lists).

### S0 as built (2026-11-19), retired the same day

The prototype is gone from the tree (with every other `/dev/*ideas` page, at the maintainer's word); what was decided
on it is in §4 and §5, the glyphs in `static/icons/species/`. Its code is in git: `94262f0` (rounds 1 and 2:
frame, Species tab, Templates & masks, Tomograms, chips, drift marks) and `e0cde47` (round 3: palettes, the glyph
picker, the pick-mark controls, the molecules that were turned down); `git show <commit>:ui/registry_ideas/page.py`.
The Tomograms table's join (`data._tomos`) and its HTML (`parts.tomo_table_html`) are the closest thing to S5's.

`/dev/registry-ideas` (`ui/registry_ideas/`, route in `ui/main_ui.py`), on three projects read and never written:
`?project=grid2` (AGG412 Grid2 rerun: 48 tomograms, one list each, two species), `sta` (AGG412_STA: three 412
templates, Class 3D and Reconstruct jobs at C1 against the species' I1) and `mystery` (MysteryLattice: one tomogram
with six lists). Further switches: `species`, `pane=wide|narrow` (1060 / 720 px), `tm=a|c`; `reload=1` reads again.
Nothing saves; an action says what it would do.

- **A.** The registry at the workspace's width: the title line with the five tabs (D2), the rail counting picks
  ("no template" in amber ink when a species has none), the Species tab (S3). The tabs work.
- **B.** Templates & masks, D3 (a) and (c). The eye loads the volume's projections into the viewer column (a stand-in
  for molstar); New template… and New mask… open the forms in a dialog.
- **C.** Tomograms: sortable by any column head, sub-rows for a tomogram with several lists, Import picks… once on
  the head.
- **D.** Colour, D1: the chips today and proposed; the five palettes (`?palette=` puts one on the page), each with
  Grid2's densest tomogram's 1,321 picks dealt to its eight colours in turn and a typical tomogram (Position_8_4)
  with each species in its own colour, drawn as G's rings.
- **E.** Drift on the roster, D4, in two marks: M1 "≠" inside the species tag, M2 a corner notch.
- **F.** The species glyph, D7. Round 2 (`ui/registry_ideas/molecules.py`): globule and trimer as default
  candidates beside today's hexagon, and eleven molecules (ribosome, icosahedral capsid, enveloped virus,
  glycoprotein spike, membrane protein, actin filament, microtubule end-on, proteasome, nuclear pore, nucleosome,
  hexameric lattice), each a coarse-grained bead model drawn as `?gstyle=density` (the model's outline and, where
  it helps, its core) or `beads`; `line` is round 1. The picker with the hues sits beside the places the mark is
  drawn; a pick there or on the Species tab's Glyph row (A) changes the frame too.
- **G.** Picks on the tomogram picture, D8: today's dots and dots drawn with the controls (mark, size, opacity,
  white edge, palette, species), on a wall tile and on the zoom, for Grid2's densest tomogram and one with about 300
  picks.

Places where the prototype differs from §3, for the maintainer to accept or turn down:
- The tomogram column shows the position name (`Position_10_2`), the full name on hover, not `path_text`: the
  project prefix is the same on every row.
- Stats show σ only, min and max on hover; the amber dot marks an unusual σ.
- The template dialog has PDB and EMDB as two tabs, so each tab has one action in the footer.
- Curate is 17 px tall so that it fits a 22 px row.
- With one list per tomogram, which list it is gets said once above the table rather than on each row.

### Build log (2026-11-19)

Built in a worktree of its own (`crboost_server_registry`, detached at `de0ad82`), one commit per stage.

- **S1.** `SPECIES_OVERLAY_COLORS` is Cockpit; `current_overlay_color` (`services/models_base.py`) moves a colour
  of the old palette to the same slot, on every project load (species and segmentation features, which draw from
  the same palette) and on a catalog import; any other colour stays. Polarity is a word behind a 7 px square,
  method plain mono (`ui/components/chip.py`). The workbench: selection is `cb-sel` (the roster's hairline ring),
  `_INDIGO` / `_PURPLE` / `COLOR_PALETTE` gone (the viewer's item colours offer the overlay palette), σ alone
  with min and max on hover, an amber dot on an unusual σ or a large file, red on a constant volume. The swatch
  is a 10 px square and its value. Pick-list rows lose the species dot.

## 6. Earlier rounds

The August round on the same surfaces (picking_ui 03–06, rows P-13 to P-31 of
[particles-ux-peeves](roadmap_particles-ux-peeves.md)) gave the Overview its three blocks, the workbench its
side-by-side columns with fixed-height lists, the three type ranks and the `.cb-field` inputs. This roadmap
replaces those layouts; its rules (no Quasar tabs or fields, labels to their values, no repeated facts) still hold.
The fixed-height lists were added so two side-by-side columns stayed aligned; with the tables stacked (S4) they lose
their reason.

## 7. Not in this roadmap

The STA kitchen's own layout; the pick viewer page (`View.PICKS`); napari's ingest (roadmap 55 S5); the job page
(roadmap 59).
