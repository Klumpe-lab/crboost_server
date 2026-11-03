---
name: project-title-line-tools-2026-10-26
description: "2026-10-26 header pass + shared TS selection + one layers list: title-line hover tools; wall↔Journey selection; picks+segmentations in one list with pin-based follow-the-version; Particles panel = version/layers/list tools; NOT RUN"
metadata:
  node_type: memory
  type: project
  originSessionId: d7e7c0dd-2f15-471f-8a26-bf8b881bfe8c
  modified: 2026-10-26T13:09:31.158Z
---

Built 2026-10-26 on branch `roadmaps`, ruff-clean, NOT RUN (no python in the sandbox).

Pass 1 (title lines):
- `ui/components/hover_menu.py` (new): `hover_menu(trigger)` = QMenu opened client-side on hover
  (`runMethod(id,'show')`, shared `window.__cbHoverMenu` timer, 110 ms open / 240 ms close), `ChoiceRows`
  (single-choice rows, groups, `set_active` in place), `tool_trigger` (glyph + optional label + caret).
- `ui/workspace_page.py` `_view_frame`: glyph (`_VIEW_ICONS`) + name + `lead`/`tools` slots;
  `callbacks["view_head"](name)`. Roster "Jobs" line got the layers glyph + ×. Titles = IBM Plex Sans Condensed 13 px.
- Tomograms: Tomograms/Tilts switch replaces the title; tools = version (grouped by reconstruction), picks,
  segmentations (Import inside), S/M/L, refresh. Colour pickers are inline palette rows (nested menu would close
  a hover menu). Journey: `strip.py` builds into the title line; switcher = search + "Stage position N" groups,
  one line per TS; stage dots gone; species counts in ⓘ.

Pass 2 (same day, after feedback "colours on the dropdowns look ugly", "bottom row = useless info"):
- Legend dots/squares removed from the picks/segmentation triggers (`legend_marks` deleted).
- Tiles: no 3dmod chip, no pick badge, no caption numbers/segmentation fractions — caption = position + Journey
  glyph. Zoom got the pick badge and the tilts summary (`tomogram_caption_html(group)`, `compact` dropped).
- Shared selection: workspace hub `callbacks["select_ts"](ts, origin)`, `["selected_ts"]()`,
  `["follow_ts"][view]`. Wall: click selects, click on the selected tile zooms (cursor pointer → zoom-in);
  zoom ← → move the selection. Tilts tab (`TiltGallery(on_select=)`, `set_selected`, `scroll_to_selected`):
  header / beam line / tilt click selects. Journey follows after 0.3 s rest (`_FOLLOW_SETTLE_S`), deferred while
  hidden; publishes from `select_ts`/`_show_ts`/build. Scroll = `tilt_previews.scroll_into_view` (nearest
  scroll box only), on follow, tab switch, size, view activation, any re-render.

**Why:** maintainer wants to browse the wall with the Journey beside it, without clutter. **How to apply:**
runtime check owed; particles-curation feedback was announced as next.
Related: [[project_style_themes_2026-10-22]], [[project_tomo_gallery_view]], [[feedback_ui_chrome_conventions]].

Pass 3 (same day, "standardize the Particles curation panel"):
- One layers list everywhere (`ui/components/layer_menu.py`: `LayerRow`, `render_layer_list`, `layers_tool`):
  picks by species + segmentations, "Follow the version", species head toggles all its layers. Wall's two
  tools merged into one layers glyph; zoom has one Layers list; Reconstruct card + Particles use `layers_tool`.
- Visibility model = pins (`services/segmentation/display.py`): `is_shown(id, following)`; following =
  `on_source(layer recon, picture recon)`; a hand switch pins (dropped when equal to following); follow row
  unpins. Shared by id across views (TM layer id = CE iid on wall and canvas; canvas list id
  `list:<species>:<slug>`). Segmentations can now be pinned on over another recon (flagged), like picks.
- Particles panel: species tabs + list table gone. Title line = version tool (`_canvas_versions`: recon per
  reconstruct job + denoised, key = slab dir, `_CANVAS_VERSION`), layers tool, list tool (grouped by
  species, `_ACTIVE_SPECIES`) + the shown list's count/ext/copy/delete. Slabs render from the chosen
  version (`tomo_geometry.volume_dims`).
- Follow-up: species colour = `dashboard_data.species_color(species, plan idx)` on wall, Journey canvas and
  roster (wall had per-layer palette slots, so colours differed); wall pick-layer recolour removed.
  Particles header shows only the red missing-dims/apix markers; cutout tiles lost the "#N" sort-rank tag.

**2026-10-27:** the Particles panel tools left the title line: version + layers over the slabs, list over the
gallery, boxed (see [[project_pane_focus_particles_tools_2026-10-27]]).
