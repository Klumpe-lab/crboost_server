# Template & Mask First-Class Objects — v2 Plan

_Drafted 2026-05-10, supersedes `TEMPLATE_REGISTRY_REFACTOR_PLAN.md` (v1).
v1 framed this as a schema-cleanup. v2 reframes it around the two concrete
pains we feel today: (a) templates and masks have no first-class identity
or metadata anywhere outside the workbench, so picking and post-hoc
diagnosis are blind; (b) the molstar-based workbench viewer is broken and
visually outdated, and any first-class-metadata UI we add will require
rebuilding it anyway._

## What we're solving (today)

**Pain 1 — templates dissolve into bare filepaths the moment the
workbench closes.** The only surface where templates are introspectable —
where you can see what box you created them at, what apix, whether the
mask is sensible, what lowpass was applied — is the
`ui/template_workbench.py` widget. Once you leave it, every other
surface (TM job-config tab, candidate-extract config, the per-tomo
dashboard, the pixel-sanity panel) treats the template as an opaque path
string. Polarity is filename-encoded magic (`_white.mrc` / `_black.mrc`).
There is no provenance trail (PDB/EMDB id, when it was rendered, who
filtered it), no tags, no notes.

This causes two concrete failures:

1. **Pre-flight blind picks.** When you set up a TM job and choose a
   template, you see a path. You can't see "this template was rendered
   at 5.4 Å/px, box 64 — your tomo is binned to 8 Å/px, you're going to
   under-sample." That's the kind of mismatch that under/overpicks for
   reasons that are obvious in hindsight and invisible at submission.
2. **Post-hoc diagnostic blindness.** When picks are bad, the dashboard
   sanity panel (per `services/visualization/ROADMAP.md` §11) tries to
   reconstruct what the template *was* — `_resolve_species` walks
   `instance_id.split("__")`, `_read_template_apix_box` reaches around
   to the MRC header, `species.workbench.pixel_size` is usually 0. The
   forensics work but only by accident; they should be a one-liner read
   off the species/template object.

**Pain 2 — the molstar workbench viewer is broken and ugly.** The
viewer in `ui/template_workbench.py:73 (molstar_workbench_viewer)` is
the only place we visualize templates and masks today. It doesn't render
reliably and looks dated. Since first-class template metadata UI will
need a viewer surface anyway (slice triplet, mask overlay, "is this
template sane" inspection), we should rebuild it as part of this work
rather than glue more onto the broken one.

These pains compound: the workbench tries to be the entire template
introspection story, but its viewer is broken; meanwhile every other
surface is blind. Fixing this means making templates first-class
infrastructure objects (so all surfaces can show metadata), and giving
them a viewer that works.

## Goal

Templates and masks become first-class objects in the project model:

- Persistent identity, metadata, and provenance independent of any one
  job's parameters.
- Visible at every surface that references them (TM config, candidate
  extract config, pixel sanity panel, per-tomo dashboard) with the
  metadata that matters for that surface.
- A working slice viewer that replaces the broken molstar mount.
- Survive into post-hoc analytics so "did I mismatch box vs binning"
  becomes a one-line read, not an MRC-header treasure hunt.

## Target schema

Singular per species (we agreed not to abstract over multi-template
until we have a concrete second-template need). No persisted MRC cache
(read on demand; per-process LRU keyed by `(path, mtime)` lives in the
viz layer, not in the persisted model).

```python
class TemplateMask(BaseModel):
    """A mask volume associated with a specific template. Same box
    and apix as its template (enforced by sanity check, not by
    field duplication)."""

    mask_path: str
    mask_type: Optional[Literal["spherical", "cylindrical", "auto", "manual"]] = None
    notes: str = ""


class ParticleTemplate(BaseModel):
    """A specific template volume + its metadata. One per species."""

    template_path: str
    polarity: Literal["white", "black"] = "black"   # was filename magic
    lowpass_resolution_ang: Optional[float] = None  # if filtered

    # Provenance (all optional — populated as known)
    source: Optional[str] = None    # "PDB:6Z6J" / "EMDB-1234" / "custom"
    created_at: Optional[datetime] = None
    notes: str = ""

    mask: Optional[TemplateMask] = None

    # Read on demand from MRC header — NOT persisted
    # def pixel_size_ang(self) -> Optional[float]: ...
    # def box_px(self) -> Optional[int]: ...


class ParticleSpecies(BaseModel):
    id: str
    name: str
    color: str = "#3b82f6"

    # Particle-intrinsic (not per-job)
    diameter_ang: Optional[float] = None    # was: CandidateExtractPytomParams.particle_diameter_ang
    symmetry: str = "C1"                    # was: TemplateMatchPytomParams.symmetry
    notes: str = ""

    template: Optional[ParticleTemplate] = None

    # Pure UI widget state — survives schema migrations independent of
    # particle metadata
    workbench_ui: TemplateWorkbenchUIState = Field(default_factory=TemplateWorkbenchUIState)
```

Job-side changes:

| Job | Remove | Keep | Notes |
|-|-|-|-|
| `TemplateMatchPytomParams` | `template_path`, `mask_path`, `symmetry` | `angular_search`, all PyTOM flags | Reads template/mask via `species.template`. Stamp `template_path`/`mask_path` *resolved* values onto driver context at submission so historical jobs survive species edits |
| `CandidateExtractPytomParams` | `particle_diameter_ang` | cutoff/threshold params | Reads diameter via `species.diameter_ang` |
| `SubtomoExtractionParams` | (no change) | `box_size`, `crop_size`, `binning` | These are subtomo-job decisions; relation to `template.box_px` is a sanity check, not a duplication |

Read-side rules:
- `template_path`/`mask_path` resolve through `species.template` (singular).
- `template.pixel_size_ang` and `template.box_px` are computed from the
  MRC header (cached in viz layer, not persisted).
- `species.diameter_ang` is the single source for the sanity panel's
  particle-diameter check.
- `species.symmetry` is the single source for symmetry.

## UI surface map — where templates show up with metadata

This is where v2 diverges most from v1. v1 talked about schema; v2 says
where in the UI templates become visible.

### A. Template summary card (new) — `ui/template_card.py`
Reusable component displaying one template's metadata in compact form:
name (species name), apix, box, polarity, mask presence, lowpass,
source, modified date, notes. Used wherever a template is referenced.
Click → opens the detail panel.

### B. TM / candidate-extract job-config tabs
Currently `ui/job_plugins/template_match.py` renders raw path inputs at
lines 39–48. After: drop the path inputs, embed the template summary
card (read-only, shows what's resolved from the species), plus an "edit
in workbench" link. User no longer types a path; if they need a
different template, they edit the species.

### C. Pixel-sanity panel (per-tomo dashboard, ROADMAP §11)
Reads `species.template.{pixel_size_ang, box_px}` directly via MRC
header. Drops the `_resolve_species` `instance_id.split("__")` walk and
the `species.workbench.pixel_size` fallback. Box-vs-binning warnings
name the template by species name and the resolved apix, not "the
template at /lustre/.../some_path.mrc."

### D. Workbench (`ui/template_workbench.py`)
Stripped down. Becomes the *editor* — create/import a template, attach
a mask, set lowpass, edit notes/source. No longer the *only* place
templates exist; the picker (B) and sanity panel (C) read its outputs.
The molstar viewer mount gets replaced (next section).

### E. Species workbench panel (`ui/species_workbench_panel.py`)
Lists species with their template summary card embedded — at a glance
you see "ribosome: 5.4 Å/px box 64 black, mask: spherical 250 Å,
lowpass 30 Å, from PDB:6Z6J" without opening anything.

## Viewer replacement

Mosltar (sic — actually molstar) is broken and visually outdated.
Replace with a slice-triplet renderer suited to small, fully-loaded
template volumes.

**Important constraint** — per memory, we don't render *tomogram*
volumes in the browser (missing-wedge streaks, Lustre latency). That
constraint does **not** apply here: templates are 64–128 px boxes,
~1–8 MB total, dense, no missing wedge. Browser-renderable cleanly.

Three options, recommendation in bold:

1. **NiceGUI + Plotly heatmap triplet (recommended).** Three plotly
   panels (XY/XZ/YZ at center, slice slider per axis), mask overlay
   toggle, isovalue slider for thresholded view. Reuses our existing
   plotly-via-dict pattern (per `feedback_visualization_patterns.md`).
   Ugly is solvable with CSS; broken isn't. Lowest-risk path.
2. NiceGUI scene + custom three.js volume. Pretty, full 3D, but real
   build effort and we already chose to not browser-render tomograms
   for related reasons. Defer.
3. 3dmod handoff button only (no in-browser viewer). Consistent with
   how we handle big volumes elsewhere. Likely too coarse for the
   fast-iteration template-tuning loop the workbench is for.

Recommend (1) as the primary in-browser viewer and (3) as a "open in
3dmod" companion button for full inspection. Drop molstar.

Module: new `ui/components/template_viewer.py`. Old molstar bridge in
`template_workbench.py` (lines 73, 691–694, 1006–1045) gets deleted in
the same change.

## Migration (additive, staged — not big-bang)

Bump `SCHEMA_VERSION` to `(2, 0)`. Stage in three PRs, not one.

**PR 1 — additive schema land.** Add `ParticleTemplate`, `TemplateMask`,
`species.template`, `species.diameter_ang`, `species.symmetry`,
`species.workbench_ui`. Keep all v1 fields present. Write
`_migrate_v1_to_v2` (idempotent, guarded by schema version). Save a
`project_params.json.v1.bak` snapshot before migration writes back.
Dual-read in `path_resolution_service.py`: prefer `species.template`,
fall back to v1 fields.

**PR 2 — UI surfaces flip.** Template summary card (A), job-config
embedding (B), species workbench panel embedding (E), pixel-sanity
panel reads (C). Workbench keeps molstar for now.

**PR 3 — viewer + workbench cleanup.** New `template_viewer.py` ships,
molstar bridge deleted, workbench rewired. v1 fields deleted. Dual-read
shim removed. v2-only.

Each PR is independently shippable; if PR 3 takes longer than expected
the project is still in a strictly-better state at PR 2.

Migration sketch (idempotent, version-guarded):

```python
def _migrate_v1_to_v2(data: dict) -> None:
    if tuple(data.get("schema_version", (1, 0))) >= (2, 0):
        return
    species_by_id = {s["id"]: s for s in data.get("species_registry", [])}

    for sid, sp in species_by_id.items():
        wb = sp.get("workbench", {})
        if sp.get("template_path") and not sp.get("template"):
            polarity = "white" if "_white" in sp["template_path"] else "black"
            sp["template"] = {
                "template_path": sp["template_path"],
                "polarity": polarity,
                "lowpass_resolution_ang": wb.get("template_resolution"),
                "mask": {"mask_path": sp["mask_path"]} if sp.get("mask_path") else None,
            }
        sp.setdefault("workbench_ui", {
            k: wb.get(k) for k in ("auto_box", "apply_lowpass", "basic_shape_def", "auto_infer_seed")
            if k in wb
        })
        # Leave sp["workbench"], sp["template_path"], sp["mask_path"] in place during PR 1.
        # Removed in PR 3.

    for iid, jm in data.get("jobs", {}).items():
        sid = jm.get("species_id")
        if not sid or sid not in species_by_id:
            continue
        sp = species_by_id[sid]
        if jm.get("job_type") == "tmextractcand" and "particle_diameter_ang" in jm:
            sp.setdefault("diameter_ang", jm["particle_diameter_ang"])
        if jm.get("job_type") == "templatematching" and "symmetry" in jm:
            sp.setdefault("symmetry", jm["symmetry"])
        # Leave jm fields in place during PR 1.

    data["schema_version"] = [2, 0]
```

## Carryover from v1 — explicit

**Kept (refined):**
- Pull `template_path`/`mask_path` off TM job onto species ✓
- New `ParticleTemplate` model ✓ (singular, not list)
- Pull `particle_diameter_ang` off Pick job and `symmetry` off TM job onto species ✓
- Separate `TemplateWorkbenchUIState` from particle metadata ✓
- Schema bump v1→v2 with idempotent migration ✓
- Compatibility shim in `path_resolution_service.py` (now: dual-read for one PR cycle, not permanent) ✓

**Cut from v1:**
- `templates: list[ParticleTemplate]` → `template: Optional[ParticleTemplate]` (singular).
  Defer multi-template until a concrete second-template case appears.
- Persisted `pixel_size_ang` / `box_px` cache → drop. Read MRC header
  on demand; cache in the viz layer keyed by `(path, mtime)`.
- "Primary template by index 0" / `template_id: Optional[str]`
  selector → not needed in singular model.
- Big-bang one-PR migration → staged into PR 1/2/3 above.
- Open-question 2 (project-wide template registry across species) →
  defer indefinitely.
- Open-question 3 (auto-discovery from `templates/<species>/` dir) →
  defer; nice-to-have once the schema lands.

## Out of scope

- Multi-template per species. Defer until a concrete second-template
  case appears. The schema can grow `template` → `templates` later;
  migration is straightforward.
- Project-wide template asset registry (templates shared across species).
- Filesystem auto-discovery of new `.mrc` files as templates.
- Automated "re-render template at recon px" via PyTOM downsample.
  List as a future Slice E action; the schema unblocks it but it's not
  in this scope.
- Tagging UI (filterable `tags: list[str]`). Start with `notes` free-form
  text; promote to tags if usage justifies it.

## Open decisions before code starts

1. **Mask as sub-object of template, or its own first-class species
   field?** This plan assumes sub-object (a mask is template-shaped and
   only meaningful relative to its template). Alternative: mask
   alongside template on species. Recommend sub-object.
2. **Polarity as a `Literal["white","black"]` field, or keep filename
   convention?** This plan promotes to a field (filename becomes
   convenience, not state). Recommend the field.
3. **Workbench role after refactor.** Pure editor (create/import/edit
   one template) vs. continues to do mask-painting and lowpass
   preview. Recommend pure editor for now; mask-painting workflows
   stay where they are but read/write through the new schema.
4. **Viewer technology.** Recommend NiceGUI + plotly heatmap triplet
   (option 1 above). Confirm or push for three.js.

## Phasing summary

- PR 1: schema land + migration + dual-read. No UI change yet.
  Estimate: 1 day.
- PR 2: UI surfaces flip — summary card, job-config embedding,
  sanity-panel reads, species panel embedding. Estimate: 1 day.
- PR 3: viewer replacement + workbench cleanup + v1-field deletion.
  Estimate: 1–2 days (most of which is the viewer).

Total: 3–4 days. Land before adding more rules to the pixel-sanity
panel — several rules become one-liners after PR 2.
