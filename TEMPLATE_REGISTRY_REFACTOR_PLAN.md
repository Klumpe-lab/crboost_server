# Template Registry Refactor — Plan

_Drafted 2026-05-08 in response to a session where the dashboard pixel-sanity
panel kept tripping over template-metadata wiring (workbench.pixel_size = 0,
species linkage missing on bare instance_ids, particle diameter only on Pick).
This is a **planning doc, not a code change** — it surveys the current mess
and proposes a target schema. No implementation yet._

## What's broken today

Template metadata is spread across four places, each authoritative for a
different field, and none authoritative for the whole picture:

| Field | Lives on | Source-of-truth status |
|-|-|-|
| `template_path` | `ParticleSpecies` AND `TemplateMatchPytomParams` | **Duplicated** — TM job takes precedence at runtime, species copy is for the workbench widget |
| `mask_path` | `ParticleSpecies` AND `TemplateMatchPytomParams` | Duplicated, same as above |
| Template `pixel_size` (Å) | `species.workbench.pixel_size` | **Often 0.0** in real projects (workbench widget rarely sets it). MRC header is the actual canonical source |
| Template `box_size` (px) | `species.workbench.box_size` (default 96) | Same: workbench may be unset; MRC header is canonical |
| `particle_diameter_ang` (Å) | `CandidateExtractPytomParams` | Lives on the Pick job — but it's a *particle* property, not a Pick-job decision |
| `symmetry` | `TemplateMatchPytomParams` | Same: belongs to the particle, not to the TM job |
| `angular_search` (°) | `TemplateMatchPytomParams` | Genuinely a TM-job decision — fine where it is |
| Template lowpass cutoff | `species.workbench.template_resolution` | Workbench widget state |
| Template polarity (white/black) | Encoded in *filename* (`_white.mrc` / `_black.mrc`) | Filename-based magic, not a real field |
| Provenance (PDB/EMDB ID, source) | None | Not tracked |
| Created/modified timestamp | `Path.stat().st_mtime` | Implicit; never persisted |

The `TemplateWorkbenchState` model mixes two distinct concerns:
- Genuine UI widget state: `auto_box`, `apply_lowpass`, `auto_infer_seed`,
  `basic_shape_def`. These are fine on the workbench — they describe how
  the workbench widget should behave.
- Persistent particle metadata: `pixel_size`, `box_size`,
  `template_resolution`. These are properties of the template itself and
  should be a first-class field on the species or on a `ParticleTemplate`
  entity.

## Failure modes this causes

1. **Species linkage fails on bare instance_ids.** Real projects use bare
   `templatematching` (no `__copia` suffix) and set `species_id` on the
   job model. Anything that walks `instance_id.split("__")` to find the
   species silently misses, so the species's metadata is "lost."
2. **`workbench.pixel_size = 0` is the common case** because the workbench
   widget hasn't been touched in months (user's own description). Anything
   that reads `species.workbench.pixel_size` for sanity checks gets
   nothing useful and skips the check.
3. **`template_path` and `mask_path` duplication** means edits made
   through one path (workbench widget vs. job-config tab) can drift apart;
   the source-of-truth is whichever was edited last.
4. **Particle diameter mismatch between Pick instances of the same
   species** is currently catchable but not preventable — there's no
   single canonical diameter for the species.
5. **Symmetry on the TM job** means if a user runs two TM passes on the
   same species (different angular searches), they could trivially set
   different symmetries and get inconsistent picks — symmetry is a
   particle property, not a TM decision.
6. **No template provenance** — if picks are bad, you can't tell which
   template was used for that TM run after the fact (only the path
   remains; if the path got overwritten, the history is gone).

## Proposed target schema

Make `ParticleSpecies` the source-of-truth bundle for everything intrinsic
to the particle, and pull a new `ParticleTemplate` model out of the
workbench so each species can carry one or more templates with full
metadata.

```python
class ParticleTemplate(BaseModel):
    """A specific template volume + its metadata. A species may have
    several (different lowpass cutoffs, white/black polarity, etc.)."""

    id: str                                    # slug: "ribosome_lp45_black"
    template_path: str                         # absolute path to .mrc
    mask_path: Optional[str] = None

    # Properties read from the MRC header on registration. Cached here so
    # the dashboard doesn't need to re-open the file every render.
    pixel_size_ang: Optional[float] = None     # voxel_size.x from header
    box_px: Optional[int] = None               # nx/ny/nz from header

    # Preparation / filters
    lowpass_resolution_ang: Optional[float] = None  # if filtered
    polarity: Optional[Literal["white", "black"]] = None

    # Provenance
    source: Optional[str] = None               # "PDB:6Z6J" / "EMDB-1234" / "custom"
    created_at: Optional[datetime] = None
    notes: str = ""


class ParticleSpecies(BaseModel):
    id: str
    name: str
    color: str = "#3b82f6"

    # Particle-intrinsic properties (the things that should NOT be on
    # any one job's params)
    diameter_ang: Optional[float] = None       # was: CandidateExtractPytomParams.particle_diameter_ang
    symmetry: str = "C1"                       # was: TemplateMatchPytomParams.symmetry
    notes: str = ""

    # Templates registered for this species. Index 0 is "primary".
    templates: list[ParticleTemplate] = Field(default_factory=list)

    # UI widget state — kept SEPARATE from particle metadata above
    workbench_ui: TemplateWorkbenchUIState = Field(default_factory=TemplateWorkbenchUIState)


class TemplateWorkbenchUIState(BaseModel):
    """Pure widget state — not particle metadata. Survives a workbench
    refactor without affecting downstream sanity checks."""

    auto_box: bool = True
    apply_lowpass: bool = False
    basic_shape_def: str = "550:550:550"
    auto_infer_seed: bool = True
    selected_template_id: Optional[str] = None  # which template the widget is editing
```

Job-side changes:

| Job | Remove | Keep | Add |
|-|-|-|-|
| `TemplateMatchPytomParams` | `template_path`, `mask_path`, `symmetry` | `angular_search`, all PyTOM flags | `template_id: Optional[str]` (selects which `species.templates[*]` to use; defaults to primary) |
| `CandidateExtractPytomParams` | `particle_diameter_ang` | cutoff/threshold params | (none) |
| `SubtomoExtractionParams` | (no change) | `box_size`, `crop_size`, `binning` are subtomo-job decisions | (none) |

Read-side rules:
- Template path/mask come from `species.templates[template_id or 0]`.
- Template apix/box come from the same template's cached fields, with a
  fallback to the MRC header if the cache is empty (single source of
  truth: MRC header; `ParticleTemplate` is a cache).
- Particle diameter for sanity checks comes from `species.diameter_ang`,
  not from any one Pick instance.
- Symmetry comes from `species.symmetry`.

## Migration

Bump `SCHEMA_VERSION` to `(2, 0)`. In `ProjectState.load()`:

```python
def _migrate_v1_to_v2(data: dict) -> None:
    """Lift template/particle metadata from job models + workbench onto
    the species. Idempotent."""
    species_by_id = {s["id"]: s for s in data.get("species_registry", [])}
    for sid, sp in species_by_id.items():
        wb = sp.get("workbench", {})
        templates = sp.setdefault("templates", [])
        if not templates and sp.get("template_path"):
            templates.append({
                "id": sid,  # or sid + "_primary"
                "template_path": sp["template_path"],
                "mask_path": sp.get("mask_path"),
                "pixel_size_ang": wb.get("pixel_size") or None,
                "box_px": wb.get("box_size") or None,
                "lowpass_resolution_ang": wb.get("template_resolution"),
            })
        sp.setdefault("workbench_ui", {
            k: wb.get(k) for k in
            ("auto_box", "apply_lowpass", "basic_shape_def", "auto_infer_seed")
            if k in wb
        })
        sp.pop("workbench", None)
        sp.pop("template_path", None)
        sp.pop("mask_path", None)

    # Lift particle_diameter_ang and symmetry from job models onto species
    for iid, jm in data.get("jobs", {}).items():
        sid = jm.get("species_id")
        if not sid or sid not in species_by_id:
            continue
        sp = species_by_id[sid]
        if jm.get("job_type") == "tmextractcand" and "particle_diameter_ang" in jm:
            sp.setdefault("diameter_ang", jm["particle_diameter_ang"])
            del jm["particle_diameter_ang"]
        if jm.get("job_type") == "templatematching" and "symmetry" in jm:
            sp.setdefault("symmetry", jm["symmetry"])
            del jm["symmetry"]
        # template_path / mask_path on TM job: drop in favor of species
        for k in ("template_path", "mask_path"):
            jm.pop(k, None)
```

Plus a thin compatibility shim in `path_resolution_service.py` to look up
`tm_jm.template_path` via `species.templates[tm_jm.template_id or 0].template_path`.

## Where this lands relative to the dashboard sanity panel

The pixel sanity panel currently does **the right thing under the wrong
assumptions** — it falls back to `_resolve_species` + MRC-header reads
because the underlying schema is messy. After this refactor:

- `_resolve_species` becomes a one-liner (the species_id is unambiguously
  on the job model, no fallbacks needed).
- `_read_template_apix_box` becomes a one-liner (the template metadata is
  in `species.templates[i]`).
- The "template px ≠ recon px" warning can name *which template* needs
  re-rendering (`species.templates[i].id`) and offer a "re-render at recon
  px" button as a Slice E action.
- The pixel-sanity table's "particle" column reads `species.diameter_ang`
  directly — no cross-instance join needed for the consistency check
  (since there's only one diameter per species).

## Scope estimate

- `services/project_state.py`: +80 lines (new models, migration, getters)
- `services/jobs/template_match.py`: -10 lines (drop fields), small touch-up
- `services/jobs/candidate_extract.py`: -3 lines, touch-up
- `services/path_resolution_service.py`: ~10 lines for the template-id resolver
- `ui/template_workbench.py`: rewire to read/write the new schema (~50 lines)
- `ui/tomo_dashboard_dialog.py`: simplify `_resolve_species`, drop MRC-header fallback (~20 lines deleted)
- `ui/job_plugins/*.py`: any plugin that reads `template_path` from the job model (grep)
- Migration test: load a v1 project → save → reload, verify equivalence

Estimated 1–2 day refactor. Do it BEFORE adding more sanity rules to the
panel (since several rules become much cleaner with the new schema).

## Open questions

1. **Multi-template per species.** Worth it? Or is "one species ↔ one
   template" enough? Current naming convention (`_white.mrc` / `_black.mrc`)
   suggests at minimum two: the matching template and a reference for
   visualization. Could also support: lowpass-filtered variants, masked
   variants. Recommendation: support multi-template but default to one.
2. **Template asset registry (project-wide).** Should templates be
   addressable by global ID across species (e.g., `templates/ribosome.mrc`
   shared by two species variants)? Probably overkill for now —
   keep templates owned by the species.
3. **Template auto-discovery.** When a user drops a `.mrc` into
   `templates/<species>/`, should we auto-register it as a
   `ParticleTemplate`? Could be a nice workbench-widget action.
4. **Re-render template at recon px.** Once schema lands, is there an
   automated way to re-render? PyTOM has a downsampling utility; could
   wrap it as a one-click Slice E action when the px-mismatch warning
   fires.
