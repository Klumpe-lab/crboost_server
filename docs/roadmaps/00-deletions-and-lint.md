# Roadmap 00 — Deletions, lint floor, and three tiny bug fixes

**Status: COMPLETE — all 5 stages landed + committed 2026-08-11** (details in the architecture-assessment
memory trail; `ruff check` clean repo-wide, runtime checklist below remains the user's step).

## Before → After

**Before:** ~2,400 lines of verified-dead code ride along in the repo: a superseded metadata
translator, seven orphaned `filterTilts/` modules, two zero-byte service files, and a compat shim
that is missing one export (so its advertised import path raises `ImportError`). Ruff enforces only
`E,F` with no Python-version target, so none of the type/style drift the assessment documents is
machine-visible.

**After:** dead code gone; the lint floor knows the codebase is Python 3.11 and flags legacy
spellings and bug-prone patterns on newly touched code; three latent bugs fixed with one-to-three-line
diffs. **Gain:** every subsequent roadmap operates on a smaller, honest codebase; grep results stop
lying (e.g. searches for `MetadataTranslator` or star-parsing helpers currently surface dead hits);
nobody "fixes" dead code again.

## Verification evidence (gathered 2026-08-10, sandbox grep)

- `MetadataTranslator` (`services/configs/metadata_service.py:153-922`, runs to EOF): zero code
  references outside its own file — all other mentions are docstrings/comments in
  `tilt_series_service.py` and the four adapters describing what replaced it. Only live import from
  the module: `from services.configs.metadata_service import WarpXmlParser`
  (`services/tilt_series/adapters/ts_ctf.py:28`).
- Dead `filterTilts/` modules (`filterTiltsInt.py`, `filterPipeline_orchestrator.py`,
  `deepLearning/deepLearning_orchestrator.py`, `plotter.py`, `star_handler.py`,
  `warpProjectHandler.py`, `filterTiltsRule.py`): zero importers outside the dead set itself.
  Live set (imported by `drivers/tilt_filter.py:55-57`, `services/tilt_series_service.py:324`):
  `image_processor.py`, `deepLearning/model_loader.py`, `deepLearning/statistics_calculator.py`,
  plus `deepLearning/model_architectures.py` (imported by `model_loader.py:5`). Keep `__init__.py`s.
- `services/parameters_service.py`, `services/container_service.py` (both 0 bytes): zero importers.

## Stages

### Stage 1 — delete dead files *(zero behavior change)*
```
git rm filterTilts/filterTiltsInt.py filterTilts/filterPipeline_orchestrator.py \
       filterTilts/plotter.py filterTilts/star_handler.py filterTilts/warpProjectHandler.py \
       filterTilts/filterTiltsRule.py filterTilts/deepLearning/deepLearning_orchestrator.py \
       services/parameters_service.py services/container_service.py
```

### Stage 2 — excise `MetadataTranslator` *(zero behavior change)*
Truncate `services/configs/metadata_service.py` after `WarpXmlParser` (keep lines 1-152), then remove
top-of-file imports that become unused (ruff `F401` will list them). Note in the module docstring that
this file now contains only the Warp XML parser (its eventual home is `services/formats/warp_xml.py`,
Roadmap 01 stage 5 — don't move it here, one concern per commit).

### Stage 3 — tiny bug fixes (one commit each)
1. **Shim omission:** add `TiltFilterParams` to the import list and `__all__`-equivalent of
   `services/job_models.py` (today `from services.job_models import TiltFilterParams` raises
   `ImportError` while all sibling param classes import fine).
2. **"Failed to start: None":** unify the failure key of `deploy_and_run_scheme`
   (`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:69,290,339,373,394` return
   `"message"`; `:722,742` return `"error"`) to `"error"`, matching the consumer
   (`ui/pipeline_builder/pipeline_builder_panel.py:515-516`). Roadmap 03 will replace the dicts
   wholesale; this is the stop-the-bleeding fix.
3. **Double-write bug:** `services/configs/starfile_service.py:25-27` — the except branch repeats the
   identical failing `starfile.write` call. Make it re-raise (`raise`) after logging.

### Stage 4 — lint floor
In `ruff.toml`: add `target-version = "py311"`; extend select to `["E", "F", "UP", "B", "RUF"]`.
Run `ruff check --fix` (autofixes `UP` spellings mechanically), review the diff, then handle the
residue: expect a wave of `B008`-style findings and implicit-Optional flags
(`backend.py:1785`, `services/computing/slurm_service.py:108`, `drivers/driver_base.py:44,206,271`);
fix implicit-Optionals by hand (they are 1-line each), and `# noqa` with a comment anything genuinely
intentional. Do **not** enable `ANN` yet — that belongs to per-area roadmaps so annotations arrive
with understanding, not as a checkbox.

### Stage 5 — housekeeping (optional, cheap)
- Move in-package planning docs to `docs/` (292 KB: `services/visualization/*.md`,
  `services/scheduling_and_orchestration/*.md`, `ui/dashboard/REFACTOR_HANDOFF.md`); leave a one-line
  pointer if any code comments reference them.
- Decide `transient_scripts/` (590 tracked lines that announce their own disposability): delete or
  move under `docs/attic/`.
- Root-level `roadmap_10_08_2026.md` / `roadmap_24_07_2026.md` → `docs/`.

## Modern-Python weave-in

Stage 4 *is* the weave-in here: `UP` rewrites `Dict/List/Optional` → `dict/list/|` on touched code,
and `target-version` unlocks `StrEnum`/`Self`/`match` suggestions in later roadmaps. Nothing else in
this roadmap should introduce new constructs — deletions stay pure.

## Runtime checklist (after restart)

- App boots; landing page renders; open a project with a TiltFilter job (`projects/pos9_10`).
- Run one tilt-filter DL pass or open the tilt-filter panel (exercises the surviving
  `filterTilts.image_processor` / `deepLearning` imports).
- Open a job's config tab and the IO tab (exercises `job_models` shim imports incl. the new
  `TiltFilterParams`).
- Start a pipeline with no jobs selected → error toast must show "No jobs selected", not "None".
