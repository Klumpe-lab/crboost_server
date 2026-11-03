---
name: project_landing_config_and_hub_merge
description: "Landing status strip + per-user config override editor, and the merged project-hub panel (2026-07-24)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 758c059d-5fc7-4b18-a7aa-fafc38761084
---

Three-part UI/infra change landed 2026-07-24 (code-clean + config-backend runtime-verified; UI PENDING RUNTIME — server has no auto-reload, restart `python main.py` to see it).

**Containers centralized**: all tool `.sif`s + cistem binary now under `/groups/klumpe/software/containers/sifs/` (was scattered). All 10 paths in `config/conf.yaml` + curation sif verified to exist post-move.

**① Per-user config override** (`services/configs/config_service.py`): `ConfigService.__init__` now layers server default `config/conf.yaml` (base) + `~/.crboost/conf.yaml` (this user's override, reuses the `~/.crboost/` dir already used by [[feedback_background_task_no_client_context]]'s prefs). `_deep_merge`/`_deep_diff` helpers → `save_user_override(nv)` writes ONLY leaves differing from base (minimal delta), `revert_to_defaults()` deletes it, both call `reset_config_service()`. `has_user_override`, `effective_dict()`, `base_dict()`, `container_status()`, module-level `check_path_exists()`. Deployment model that makes this safe: ONE server process per user (each launches own `python main.py` from shared repo checkout) → editing shared conf.yaml would hit everyone; home override is per-user. `slurm_defaults`+`tools` are read LIVE at submit (override applies next run); `crboost_python` is BAKED into `config/qsub.sh` at preflight (override needs re-preflight/restart — surfaced as a note in the editor).

**② Landing status strip** (`ui/landing_status_strip.py`, mounted above data-import in `ui/main_ui.py` landing_page): "CryoBoost" wordmark + FingerprintedView status dots (SLURM reachability via `backend.slurm_service.get_partitions_info()`, current user, host:port via `CRBOOST_PORT` env set in `main.py.__main__`, containers N/N via `container_status()`, custom-config badge) + gear → `ui/config_settings_dialog.py`. Settings dialog = structured editor (Environment/Containers&tools/SLURM defaults/Project defaults), every path field has a live green/red existence dot updating on keystroke, Save→save_user_override, Revert→revert_to_defaults with confirm.

**③ Project-hub merge** (`ui/pipeline_builder/pipeline_roster.py` + `ui/projects_overview.py`): removed the separate sidebar metadata widget (`_build_metadata_btn` gone) + `_render_slurm_defaults_section`/`_profile_row_fields` (SLURM moved to config editor). Avatar click → `_open_project_hub` now a wide (96vw/88vh) 2-col panel: LEFT = `_render_project_params(state)` for the previewed project (loaded detached via new `backend.read_project_state_detached()` — no registry pollution), RIGHT = `ProjectsOverview` roster. New `ProjectsOverview` opt-in preview mode: `on_select` (row click → preview + faint outline via `set_selected`), `selected_path`, `height_css`, always-visible per-row travel arrow (`_travel`→on_open). Landing-page caller unchanged (no on_select → still click-to-travel). Fixed BASE-input width cutoff (inner flex row shrink-wrapped to ~80px content → added `width:100%`, same class of bug as ArtiaX split).

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

2026-07-24: containers centralized to /groups/klumpe/software/containers/sifs/; per-user ~/.crboost/conf.yaml override layer (config_service, runtime-verified) + landing status strip + structured config editor w/ live path dots; merged project hub (avatar→wide 2-col: left params-preview, right roster w/ click=preview+arrow=travel); BASE width bug fixed. venv/bin/python3 DOES run (py_compile + non-nicegui exec) — corrects "no python interpreter" in reference_crboost_data_access.
