---
name: project-ownership-sharing
description: "Project sharing/transfer via mutable `owner` field (vs immutable `created_by`); no auth, no disk move; SHARED_OWNER=\"@lab\" sentinel"
metadata: 
  node_type: memory
  type: project
  originSessionId: b828f5f3-259e-4212-850c-38c13da2b136
---

Lab members fix/update each other's projects; "spawning it as my own user feels wrong." Shipped a sharing/transfer mechanism (landed 2026-06-23).

**Key architectural facts (some non-obvious):**
- There is **NO auth / access control** anywhere. Identity = `getpass.getuser()`, i.e. each lab member runs their **own** server process as their own OS user against a **shared filesystem** (common `DefaultProjectBase` everyone scans). So "sharing" is **attribution + roster grouping, NOT access enforcement** — everyone already can load any project. Honor-system; cross-process concurrency is the same unhandled race as any concurrent edit.
- Ownership is **metadata fully decoupled from the on-disk path** (path = `DefaultProjectBase / project_name`, not user home; nothing parses owner from path). So transfer changes one field and **never moves the directory** — moving would break absolute paths baked into `project_params.json`, driver `--project_path` args, container binds, and the path-keyed `_project_states` registry.

**The model (`services/project_state.py`):**
- `created_by: Optional[str]` — immutable provenance (who made it). `owner: Optional[str]` — mutable, drives sharing. `effective_owner = owner or created_by`. Sentinel `SHARED_OWNER = "@lab"` ⇒ shared/lab project; `is_shared` property checks it. `owner=None` ⇒ legacy/owned-by-creator (backward compat, no migration, no schema bump).
- Transfer: `backend.transfer_project_ownership(path, new_owner)` — resolves authoritative state via `state_for()` (load-if-absent, avoids clobber per [[feedback_inmemory_state_authoritative]]), sets `owner`, saves through StateService lock. Metadata only.

**UI:**
- Create-as-shared toggle in `ui/data_import_panel.py` (next to aggregation toggle) → `di.is_shared` → threaded as `shared=` through `create_project_and_scheme` → `initialize_new_project` (sets `state.owner = SHARED_OWNER if shared else None`).
- `ui/projects_overview.py`: roster groups by `effective_owner` (me → Lab/Shared → others alpha); "Only mine" shows mine **plus** the Lab/Shared section; per-row hover "Transfer…" button (`swap_horiz`) → dialog (pick a known user from scanned creators/owners, free-text allowed, or "Move to Lab / Shared") → `on_transfer` callback. Scan emits `owner` alongside `creator` in `_scan_for_projects_sync`.

**Remember:** server has NO auto-reload — user must restart `python main.py` (see [[hpc-env-setup-for-running-python-ruff-in-this-project]]). Full-app import verification needs the user's pandas env; I verified py_compile + ruff only.
