---
name: reference_settings_dialog_drops_override_keys
description: The landing settings dialog rewrites ~/.crboost/conf.yaml from only its five sections — any hand-added key (e.g. species_catalog_root) is silently dropped on the next save
metadata:
  type: reference
---

`ui/config_settings_dialog.py` builds its working dict from five sections only (`crboost_python`, `local`,
`slurm_defaults`, `job_resource_profiles`, `tools`) and `ConfigService.save_user_override` writes the diff of
that dict against the server default. A top-level key added by hand to `~/.crboost/conf.yaml` that the dialog
doesn't edit — `species_catalog_root` is the live example (2026-10-05) — disappears the next time anyone saves
the dialog, and the feature it enabled turns off silently.

**How to apply:** put keys the dialog doesn't expose in `config/conf.yaml` (the shared server file), not in the
per-user override. Related: [[project_species_registry_arc]], [[project_landing_config_and_hub_merge]].
