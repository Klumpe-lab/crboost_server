---
name: reference_int_job_param_corruption
description: widget bind_value + no validate_assignment writes raw values into job params — floats into int fields (job dropped on load) and bare strings into str-Enum fields (isinstance checks silently fail); fixed via numeric_forward + enum_forward
metadata:
  type: reference
---

**Symptom:** `Skipping job instance 'X' - failed to deserialize … int_from_float, input_value=0.05`
in the server log, then `FATAL: Instance 'X' not found in project_params.json. Available: [...]`
from the driver. The instance is *in* the JSON — it just failed validation and got dropped.

**Mechanism (three links, all needed):**
1. `ui.number` always hands back a JS float (and `None` when the box is cleared).
2. `AbstractJobParams` sets no `validate_assignment`, so pydantic v2 accepts
   `job_model.max_num_particles = 0.05` on an `int` field without a murmur; `model_dump()` writes
   `0.05` to project_params.json.
3. `ProjectState.load` validates strictly → `int_from_float` → the whole instance is skipped, taking
   the species binding, I/O overrides and job dir with it. Integral floats (`786.0`) round-trip
   fine, which is why this hid for so long — it only detonates on a fractional value.

**Fixed 2026-08-18** in `ui/job_plugins/_field_styles.py` (`_numeric_kind` + `numeric_forward`;
`numeric_field` passes `precision=0` for int fields and a coercing `forward=` to `bind_value`) and
`ui/pipeline_builder/slurm_tab.py` (`array_throttle` was the one numeric field bypassing the shared
helper). `ProjectState._build_job_params` now retries without the offending keys and reports each
repair through `load_warnings` instead of dropping the instance — see
[[feedback_never_silent_defaults]].

**Still open (deliberately):** `model_config = ConfigDict(validate_assignment=True)` on
`AbstractJobParams` would catch this at every write site, not just number widgets, but it can turn
latent bad assignments elsewhere into runtime exceptions — needs a live run before turning on.

**Debug recipe when a job "disappears":** grep the project JSON for a decimal point on a key that is
an `int` in `services/jobs/<type>.py`. Found in `denovo_picking_testdrive`: `max_num_particles: 0.05`
(fatal), plus `box_size: 786.0` / `crop_size: 448.0` on subtomoExtraction (benign, same cause).

**Same mechanism, enum flavour (found 2026-08-28):** `ui.select` writes the raw option string, so
picking "IsoNet" in the denoise-train tab left `denoise_method` holding the bare `str`, not
`DenoiseMethod.ISONET`. Every `==` still passes (str-Enum), so train ran IsoNet correctly — but
`DenoisePredictParams.inherited_from_train` gated on `isinstance(m, DenoiseMethod)`, which a bare
str fails, so denoise-predict read "no train method" and silently ran its cryoCARE default against
an IsoNet model. Fixed with `enum_forward()` in `_field_styles.py` (the enum sibling of
`numeric_forward`), plus coercion instead of isinstance in `inherited_from_train`.

**Rule:** never `isinstance(x, SomeEnum)` on a job param — coerce with `SomeEnum(x)` instead. A
str-Enum param that a widget last wrote is a `str`, and `==` will not tell you.

**Third flavour — in-range-but-meaningless (2026-08-28):** `max_num_particles: 0` on tmextractcand
reached PyTOM as `-n 0` → `error: -n must be larger than 0`, argparse exit 2, on a GPU node three
jobs deep. The value was *type*-valid, so neither `numeric_forward` nor load validation objected —
the field simply declared no `ge`. Fixed by `ge=1` on the field plus `_numeric_bounds()` in
`_field_styles.py`: `numeric_field` now passes the declared `ge`/`le` to `ui.number` as min/max
(Quasar clamps on blur), and `numeric_forward` REFUSES an out-of-range value the way it refuses
garbage — refusing, not clamping, is what keeps mid-typing from fighting the user.

**Rule:** a job param whose downstream tool has a domain (`> 0`, a range) must declare `ge`/`le`.
Only ge/le are used across services/jobs — gt/lt appear nowhere, and `_numeric_bounds` models neither.
