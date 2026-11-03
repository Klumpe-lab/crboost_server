---
name: aretomo-long-ts-name-mangles-tlt
description: AreTomo 1.0 (warp dev36 sif) mangles its IMOD .tlt file name when the tilt-series basename is long (51 chars broke, 42 works); Warp ts_aretomo then "Could not find .tlt", marks the TS UnselectManual=True, runs on NOMINAL tilt angles; driver said success until 2026-09-09
metadata:
  type: reference
---

Seen 2026-09-09 on `copia-empiar12580-tutorial-20260909-1414` (TS name 51 chars): `_Imod/` held `20260909-1414_Position_1.st` (= the .tlt, 252 bytes) instead of `<ts>_st.tlt`; XML `<Angles>` stayed nominal (−32 vs −44.01 with AreTomo's 12° tilt-offset applied), `AxisAngle` unrefined, `UnselectManual="True"`; ts_ctf absorbed it as `PlaneNormal` ≈ 12° → tomogram preview shows the particle layer as a diagonal band; TM still picked ~1000. Everything from alignment on is invalid in such a project.

**Why:** our TS id = `<project dirname>_<mdoc stem>`; protocol projects are `<protocol>-<YYYYMMDD-HHMM>`, so protocol name length lands in the AreTomo file name. Exact AreTomo limit unknown (42 good, 51 bad).
**How to apply:** keep protocol/project names short (renamed `copia-empiar12580-tutorial` → `copia-tutorial`, 39-char TS name). Guards landed 2026-09-09 (uncommitted): `drivers/ts_alignment.py` `warp_marked_unselected()` fails the series on `UnselectManual="True"`; `services/protocols/apply.py` `long_tilt_series_name_warnings()` warns at creation above 42. Non-protocol project creation has no warning yet. Full entry: docs/known_bugs.md #3. Related: [[copia-tutorial-sta-chain]].
