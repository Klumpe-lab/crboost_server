---
name: reference-pytom-angle-convention
description: PyTOM candidates.star angles (rlnAngleRot/Tilt/Psi) turn the template as Rotation.from_euler("ZYZ", [rot, tilt, psi]).as_matrix().T — measured, not assumed
metadata:
  type: reference
---

To show a TM template in a pick's orientation, rotate the template volume by R = scipy
`Rotation.from_euler("ZYZ", [rot, tilt, psi], degrees=True).as_matrix().T` (intrinsic ZYZ, transposed), applied as
`rotated(r) = template(R^T r)`; in (z, y, x) index order the affine matrix is `P @ R1 @ P` with R1 the untransposed
matrix and P the axis reversal (`services/kitchen/loupe.py` `turned_template`).

Measured 2026-10-28 on AGG412_20260311_MyrGFP_Grid2 job027 (ribosome, 6xu8 template at 6.2 Å): 3D NCC with the
tomogram at the 12 best picks was 0.18 for this convention at every pick, 0.07–0.08 for ZYZ, zyz, zyz^T, ZXZ, ZXZ^T and
random turns. Script pattern: scratchpad `euler.py` (not kept). Related: [[project-sta-kitchen]].
