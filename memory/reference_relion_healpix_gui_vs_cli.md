---
name: relion-healpix-gui-vs-cli
description: RELION GUI angular sampling degrees are the OVERSAMPLED values — GUI 3.75° = --healpix_order 3 (with --oversampling 1); services/jobs/class3d.py's "4=3.75deg" description is the base sampling and misleads protocol authors
metadata:
  type: reference
---

RELION GUI list (30, 15, 7.5, 3.7, 1.8, 0.9 …°) maps to `--healpix_order i` for index i (0-based) *because* the GUI always adds `--oversampling 1`, which halves the step. So GUI 3.75° → `--healpix_order 3`, GUI 1.8° → `--healpix_order 4`. Base steps: order 3 = 7.5°, order 4 = 3.75° (`_rlnPsiStep` in sampling.star shows the base). Likewise GUI offset step s → `--offset_step 2s`.
`services/jobs/class3d.py` (`healpix_order` Field description "2=15deg, 3=7.5deg, 4=3.75deg") quotes base steps, so anyone translating a GUI/tutorial value picks one order too fine (happened in the copia protocol, see [[copia-tutorial-sta-chain]]).
