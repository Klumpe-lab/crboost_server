#!/bin/bash

PYTOM_CONTAINER="/users/$USER/pytom.sif"
PROJECT_BIND="/scratch-cbe/users/$USER/cryoboost_projects:/project"
DATA_BIND="/users/artem.kushner/dev/001_CopiaTestSet/:/data:ro"

echo "--- pytom_extract_candidates.py Adapter Activated ---" >&2
echo "Job Directory: $(pwd)" >&2
echo "Executing inside pytom.sif: pytom_extract_candidates.py $*" >&2
echo "---------------------------------" >&2

apptainer run \
    --nv \
    -B "${PROJECT_BIND}" \
    -B "${DATA_BIND}" \
    --pwd "$(pwd)" \
    "${PYTOM_CONTAINER}" \
    pytom_extract_candidates.py "$@"
