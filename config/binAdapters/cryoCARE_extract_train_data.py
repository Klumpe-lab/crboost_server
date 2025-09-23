#!/bin/bash

# --- Configuration ---
CRYOCARE_CONTAINER="/users/artem.kushner/cryocare.sif"  # Update this path
PROJECT_BIND="/scratch-cbe/users/$USER/cryoboost_projects:/project"
DATA_BIND="/users/artem.kushner/dev/001_CopiaTestSet/:/users/artem.kushner/dev/001_CopiaTestSet/:ro"

echo "--- cryoCARE_extract_train_data.py Adapter Activated ---" >&2
echo "Job Directory: $(pwd)" >&2
echo "Executing command inside cryocare.sif: cryoCARE_extract_train_data.py $*" >&2
echo "---------------------------------" >&2

# Execute the final cryoCARE command inside the container
apptainer run \
    --nv \
    -B "${PROJECT_BIND}" \
    -B "${DATA_BIND}" \
    --pwd "$(pwd)" \
    "${CRYOCARE_CONTAINER}" \
    cryoCARE_extract_train_data.py "$@"
