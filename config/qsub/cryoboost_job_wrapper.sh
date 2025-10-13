#!/bin/bash
#
# This script programmatically finds the --o <out_dir> argument provided by Relion,
# changes the working directory to <out_dir>, and then executes the rest of 
# the command-line arguments. The difference this makes is that the results of jobs arrive 

set -e

# Find the output directory and store the command to be executed
OUT_DIR="."
CMD_ARGS=()
SKIP_NEXT=false

for arg in "$@"; do
  if [ "$SKIP_NEXT" = true ]; then
    SKIP_NEXT=false
    continue
  fi

  if [ "$arg" == "--o" ]; then
    # The next argument is the output directory path
    SKIP_NEXT=true
    OUT_DIR=$(eval echo "$2") # Use eval to handle potential ~ or variables
  else
    # This is part of the actual command to run
    CMD_ARGS+=("$arg")
  fi
  # Shift to the next argument for the next loop iteration
  shift
done

# Change to the job's output directory
echo "==> Wrapper: Changing directory to ${OUT_DIR}"
cd "${OUT_DIR}"

# Execute the actual command with its arguments
echo "==> Wrapper: Executing command: ${CMD_ARGS[@]}"
exec "${CMD_ARGS[@]}"