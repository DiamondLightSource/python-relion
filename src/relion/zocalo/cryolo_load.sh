#!/bin/bash

run_activate_scripts() {
    ACTIVATE_D="$1"
    if [[ -d "$ACTIVATE_D" ]]; then
        while IFS= read -r -d '' f; do
          # shellcheck disable=SC1090
          source "$f"
        done < <(find "${ACTIVATE_D}" -name "*.sh" -print0)
    fi
}

export CONDA_PREFIX="../../envs/cryolo_env"
export CONDA_DEFAULT_ENV="0.0"
export CONDA_ENV_PATH="$CONDA_PREFIX"
export CONDA_PATH_BACKUP="$PATH"
export CONDA_PS1_BACKUP="$PS1"

export PATH="$CONDA_PREFIX/bin:$PATH"
run_activate_scripts "$CONDA_PREFIX/etc/conda/activate.d" &&
exec "$@"
