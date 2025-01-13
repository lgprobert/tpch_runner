#!/bin/bash
# shellcheck source=/dev/null

venv_path=~/venv/bin/activate

err_exit() {
    echo -e "$1"
    exit 1
}

source "$venv_path"

uvicorn tpch_runner.app:app --host 0.0.0.0 --reload
