#!/bin/bash
# shellcheck source=/dev/null

venv_path=~/venv/bin/activate

err_exit() {
    echo -e "$1"
    exit 1
}

source "$venv_path"
# cd ./pydbgen ||  err_exit "Directory .pydbgen not found"

uvicorn pydbgen.app:app --host 0.0.0.0 --reload
