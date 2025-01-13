#!/bin/bash
# shellcheck disable=SC2034

DSS_PATH='data'
DSS_CONFIG='tool'
DSS_DIST='tool/dists.dss'
DSS_QUERY='templates'
action=dbgen

query_generator() {
    echo -e "Generating queries for scale factor $SCALE_FACTOR"
    for i in {1..22}; do
        ./qgen -d "$i" -s "$SCALE_FACTOR" > queries/"$i".sql
    done
}

data_generator() {
    echo -e "Generating queries for table $TABLE of scale factor $SCALE_FACTOR"
    for i in {1..22}; do
        ./qgen -d "$i" -s "$SCALE_FACTOR" > queries/"$i".sql
    done
}

if [[ -n "$1" && ("$1" == "dbgen" || "$1" == "qgen") ]]; then
    action="$1"
fi

while getopts "s:t:q:h" option
do
    case $option in
        s)
            SCALE_FACTOR=$option
            ;;
        t)
