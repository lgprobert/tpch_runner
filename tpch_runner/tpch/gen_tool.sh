#!/bin/bash
# shellcheck disable=SC2034

DSS_PATH='data'
DSS_CONFIG='tool'
DSS_DIST='dists.dss'
DSS_QUERY='templates'
action=dbgen

all_tables=("region" "nation" "customer" "part" "supplier" "partsupp" "orders" "lineitem")

usage() {
    echo "Usage: $0 [-s scale_factor] [-t table] [-q query_number] [dbgen|qgen]"
    echo "  -s scale_factor: Scale factor for data generation (default: 1)"
}

err_exit() {
    echo "Error: $1" >&2
    exit 1
}

query_generator() {
    echo -e "Generating queries for scale factor $SCALE_FACTOR"
    if (( QUERY == -1 )); then
        for i in {1..22}; do
            tool/qgen -d -s "$SCALE_FACTOR" "$i" > queries/q"$i".sql
        done
    else
        tool/qgen -d -s "$SCALE_FACTOR" "$QUERY" > tmp.sql
    fi
}

data_generator() {
    echo -e "Generating data for table $TABLE of scale factor $SCALE_FACTOR"
    pushd tool
    if [ "$TABLE" = "all" ]; then
        tool/dbgen -f -s "$SCALE_FACTOR"
    else
        tool/dbgen -f -s "$SCALE_FACTOR" -T "$TABLE"
    fi
    popd
}

if [[ -n "$1" && ("$1" == "dbgen" || "$1" == "qgen") ]]; then
    action="$1"
    shift
else
    err_exit "Error: The first argument must be 'dbgen' or 'qgen'."
fi

while getopts "s:t:q:h" option
do
    case $option in
        s)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]]; then
                SCALE_FACTOR=$option
            else
                err_exit "-s requires a number"
            fi
            ;;
        t)
            if printf "%s\n" "${all_tables[@]}" | grep "$OPTARG" > /dev/null; then
                TABLE=$OPTARG
            else
                err_exit "Invalid table: $OPTARG"
            fi
            ;;
        q)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]]; then
                if (( OPTARG >= 1 && OPTARG <= 22 )); then
                    QUERY=$OPTARG
                else
                    err_exit "Invalid query number: $OPTARG"
                fi
            else
                err_exit "-q requires a number between 1 and 22"
            fi
            ;;
        h)
            usage
            ;;
        *)
            usage
            echo
            err_exit "Invalid option: -$OPTARG" >
            ;;
    esac
done

if [[ -z "$SCALE_FACTOR" ]]; then
    SCALE_FACTOR=1
fi

if [[ -z "$TABLE" ]]; then
    TABLE="all"
fi

if [[ -z "$QUERY" ]]; then
    QUERY=-1
fi

if [ "$action" == "dbgen" ]; then
    data_generator
elif [ "$action" == "qgen" ]; then
    query_generator
fi
