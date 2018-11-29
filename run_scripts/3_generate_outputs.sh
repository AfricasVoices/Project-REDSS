#!/usr/bin/env bash

set -e

if [ $# -ne 2 ]; then
    echo "Usage: ./3_generate_outputs.sh <user> <data-root>"
    echo "Generates the outputs needed downstream from raw data files generated by step 2"
    exit
fi

USER=$1
DATA_ROOT=$2

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run.sh alexander "$DATA_ROOT/UUIDs/phone_uuids.json" \
    "$DATA_ROOT/Raw Data/csap_s01e01_activation.json" "$DATA_ROOT/Raw Data/csap_s01e02_activation.json" \
    "$DATA_ROOT/Raw Data/csap_s01e03_activation.json" "$DATA_ROOT/Raw Data/csap_s01e04_activation.json" \
    "$DATA_ROOT/Raw Data/csap_demog.json" "$DATA_ROOT/Raw Data/csap_evaluation.json" "$DATA_ROOT/Coded Coda Files/" \
    "$DATA_ROOT/Outputs/traced_data.json" "$DATA_ROOT/Outputs/Interface Files/" \
    "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/redss_analysis_messages.csv" "$DATA_ROOT/Outputs/redss_analysis_individuals.csv" \
    "$DATA_ROOT/Outputs/redss_production.csv"
