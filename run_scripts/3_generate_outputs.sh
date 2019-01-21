#!/usr/bin/env bash

set -e

if [ $# -ne 4 ]; then
    echo "Usage: ./3_generate_outputs.sh <user> <data-root> <drive-auth> <drive-upload-dir>"
    echo "Generates the outputs needed downstream from raw data files generated by step 2 and uploads to Google Drive"
    exit
fi

USER=$1
DATA_ROOT=$2
DRIVE_AUTH=$3
DRIVE_UPLOAD_DIR=$4

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run.sh "$USER" "$DATA_ROOT/UUIDs/phone_uuids.json" \
    "$DATA_ROOT/Raw Data/csap_s01e01_activation.json" "$DATA_ROOT/Raw Data/csap_s01e02_activation.json" \
    "$DATA_ROOT/Raw Data/csap_s01e03_activation.json" "$DATA_ROOT/Raw Data/csap_s01e04_activation.json" \
    "$DATA_ROOT/Raw Data/csap_demog.json" "$DATA_ROOT/Raw Data/csap_evaluation.json" "$DATA_ROOT/Coded Coda Files/" \
    "$DATA_ROOT/Outputs/traced_data.json" \
    "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/csap_msg.csv" "$DATA_ROOT/Outputs/csap_ind.csv" \
    "$DATA_ROOT/Outputs/csap_prod.csv" \
    "$DRIVE_AUTH" "$DRIVE_UPLOAD_DIR/csap_msg.csv" "$DRIVE_UPLOAD_DIR/csap_ind.csv" "$DRIVE_UPLOAD_DIR/csap_prod.csv"
