#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --drive-upload)
            DRIVE_SERVICE_ACCOUNT_CREDENTIALS_URL=$2
            DRIVE_UPLOAD_DIR=$3

            DRIVE_UPLOAD_ARG="--drive-upload $DRIVE_SERVICE_ACCOUNT_CREDENTIALS_URL $DRIVE_UPLOAD_DIR/csap_mes.csv $DRIVE_UPLOAD_DIR/csap_ind.csv $DRIVE_UPLOAD_DIR/csap_prod.csv"
            shift 3;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [ $# -ne 2 ]; then
    echo "Usage: ./3_generate_outputs.sh [--drive-upload <drive-service-account-credentials-url> <drive-upload-dir>] <user> <data-root>"
    echo "Generates the outputs needed downstream from raw data files generated by step 2 and uploads to Google Drive"
    exit
fi

USER=$1
DATA_ROOT=$2

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run.sh ${DRIVE_UPLOAD_ARG} \
    "$USER" "$DATA_ROOT/UUIDs/phone_uuids.json" \
    "$DATA_ROOT/Raw Data/csap_s01e01_activation.json" "$DATA_ROOT/Raw Data/csap_s01e02_activation.json" \
    "$DATA_ROOT/Raw Data/csap_s01e03_activation.json" "$DATA_ROOT/Raw Data/csap_s01e04_activation.json" \
    "$DATA_ROOT/Raw Data/csap_demog.json" "$DATA_ROOT/Raw Data/csap_evaluation.json" "$DATA_ROOT/Coded Coda Files/" \
    "$DATA_ROOT/Outputs/traced_data.json" \
    "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/csap_mes.csv" "$DATA_ROOT/Outputs/csap_ind.csv" \
    "$DATA_ROOT/Outputs/csap_prod.csv"
