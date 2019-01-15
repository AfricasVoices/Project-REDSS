#!/bin/bash

set -e

IMAGE_NAME=redss-csap

# Check that the correct number of arguments were provided.
if [ $# -ne 19 ]; then
    echo "Usage: ./docker-run.sh <user> <phone-number-uuid-table-path>
    <s01e01-input-path> <s01e02-input-path> <s01e03-input-path> <s01e04-input-path>
    <demog-input-path> <evaluation-input-path> <prev-coded-dir> <json-output-path>
    <icr-output-dir> <coded-output-dir> <messages-output-csv> <individuals-output-csv> <production-output-csv>
    <drive-auth-file> <messages-drive-path> <individuals-drive-path> <production-drive-path>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_PHONE_UUID_TABLE=$2
INPUT_S01E01=$3
INPUT_S01E02=$4
INPUT_S01E03=$5
INPUT_S01E04=$6
INPUT_DEMOG=$7
INPUT_EVALUATION=$8
PREV_CODED_DIR=$9
OUTPUT_JSON=${10}
OUTPUT_ICR_DIR=${11}
OUTPUT_CODED_DIR=${12}
OUTPUT_MESSAGES_CSV=${13}
OUTPUT_INDIVIDUALS_CSV=${14}
OUTPUT_PRODUCTION_CSV=${15}
SERVICE_ACCOUNT_CREDENTIALS_URL=${16}
MESSAGES_DRIVE_PATH=${17}
INDIVIDUALS_DRIVE_PATH=${18}
PRODUCTION_DRIVE_PATH=${19}

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
# When run, the container will:
#  - Copy the service account credentials from the gcloud bucket url 'SERVICE_ACCOUNT_CREDENTIALS_URL'.
#  - Run the pipeline.
# The gcloud bucket access is authorised via volume mounting (-v in the docker container create command)
CMD="
    gsutil cp $SERVICE_ACCOUNT_CREDENTIALS_URL /root/.config/drive-service-account-credentials.json && \

    pipenv run python -u redss_pipeline.py $USER /data/phone-number-uuid-table-input.json \
    /data/s01e01-input.json /data/s01e02-input.json /data/s01e03-input.json /data/s01e04-input.json \
    /data/demog-input.json /data/evaluation-input.json /data/prev-coded \
    /data/output.json /data/output-icr /data/coded \
    /data/output-messages.csv /data/output-individuals.csv /data/output-production.csv \
    /root/.config/drive-service-account-credentials.json $MESSAGES_DRIVE_PATH $INDIVIDUALS_DRIVE_PATH $PRODUCTION_DRIVE_PATH
"
container="$(docker container create -v=$HOME/.config/gcloud:/root/.config/gcloud -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$INPUT_PHONE_UUID_TABLE" "$container:/data/phone-number-uuid-table-input.json"
docker cp "$INPUT_S01E01" "$container:/data/s01e01-input.json"
docker cp "$INPUT_S01E02" "$container:/data/s01e02-input.json"
docker cp "$INPUT_S01E03" "$container:/data/s01e03-input.json"
docker cp "$INPUT_S01E04" "$container:/data/s01e04-input.json"
docker cp "$INPUT_DEMOG" "$container:/data/demog-input.json"
docker cp "$INPUT_EVALUATION" "$container:/data/evaluation-input.json"
if [ -d "$PREV_CODED_DIR" ]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"

mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

mkdir -p "$OUTPUT_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_CODED_DIR"

mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"
