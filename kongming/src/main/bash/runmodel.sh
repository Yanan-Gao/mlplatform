#!/bin/bash

ENV="prod"
if [ ! -z "$1" ]
  then
    ENV=$1
fi

DATE_PARTITION=$(date -d "$date -1 days" +"%Y%m%d")
if [ ! -z "$2" ]
  then
    DATE_PARTITION=$2
fi

MODEL_INPUT="/opt/application/"
DOCKER_IMAGE_NAME="kongming/training-gpu"
DOCKER_IMAGE_VERSION="release"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="/mnt"

INPUT_DEST="/mnt/input/"
META_DEST="/mnt/assets_string/"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

sudo docker run --gpus all \
           -v ${INPUT_DEST}:/opt/application/input/ \
           ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} \
      "--env=${ENV}" \
      "--model_creation_date=${DATE_PARTITION}" \
      "--batch_size=8192" \
      "--eval_batch_size=197934" \
      "--num_epochs=1" \
      "--model_choice=basic" \
      "--early_stopping_patience=5"

