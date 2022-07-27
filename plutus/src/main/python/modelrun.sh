#!/bin/bash

MODEL_INPUT="/var/tmp/csv_input/"
DOCKER_IMAGE_NAME="plutus-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="../../../../../../mnt"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

# echo "this is what im trying to use to login: " ${DOCKER_USER} ${CREDS} ${DOCKER_INTERNAL_BASE}

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:release

sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v /mnt/csv_input:${MODEL_INPUT} \
  ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:release  \
      "--nodummy" \
      "--batch_size=65536" \
      "--eval_batch_size=197934" \
      "--num_epochs=10" \
      "--steps_per_epoch=3000" \
      "--exclude_features=ImpressionPlacementId,AdFormat,DealId" \
      "--training_verbosity=2" \
      "--model_arch=dlrm" \
      "--early_stopping_patience=3" \
      "--csv_input_path=/var/tmp/csv_input/" \
