#!/bin/bash

MODEL_INPUT="/var/tmp/input/"
DOCKER_IMAGE_NAME="philo-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
DOCKER_TAG="release"
HOME_HADOOP="../../../../../../mnt"
S3_OUTPUT_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/dev/"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

# echo "this is what im trying to use to login: " ${DOCKER_USER} ${CREDS} ${DOCKER_INTERNAL_BASE}

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_TAG}

sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v /mnt/tfrecords:${MODEL_INPUT}/tfrecords \
  -v /mnt/metadata:${MODEL_INPUT}/metadata/ \
  ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_TAG}  \
      "--nodummy" \
      "--batch_size=512" \
      "--eval_batch_size=131072" \
      "--num_epochs=20" \
      "--input_path=/var/tmp/input/tfrecords/" \
      "--s3_output_path=${S3_OUTPUT_PATH}" \
      "--training_verbosity=2" \
      "--model_arch=deepfm" \
      "--early_stopping_patience=5"