#!/bin/bash

MODEL_INPUT="/var/tmp/csv_input/"
DOCKER_IMAGE_NAME="plutus-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="$HOME/../../mnt"

while getopts ":t:" opt; do
  case "$opt" in
    t)
      IMAGE_TAG="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting image tag to $OPTARG" >&1
      ;;
    *)
      echo "No image flag set. falling back to release" >&1
      IMAGE_TAG="release"
  esac
done

echo "this is my image tag $IMAGE_TAG" >&1

if [ "$IMAGE_TAG" != "release" ] && [ "$IMAGE_TAG" != "latest" ];then
    echo "invalid image tag provided falling back to release" >&1
    IMAGE_TAG="release"
fi

FULL_CONTAINER=${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo "$SECRETJSON" | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p "$CREDS" $DOCKER_INTERNAL_BASE

eval docker pull ${FULL_CONTAINER}

sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v /mnt/csv_input:${MODEL_INPUT} \
  ${FULL_CONTAINER}  \
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
