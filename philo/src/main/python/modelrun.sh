#!/bin/bash

MODEL_INPUT="/var/tmp/input/"
DOCKER_IMAGE_NAME="philo-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="../../../../../../mnt"
# output_path move to main to make it consistent
# S3_OUTPUT_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/prod/"

while getopts "t:o:r:" opt; do
  case "$opt" in
    t)
      IMAGE_TAG="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting image tag to $OPTARG" >&1
      ;;
    o)
      OUTPUT_PATH="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting output path to $OPTARG" >&1
      ;;
    r)
      REGION="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting region to $OPTARG" >&1
      ;;
    *)
      echo "Invalid arg $OPTARG" >&1
  esac
done

if [ -z "$IMAGE_TAG" ]
then
   IMAGE_TAG="release"
   echo "No image flag set. Falling back to $IMAGE_TAG" >&1
fi

if [ -z "$OUTPUT_PATH" ]
then
   OUTPUT_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/prod/"
   echo "No output path set. Falling back to $OUTPUT_PATH" >&1
fi

if [ -z "$REGION" ]
then
   REGION=""
   echo "No region set. Falling back to empty region $REGION" >&1
fi

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

# echo "this is what im trying to use to login: " ${DOCKER_USER} ${CREDS} ${DOCKER_INTERNAL_BASE}

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

echo "this is what im trying to pull: " ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

echo "image pulled, running docker container"

sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v /mnt/tfrecords:${MODEL_INPUT}/tfrecords \
  -v /mnt/metadata:${MODEL_INPUT}/metadata/ \
  -v /mnt/latest_model:${MODEL_INPUT}/latest_model \
  ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}  \
      "--nodummy" \
      "--batch_size=16384" \
      "--learning_rate=0.008" \
      "--eval_batch_size=131072" \
      "--data_trunks=3" \
      "--exclude_features=DoNotTrack"\
      "--num_epochs=3" \
      "--input_path=/var/tmp/input/tfrecords/" \
      "--meta_data_path=/var/tmp/input/metadata/" \
      "--latest_model_path=/var/tmp/input/latest_model/" \
      "--s3_output_path=${OUTPUT_PATH}" \
      "--push_training_logs=true" \
      "--region=${REGION}" \
      "--training_verbosity=2" \
      "--model_arch=deepfm" \
      "--early_stopping_patience=5"
