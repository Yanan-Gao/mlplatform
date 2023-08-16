#!/bin/bash

MODEL_INPUT="/var/tmp/input/"
DOCKER_IMAGE_NAME="philo-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="../../../../../../mnt"
HOME_DATASET="/mnt/datasets"
DATA_FOLDER="datasets" # is there a way to read from yaml file?
META_FOLDER="metadata"

while getopts "e:t:o:r:f:b:l:" opt; do
  case "$opt" in
    e)
      ENV="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting environment to $OPTARG" >&1
      ;;
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
    b)
      BATCH_SIZE="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting batch_size to $OPTARG" >&1
      ;;
    l)
      LEARNING_RATE="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting learning_rate to $OPTARG" >&1
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

if [ -z "$ENV" ]
then
   ENV="dev"
   echo "No environment. Falling back to $ENV" >&1
fi

if [ -z "$REGION" ]
then
   REGION_CMD=""
   echo "No region set. Not Support, will result in Python error later" >&1
else
   REGION_CMD="--region=$REGION"
fi
if [ -z "$OUTPUT_PATH" ]
then
   OUTPUT_PATH_CMD=""
   echo "No output path set. Using default value from config.yaml" >&1
else
   OUTPUT_PATH_CMD="--s3_output_path=$OUTPUT_PATH"
fi

if [ -z "$BATCH_SIZE" ]
then
   BATCH_SIZE_CMD=""
   echo "No batch size set. Using default value from config.yaml" >&1
else
   BATCH_SIZE_CMD="--batch_size=$BATCH_SIZE"
fi

if [ -z "$LEARNING_RATE" ]
then
   LEARNING_RATE_CMD=""
   echo "No learning rate set. Using default value from config.yaml" >&1
else
   LEARNING_RATE_CMD="--learning_rate=$LEARNING_RATE"
fi

REST_CMD="$REGION_CMD $OUTPUT_PATH_CMD $BATCH_SIZE_CMD $LEARNING_RATE_CMD"
echo "rest of the command are $REST_CMD"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

# echo "this is what im trying to use to login: " ${DOCKER_USER} ${CREDS} ${DOCKER_INTERNAL_BASE}

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

echo "this is what im trying to pull: " ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

echo "image pulled, running docker container"

sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v ${HOME_DATASET}:${MODEL_INPUT}/${DATA_FOLDER} \
  -v /mnt/metadata:${MODEL_INPUT}/${META_FOLDER}/ \
  -v /mnt/latest_model:${MODEL_INPUT}/latest_model \
  ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}  \
      "--nodummy" \
       "--env=${ENV}" \
       "${REGION_CMD}" \
       "${OUTPUT_PATH_CMD}" \
       "${BATCH_SIZE_CMD}" \
       "${LEARNING_RATE_CMD}"