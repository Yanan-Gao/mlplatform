#!/bin/bash
set -o errexit

ENV=${1:-dev}
DATE=${2:-`date -d "$date -1 days" +"%Y%m%d"`}

LOAD_MODEL=${3:false}
DOCKER_IMAGE_VERSION=${4:-release}

READENV=$([ $ENV == "prodTest" ] && echo prod || echo $ENV)

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/audience"
TRAINING_DATA_PREFIX="firstPartyPixel/dailyConversionSample/v=1"

TRAINING_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA_PREFIX}/date=${DATE}/split=train_tfrecord/"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA_PREFIX}/date=${DATE}/split=val_tfrecord/"

INPUT_DEST="/mnt/input/"

S3_SCRIPT_PATH="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts"

if [ "$LOAD_MODEL" == "true" ]
then
  MODEL_WEIGHT_FLAG="--model_weights_source s3://thetradedesk-mlplatform-us-east-1/models/${ENV}/audience/model/date=${DATE}"
fi

echo "restart httpd service so ganglia works"
sudo service httpd reload

DOCKER_USER="svc.emr-docker-ro"
DOCKER_IMAGE_NAME="audience/training-gpu"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_IMAGE=${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id $DOCKER_USER --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE
eval docker pull $DOCKER_IMAGE

aws s3 cp $S3_SCRIPT_PATH/run_model.sh .
sudo bash run_model.sh \
  --date $DATE \
  --env $ENV \
  --docker_image $DOCKER_IMAGE \
  --training_data_source $TRAINING_DATA_SOURCE \
  --validation_data_source $VALIDATION_DATA_SOURCE \
  $MODEL_WEIGHT_FLAG \
  --input_dest $INPUT_DEST \
  --gpus all \
  --extra_flags upload_weights