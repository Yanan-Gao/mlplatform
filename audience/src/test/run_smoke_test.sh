#!/bin/bash
set -o errexit

ENV=smoke
DATE=`date +%Y%m%d`
DOCKER_IMAGE_VERSION=${1:-release}
LOAD_MODEL=false

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${ENV}/audience"
ETL_BRANCH_NAME="master"

TRAINING_DATA_SOURCE="${BASE_S3_PATH}/${ETL_BRANCH_NAME}/split=train_tfrecord"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/${ETL_BRANCH_NAME}/split=val_tfrecord"

DOCKER_IMAGE_NAME="audience/training-gpu"
DOCKER_IMAGE=${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

INPUT_DEST="`pwd`/input"

bash `dirname $0`/../main/bash/run_model.sh \
  --env $ENV \
  --date $DATE \
  --docker_image $DOCKER_IMAGE \
  --training_data_source $TRAINING_DATA_SOURCE \
  --validation_data_source $VALIDATION_DATA_SOURCE \
  --input_dest $INPUT_DEST \
  --extra_flags num_epochs=1
