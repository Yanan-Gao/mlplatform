#!/bin/bash
set -o errexit

ENV=${1:-dev}
DATE=${2:-`date -d "$date -1 days" +"%Y%m%d%H%M%S"`}
LOAD_MODEL=${3:false}
DOCKER_IMAGE_VERSION=${4:-release}
SOURCE_TYPE=${5:-Seed_None}
UPLOAD_MODEL=${6:false}
# the input format for the policy table is date YYYYMMDD format
POLICY_TABLE=${7:true}
MODEL_TYPE=${8:RSM}

READENV=$([ $ENV == "prodTest" ] && echo dev || echo $ENV)

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/audience/${MODEL_TYPE}/${SOURCE_TYPE}"
TRAINING_DATA_SOURCE="${BASE_S3_PATH}/v=1/${DATE}/split=Train/"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/v=1/${DATE}/split=Val/"

INPUT_DEST="/mnt/input/"

S3_SCRIPT_PATH="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts"

if [ "$LOAD_MODEL" == "true" ]
then
  PRETRAINED_MODEL_FLAG="--model_pretrained_path s3://thetradedesk-mlplatform-us-east-1/models/${ENV}/audience/${MODEL_TYPE}/${SOURCE_TYPE}/v=1/${DATE}/"
fi

if [ "$UPLOAD_MODEL" == "true" ]
then
  UPLOAD_MODEL_FLAG="--model_output_path s3://thetradedesk-mlplatform-us-east-1/models/${ENV}/${MODEL_TYPE}/v=1/${DATE}/"
  UPLOAD_EMBEDDING_FLAG="--embedding_output_path s3://thetradedesk-mlplatform-us-east-1/configdata/${ENV}/audience/embedding/${MODEL_TYPE}/v=1/${DATE}/"
fi

# check whether provide the information for policy table
if [ "$POLICY_TABLE" == "true" ]
then
  POLICY_TABLE_FLAG="--policy_table s3://thetradedesk-mlplatform-us-east-1/configdata/${ENV}/audience/policyTable/${MODEL_TYPE}/v=1/${DATE}/"
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

aws s3 cp $S3_SCRIPT_PATH/rsm_model.sh .
sudo bash rsm_model.sh \
  --date $DATE \
  --env $ENV \
  --docker_image $DOCKER_IMAGE \
  --training_data_source $TRAINING_DATA_SOURCE \
  --validation_data_source $VALIDATION_DATA_SOURCE \
  $PRETRAINED_MODEL_FLAG \
  $UPLOAD_MODEL_FLAG \
  $POLICY_TABLE_FLAG \
  $UPLOAD_EMBEDDING_FLAG \
  --input_dest $INPUT_DEST \
  --gpus all 