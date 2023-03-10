#!/bin/bash
set -o errexit

ENV="dev"
if [ ! -z "$1" ]
then
  ENV=$1
fi

DATE_PARTITION=$(date -d "$date -1 days" +"%Y%m%d")
if [ ! -z "$2" ]
then
  DATE_PARTITION=$2
fi

DOCKER_IMAGE_NAME="ttd-base/audauto/kongming"
DOCKER_IMAGE_VERSION="release"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="/mnt"

READENV=$ENV
if [ $ENV == "prodTest" ]
then
  READENV="prod"
fi

BASE_MODEL_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/models/${READENV}/kongming/conversion_model"
BASE_S3_DATA_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/kongming"
OFFLINE_ATTRIBUTION_DATA="impressionforisotonicregression/v=1"
OFFLINE_ATTRIBUTION_CVR_DATA="adgroupcvrforbiastuning/v=1"
CLIENT_TEST_ADGROUPS_DATA="clienttestadgroups/v=1"
DATE_PATH="date=${DATE_PARTITION}"

MODEL_SOURCE="${BASE_MODEL_S3_PATH}/${DATE_PATH}"
OFFLINE_ATTRIBUTION_DATA_SOURCE="${BASE_S3_DATA_PATH}/${OFFLINE_ATTRIBUTION_DATA}/${DATE_PATH}/"
OFFLINE_ATTRIBUTION_CVR_DATA_SOURCE="${BASE_S3_DATA_PATH}/${OFFLINE_ATTRIBUTION_CVR_DATA}/${DATE_PATH}"
CLIENT_TEST_ADGROUPS_DATA_SOURCE="${BASE_S3_DATA_PATH}/${CLIENT_TEST_ADGROUPS_DATA}/${DATE_PATH}"


INPUT_DEST="/mnt/input/"
MODEL_DEST="${INPUT_DEST}model"
OFFLINE_ATTRIBUTION_DATA_DEST="${INPUT_DEST}offlineattribution"
OFFLINE_ATTRIBUTION_CVR_DATA_DEST="${INPUT_DEST}offlineattribution_cvr"
CLIENT_TEST_ADGROUPS_DATA_DEST="${INPUT_DEST}client_test_adgroups"

echo "restart httpd service so ganglia works"
sudo service httpd reload

echo "deleting any old data"
rm -rf $INPUT_DEST

echo "starting training data transfer"

aws s3 cp ${BASE_MODEL_S3_PATH} ${MODEL_DEST} --recursive --exclude "*" --include "${DATE_PATH}/*" --quiet
aws s3 cp ${OFFLINE_ATTRIBUTION_DATA_SOURCE} ${OFFLINE_ATTRIBUTION_DATA_DEST} --recursive --exclude "*" --include "*.csv" --quiet
aws s3 cp ${OFFLINE_ATTRIBUTION_CVR_DATA_SOURCE} ${OFFLINE_ATTRIBUTION_CVR_DATA_DEST} --recursive --exclude "*" --include "*.csv" --quiet
aws s3 cp ${CLIENT_TEST_ADGROUPS_DATA_SOURCE} ${CLIENT_TEST_ADGROUPS_DATA_DEST} --recursive --exclude "*" --include "*.csv" --quiet

echo "pulling image and running train and user score steps"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

sudo docker run --gpus all \
           -v ${INPUT_DEST}:/opt/application/input/ \
           --entrypoint python3 \
           ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} \
           /opt/application/app/main.py \
      "--env=${ENV}" \
      "--run_calibration=true" \
      "--date=${DATE_PARTITION}" \
      "--cpu_cores=16"

echo "finished calibration, cleaning up data"

rm -rf $INPUT_DEST

echo "finished cleaning up data"
