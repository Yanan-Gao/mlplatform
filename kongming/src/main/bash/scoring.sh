#!/bin/bash
set -o errexit

DOCKER_IMAGE_VERSION="release"
ENV="dev"
MODEL_DATE=$(date -d "$date -1 days" +"%Y%m%d")
SCORING_LOOKBACK_IN_DAYS=14
NUM_DAYS_TO_SCORE=13

while getopts v:e:m:l:d: flag
do
  case "${flag}" in
    v) DOCKER_IMAGE_VERSION=${OPTARG};;
    e) ENV=${OPTARG};;
    m) MODEL_DATE=${OPTARG};;
    l) SCORING_LOOKBACK_IN_DAYS=${OPTARG};;
    d) NUM_DAYS_TO_SCORE=${OPTARG};;
  esac
done

DOCKER_IMAGE_NAME="kongming/training-gpu"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="/mnt"

READENV=$ENV
if [ $ENV == "prodTest" ]
then
  READENV="prod"
fi

BASE_MODEL_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/models/${READENV}/kongming/conversion_model"
BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/kongming"
SCORING_DATA="dailyofflinescore/v=1"
DATE_PATH="date=${MODEL_DATE}"

SCORING_DATA_MODEL_S3_LOCATION="${BASE_S3_PATH}/${SCORING_DATA}/"
ADGROUPMAPPING_DATA="baseAssociateAdgroup/v=1"
ADGROUPMAPPING_DATA_SOURCE="${BASE_S3_PATH}/${ADGROUPMAPPING_DATA}/${DATE_PATH}/"

INPUT_DEST="/mnt/input/"
MODEL_DEST="${INPUT_DEST}model"
SCORING_DATA_DEST="${INPUT_DEST}scoreset"
MAPPING_DATA_DEST="${INPUT_DEST}adgroupmapping"

echo "deleting any old data"
rm -rf $INPUT_DEST

echo "starting data transfer"

aws s3 cp ${BASE_MODEL_S3_PATH} ${MODEL_DEST} --recursive --exclude "*" --include "${DATE_PATH}/*" --quiet

#copy scoring data
FIRST_SCORING_DATE=$(date -d "$MODEL_DATE -$SCORING_LOOKBACK_IN_DAYS days")
ALL_SCORING_PARTITIONS=""
iter=0
until [ $iter -gt $(($NUM_DAYS_TO_SCORE-1)) ]
do
  if [ $iter -gt 0 ]
  then
    ALL_SCORING_PARTITIONS+=","
  fi

  SCORING_PARTITION=$(date -d "$FIRST_SCORING_DATE +$iter days" +"%Y%m%d")
  ALL_SCORING_PARTITIONS+="$SCORING_PARTITION"
  aws s3 cp ${SCORING_DATA_MODEL_S3_LOCATION} ${SCORING_DATA_DEST} --recursive --exclude "*" --include "date=${SCORING_PARTITION}*.tfrecord.gz" --quiet

  ((iter=iter+1))
done

aws s3 cp ${ADGROUPMAPPING_DATA_SOURCE} ${MAPPING_DATA_DEST} --recursive --include "*.csv" --quiet

echo "pulling image and running user scoring step"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

sudo docker run --gpus all \
           -v ${INPUT_DEST}:/opt/application/input/ \
           ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} \
      "--env=${ENV}" \
      "--run_score=true" \
      "--model_creation_date=${MODEL_DATE}" \
      "--score_dates=${ALL_SCORING_PARTITIONS}" \
      "--model_path=./input/model/"

echo "finished scoring, cleaning up data"

rm -rf $INPUT_DEST

echo "finished cleaning up data"
