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

SCORING_LOOKBACK_IN_DAYS=14
if [ ! -z "$3" ]
then
  SCORING_LOOKBACK_IN_DAYS=$3
fi

NUM_DAYS_TO_SCORE=13
if [ ! -z "$4" ]
then
  NUM_DAYS_TO_SCORE=$4
fi

DOCKER_IMAGE_NAME="kongming/training-gpu"
DOCKER_IMAGE_VERSION="release"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="/mnt"

READENV=$ENV
if [ $ENV == "prodTest" ]
then
  READENV="prod"
fi

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/kongming"
TRAINING_DATA="trainset/tfrecord/v=1"
ADGROUPMAPPING_DATA="baseAssociateAdgroup/v=1"
SCORING_DATA="dailyofflinescore/v=1"
DATE_PATH="date=${DATE_PARTITION}"

TRAINING_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA}/${DATE_PATH}/split=train/"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA}/${DATE_PATH}/split=val/"
ADGROUPMAPPING_DATA_SOURCE="${BASE_S3_PATH}/${ADGROUPMAPPING_DATA}/${DATE_PATH}/"
SCORING_DATA_MODEL_S3_LOCATION="${BASE_S3_PATH}/${SCORING_DATA}/"

INPUT_DEST="/mnt/input/"
TRAINING_DATA_DEST="${INPUT_DEST}train"
VALIDATION_DATA_DEST="${INPUT_DEST}val"
MAPPING_DATA_DEST="${INPUT_DEST}adgroupmapping"
SCORING_DATA_DEST="${INPUT_DEST}scoreset"

echo "restart httpd service so ganglia works"
sudo service httpd reload

echo "deleting any old data"
rm -rf $INPUT_DEST

echo "starting training data transfer"

aws s3 cp ${TRAINING_DATA_SOURCE} ${TRAINING_DATA_DEST} --recursive --include "*.tfrecord.gz" --quiet
aws s3 cp ${VALIDATION_DATA_SOURCE} ${VALIDATION_DATA_DEST} --recursive --include "*.tfrecord.gz" --quiet

#copy scoring data
FIRST_SCORING_DATE=$(date -d "$DATE_PARTITION -$SCORING_LOOKBACK_IN_DAYS days")
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

echo "pulling image and running train and user score steps"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

sudo docker run --gpus all \
           -v ${INPUT_DEST}:/opt/application/input/ \
           ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} \
      "--env=${ENV}" \
      "--run_train=true" \
      "--model_creation_date=${DATE_PARTITION}" \
      "--batch_size=8192" \
      "--eval_batch_size=197934" \
      "--num_epochs=20" \
      "--model_choice=basic" \
      "--early_stopping_patience=3" \
      "--push_training_logs=true" \
      "--push_metrics=true" \
      "--run_score=true" \
      "--score_dates=${ALL_SCORING_PARTITIONS}"

echo "finished train and score, cleaning up data"

rm -r ${TRAINING_DATA_DEST}
rm -r ${VALIDATION_DATA_DEST}
rm -r ${MAPPING_DATA_DEST}
rm -r ${SCORING_DATA_DEST}

echo "finished cleaning up data"
