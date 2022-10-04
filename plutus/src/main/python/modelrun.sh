#!/bin/bash

MODEL_INPUT="/var/tmp/csv_input/"
DOCKER_IMAGE_NAME="plutus-training"
DOCKER_INTERNAL_BASE="internal.docker.adsrvr.org"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="$HOME/../../mnt"

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod"
SINGLE_DAY_SOURCE="singledayprocessed"
DATA_SOURCE="${BASE_S3_PATH}/${SINGLE_DAY_SOURCE}"
SSV_LIST=(google rubicon)
SYNC_DEST="./csv_input/"
MIN_LOOKBACK_DAYS=2
VERSION=`date +%Y%m%d%H%M`

echo "evaluating flags........."

while getopts "t:v:" opt; do
  case "$opt" in
    t)
      IMAGE_TAG="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting image tag to $OPTARG" >&1
      ;;

    v) VERSION=${OPTARG};;
  esac
done

if [ -z "$IMAGE_TAG" ]
then
   echo "No image flag set. falling back to release" >&1
   IMAGE_TAG="release"
fi

echo "beginning data sync..."

cd "${HOME_HADOOP}" || exit

for SSV in ${SSV_LIST[*]}
  do
  ##not bash doesnt support variable expansion here, so hardcoded '8', '2'
  ##if changing this also look at min lookback
  for i in {2..8}; do

      YEAR=$(date -d "$date -$i days" +"%Y")
      MONTH=$(date -d "$date -$i days" +"%m")
      DAY=$(date -d "$date -$i days" +"%d")
      # echo "syncing from ${DATA_SOURCE}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/ ${SYNC_DEST}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
      aws s3 sync "${DATA_SOURCE}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/" "${SYNC_DEST}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/" # --quiet

      if [ $i == $MIN_LOOKBACK_DAYS ];then
        MODEL_TRAIN_DATE=$(date -d "$date -$i days" +"%Y%m%d%H")
        echo "setting model date for temporal validation split to: $MODEL_TRAIN_DATE"
      fi

  done
done

FULL_CONTAINER=${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${IMAGE_TAG}

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo "$SECRETJSON" | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p "$CREDS" $DOCKER_INTERNAL_BASE

eval docker pull ${FULL_CONTAINER}

echo "beginning training for training date: ${MODEL_TRAIN_DATE}"
sudo docker run --gpus all --shm-size=5g --ulimit memlock=-1 -v /mnt/csv_input:${MODEL_INPUT} \
  ${FULL_CONTAINER}  \
      "--nodummy" \
      "--batch_size=262144" \
      "--eval_batch_size=197934" \
      "--model_creation_date=${VERSION}" \
      "--num_epochs=10" \
      "--steps_per_epoch=3000" \
      "--exclude_features=ImpressionPlacementId,AdFormat,DealId" \
      "--training_verbosity=2" \
      "--model_arch=dlrm" \
      "--early_stopping_patience=3" \
      "--csv_input_path=/var/tmp/csv_input/" \
      "--model_training_date=${MODEL_TRAIN_DATE}" \
      "--push_training_logs=true" \
      "--learning_rate=0.00001"
