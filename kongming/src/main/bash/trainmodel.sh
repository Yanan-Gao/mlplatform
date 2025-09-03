#!/bin/bash
set -o errexit

DOCKER_IMAGE_VERSION="release"
ENV="dev"
DATE_PARTITION=$(date -d "$date -1 days" +"%Y%m%d")

while getopts e:d:v:x: flag
do
  case "${flag}" in
    v) DOCKER_IMAGE_VERSION=${OPTARG};;
    e) ENV=${OPTARG};;
    d) DATE_PARTITION=${OPTARG};;
    x) EXPERIMENT_NAME="experiment=${OPTARG}/"; EXPERIMENT_NAME_FLAG="--experiment=${OPTARG}";;
  esac
done

DOCKER_IMAGE_NAME="ttd-base/audauto/kongming"
DOCKER_INTERNAL_BASE="docker.pkgs.adsrvr.org/apps-dev"
DOCKER_USER="svc.emr-docker-ro"
HOME_HADOOP="/mnt"
EXTENDED_FEATURES="contextual"

READENV=$ENV
if [ "$EXPERIMENT_NAME" != "" ]
then
  READENV="test"
elif [ $ENV == "prodTest" ]
then
  READENV="prod"
fi

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${READENV}/kongming"
TRAINING_DATA="${EXPERIMENT_NAME}trainset/csv/v=1"
DATE_PATH="date=${DATE_PARTITION}"
TRAINING_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA}/${DATE_PATH}/split=train/"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/${TRAINING_DATA}/${DATE_PATH}/split=val/"

INPUT_DEST="/mnt/input/"
TRAINING_DATA_DEST="${INPUT_DEST}train"
VALIDATION_DATA_DEST="${INPUT_DEST}val"

echo "deleting any old data"
rm -rf $INPUT_DEST

echo "starting training data transfer"

aws s3 cp ${TRAINING_DATA_SOURCE} ${TRAINING_DATA_DEST} --recursive --include "*.csv" --quiet
aws s3 cp ${VALIDATION_DATA_SOURCE} ${VALIDATION_DATA_DEST} --recursive --include "*.csv" --quiet

echo "pulling image and running model training"

SECRETJSON=$(aws secretsmanager get-secret-value --secret-id svc.emr-docker-ro --query SecretString --output text)
CREDS=$(echo $SECRETJSON | jq .emr_docker_ro)

sudo chmod 666 /var/run/docker.sock

eval docker login -u $DOCKER_USER -p $CREDS $DOCKER_INTERNAL_BASE

eval docker pull ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION}

sudo docker run --gpus all \
           -e TF_GPU_THREAD_MODE=gpu_private \
           -v ${INPUT_DEST}:/opt/application/input/ \
           --entrypoint python3 \
           ${DOCKER_INTERNAL_BASE}/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} \
           /opt/application/app/main.py \
      "--env=${ENV}" \
      "--run_train=true" \
      "--use_csv=true" \
      "--extended_features=${EXTENDED_FEATURES}" \
      "--model_creation_date=${DATE_PARTITION}" \
      "--batch_size=65536" \
      "--learning_rate=0.0028" \
      "--eval_batch_size=197934" \
      "--num_epochs=20" \
      "--model_choice=basic" \
      "--early_stopping_patience=3" \
      "--push_training_logs=true" \
      "--generate_adgroup_auc=true" \
      "--push_metrics=true" \
      $EXPERIMENT_NAME_FLAG

echo "finished model training, cleaning up data"

rm -r ${TRAINING_DATA_DEST}
rm -r ${VALIDATION_DATA_DEST}

echo "finished cleaning up data"
