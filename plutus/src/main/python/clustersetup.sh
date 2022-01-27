#!/bin/bash

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod"
MODEL_INPUT="modelinput/google"
META_DATA="metadata/google"
YEAR=$(date -d "$date -1 days" +"%Y")
MONTH=$(date -d "$date -1 days" +"%m")
DAY=$(date -d "$date -1 days" +"%d")
DATE_PATH="year=${YEAR}/month=${MONTH}/day=${DAY}"
LOOKBACK="7"
DATA_SOURCE="${BASE_S3_PATH}/${MODEL_INPUT}/${DATE_PATH}/lookback=${LOOKBACK}/format=tfrecord/"
META_SOURCE="${BASE_S3_PATH}/${META_DATA}/${DATE_PATH}/"

MNT="../../../../../../mnt/"
SYNC_DEST="tfrecords/"
META_DEST="metadata/"

echo "installing updates.... \n"
sudo yum update -y

echo "installing docker... \n"
sudo amazon-linux-extras install docker

echo "restarting docker... \n"
sudo systemctl --now enable docker

echo "getting stable distro... \n"
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
   && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | sudo tee /etc/yum.repos.d/nvidia-docker.repo

echo "installing nvidia-docker dos.. \n"
sudo yum install nvidia-docker2 -y

echo "cleaning cache... \n"
sudo yum clean expire-cache

echo "restarting docker! \n"
sudo systemctl restart docker

echo "nvidia docker tool set up complete \n starting s3 sync.. \n"


echo "syncing from ${DATA_SOURCE} to ${SYNC_DEST}"
cd ${MNT} && aws s3 sync ${DATA_SOURCE} ${SYNC_DEST} --quiet

echo "syncing meta data form ${META_SOURCE} to ${META_DEST}"
aws s3 sync ${META_SOURCE} ${META_DEST} --quiet

echo "set up complete"

