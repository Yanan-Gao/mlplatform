#!/bin/bash

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/modelinput/google"
YEAR=$(date -d "$date -1 days" +"%Y")
MONTH=$(date -d "$date -1 days" +"%-m")
DAY=$(date -d "$date -1 days" +"%-d")
LOOKBACK="7"
SYNC_SOURCE="${BASE_S3_PATH}/year=${YEAR}/month=${MONTH}/day=${DAY}/lookback=${LOOKBACK}/"
HOME_HADOOP="../../../../../../home/hadoop/"
SYNC_DEST="tfrecords/"

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


echo "syncing from ${SYNC_SOURCE} to ${SYNC_DEST}"
cd ${HOME_HADOOP} && aws s3 sync ${SYNC_SOURCE} ${SYNC_DEST}

echo "set up complete"

