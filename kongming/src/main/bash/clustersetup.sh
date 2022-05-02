#!/bin/bash

echo "restart httpd service so ganglia works"
sudo service httpd reload

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

echo "nvidia docker tool set up complete \n starting s3 cp.. \n"


echo "starting training data transfer"
ENV="prod"
if [ ! -z "$1" ]
  then
    ENV=$1
fi

DATE_PARTITION=$(date -d "$date -1 days" +"%Y%m%d")
if [ ! -z "$2" ]
  then
    DATE_PARTITION=$2
fi

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/data/${ENV}"
MODEL_INPUT="kongming/trainset/v=1"
#META_DATA="metadata"
DATE_PATH="date=${DATE_PARTITION}"

TRAINING_DATA_SOURCE="${BASE_S3_PATH}/${MODEL_INPUT}/${DATE_PATH}/split=train_tfrecord/"
VALIDATION_DATA_SOURCE="${BASE_S3_PATH}/${MODEL_INPUT}/${DATE_PATH}/split=val_tfrecord/"

# todo: Chuanxi: we don't generate metadata and don't need it for now
#META_SOURCE="${BASE_S3_PATH}/${META_DATA}/${DATE_PATH}/"
#META_SOURCE="s3://thetradedesk-useast-hadoop/cxw/share/kongming/assets_string/"


TRAINING_DATA_DEST="/mnt/input/train"
VALIDATION_DATA_DEST="/mnt/input/val"
#META_DEST="file:/mnt/assets_string/"

aws s3 cp ${TRAINING_DATA_SOURCE} ${TRAINING_DATA_DEST} --recursive --include "*.tfrecord.gz"
aws s3 cp ${VALIDATION_DATA_SOURCE} ${VALIDATION_DATA_DEST} --recursive --include "*.tfrecord.gz"
#s3-dist-cp --src=${META_SOURCE} --dest=${META_DEST}

echo "set up complete"

