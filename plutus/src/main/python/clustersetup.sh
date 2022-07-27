#!/bin/bash

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod"
SINGLE_DAY_SOURCE="singledayprocessed"
DATA_SOURCE="${BASE_S3_PATH}/${SINGLE_DAY_SOURCE}"
SSV_LIST=(google rubicon)

MNT="../../../../../../mnt/"
SYNC_DEST="./csv_input/"

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

cd ${MNT}

echo "beginning data sycn..."

for SSV in ${SSV_LIST[*]}
  do
  ##not bash doesnt support variable exapnsion here, so hardcoded '7'
  for i in {1..7}; do

      YEAR=$(date -d "$date -$i days" +"%Y")
      MONTH=$(date -d "$date -$i days" +"%m")
      DAY=$(date -d "$date -$i days" +"%d")
      echo "syncing from ${DATA_SOURCE}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/ ${SYNC_DEST}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
      aws s3 sync "${DATA_SOURCE}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/" "${SYNC_DEST}/${SSV}/year=${YEAR}/month=${MONTH}/day=${DAY}/" --quiet

  done
done

echo "set up complete"

