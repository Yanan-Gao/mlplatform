#!/bin/bash

BASE_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1"
PREFIX="filtered"
META_PREFIX="filteredmetadata"
LOOKBACK=9
ENV="prod"
DATA_SOURCE="${BASE_S3_PATH}/${ENV}/${PREFIX}"
META_SOURCE="${BASE_S3_PATH}/${ENV}/${META_PREFIX}"

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

cd ${MNT}

##not bash doesnt support variable exapnsion here, so hardcoded '9'
for i in {1..9}; do

    YEAR=$(date -d "$date -$i days" +"%Y")
    MONTH=$(date -d "$date -$i days" +"%m")
    DAY=$(date -d "$date -$i days" +"%d")
    S3_SOURCE="${DATA_SOURCE}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
    S3_META_SOURCE="${META_SOURCE}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
    if ((i <= 7))
      then echo "syncing from ${S3_SOURCE} to "${SYNC_DEST}/train""
      aws s3 sync ${S3_SOURCE} "${SYNC_DEST}/train/" --exclude "*" --include "part-*0000?*.gz" --quiet
      echo "syncing meta data form ${S3_META_SOURCE} to ${META_DEST}"
      aws s3 sync ${S3_META_SOURCE} "${META_DEST}/" --exclude "*" --include "*.csv" --quiet
     fi

    if ((i == 8))
      then echo "syncing from ${S3_SOURCE}  to "${SYNC_DEST}/validation""
      aws s3 sync ${S3_SOURCE} "${SYNC_DEST}/test/" --exclude "*" --include "part-*0000?*.gz" --quiet
    fi

    if ((i == 9))
      then echo "syncing from ${S3_SOURCE}  to "${SYNC_DEST}/test""
      aws s3 sync ${S3_SOURCE} "${SYNC_DEST}/validation/" --exclude "*" --include "part-*0000?*.gz" --quiet
    fi

done

echo "set up complete"

