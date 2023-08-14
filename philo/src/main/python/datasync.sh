#!/bin/bash

# sync model input files for the given date range to the local machine from s3

BASE_DATA_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=3"
#BASE_MODEL_S3_PATH="s3://thetradedesk-mlplatform-us-east-1/models"
VALID_FORMATS=("csv" "gz") # only csv and gz are allowed currently
LOOKBACK=9

# parse -e flag for environment
# parse -d flag for input data start date
while getopts "e:d:p:m:r:l:f:" opt; do
  case "$opt" in
    e)
      ENV="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting environment to $OPTARG" >&1
      ;;

    d)
      START_DATE="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting start date to $OPTARG" >&1
      ;;

    p)
      PREFIX="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting data prefix to $OPTARG" >&1
      ;;

    m)
      META_PREFIX="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting metadata prefix to $OPTARG" >&1
      ;;

    r)
      REGION="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting region to $OPTARG" >&1
      ;;

    l)
      LATEST_MODEL_PATH="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting latest model path to $OPTARG" >&1
      ;;

    f)
      FORMAT="$(echo -e "${OPTARG}" | tr -d '[:space:]')"
      echo "Setting file format suffix to $OPTARG" >&1
      ;;

    *)
      echo "Invalid arg $OPTARG" >&1
  esac
done

if [ -z "$ENV" ]
then
   ENV="prod"
   echo "No enviroment flag set. Falling back to $ENV" >&1
fi

if [ -z "$START_DATE" ]
then
   START_DATE="$date"
   echo "No start date set. Falling back to $START_DATE" >&1
fi

if [ -z "$PREFIX" ]
then
   PREFIX="filtered"
   echo "No data prefix set. Falling back to $PREFIX" >&1
fi

if [ -z "$META_PREFIX" ]
then
   META_PREFIX="filteredmetadata"
   echo "No metadata prefix set. Falling back to $META_PREFIX" >&1
fi

if [ -z "$REGION" ]
then
   REGION=""
   echo "No region set. Falling back to region $REGION" >&1
fi

if [ -z "$LATEST_MODEL_PATH" ]
then
   LATEST_MODEL_PATH="${BASE_MODEL_S3_PATH}/${ENV}/philo/v=3/${REGION}/models/"
   echo "No latest model path set. Falling back to $LATEST_MODEL_PATH" >&1
fi

if [ -z "$FORMAT" ]
then
   FORMAT="csv"
   echo "No file format set. Falling back to $FORMAT" >&1
else
  case "$FORMAT" in
    "${VALID_FORMATS[@]}")
      echo "The FORMAT is valid: $FORMAT"
      ;;
    *)
      echo "Invalid FORMAT: $FORMAT. It should be one of the following: ${VALID_FORMATS[@]}"
      ;;
  esac
fi

MNT="../../../../../../mnt/"

# filtered data locations
DATA_SOURCE="${BASE_DATA_S3_PATH}/${ENV}/${PREFIX}"
SYNC_DEST="datasets/"

# meta locations
META_SOURCE="${BASE_DATA_S3_PATH}/${ENV}/${META_PREFIX}"
META_DEST="metadata/"

# latest trained model
MODEL_DEST="latest_model/"

cd ${MNT}

echo "starting s3 sync for params: prefix=${PREFIX}, meta_prefix=${META_PREFIX}, env=${ENV}, date=${START_DATE}, format=${FORMAT}\n"

##not bash doesnt support variable expansion here, so hardcoded '9'
 for i in {1..9}; do

    YEAR=$(date -d "$START_DATE -$i days" +"%Y")
    MONTH=$(date -d "$START_DATE -$i days" +"%m")
    DAY=$(date -d "$START_DATE -$i days" +"%d")
    S3_SOURCE="${DATA_SOURCE}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
    S3_META_SOURCE="${META_SOURCE}/year=${YEAR}/month=${MONTH}/day=${DAY}/"
    if ((i == 1))
      then echo "syncing from ${S3_SOURCE}  to "${SYNC_DEST}/test""
      aws s3 cp ${S3_SOURCE} "${SYNC_DEST}/test/" --recursive --exclude "*" --include "*.${FORMAT}" --quiet
    elif ((i == 2))
      then echo "syncing from ${S3_SOURCE}  to "${SYNC_DEST}/validation""
      aws s3 cp ${S3_SOURCE} "${SYNC_DEST}/validation/" --recursive --exclude "*" --include "*.${FORMAT}" --quiet
    else
      echo "copying from ${S3_SOURCE} to "${SYNC_DEST}/train""
      aws s3 cp ${S3_SOURCE} "${SYNC_DEST}/train/" --recursive --exclude "*" --include "*.${FORMAT}" --quiet
      echo "syncing meta data form ${S3_META_SOURCE} to ${META_DEST}"
      aws s3 cp ${S3_META_SOURCE} "${META_DEST}/" --recursive --exclude "*" --include "*.csv" --quiet
     fi

done

# sync latest trained model for incremental training
LATEST_S3_MODEL=`aws s3 ls ${LATEST_MODEL_PATH} | awk '{print $2}' | awk -F '/' '/\// {print $1}' | sort -r | head -n 1`

if [ -n "$LATEST_S3_MODEL" ]; then
  echo "Syncing latest model $LATEST_S3_MODEL"
  aws s3 sync ${LATEST_MODEL_PATH}${LATEST_S3_MODEL} ${MODEL_DEST} --quiet
else
  echo "Cannot find any models - nothing was synced"
fi

echo "data sync complete"