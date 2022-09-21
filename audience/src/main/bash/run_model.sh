#!/bin/bash
set -o errexit

eval set -- "$(getopt --long date:,env:,docker_image:,training_data_source:,validation_data_source:,input_dest:,model_weights_source:,gpus:,extra_flags: -- $0 $@)"
while true; do
  case "$1" in
  --model_weights_source) MODEL_WEIGHT_SOURCE=$2 ;;
  --gpus) GPUS_FLAG="--gpus $2" ;;
  --date) DATE=$2 ;;
  --env) ENV=$2 ;;
  --docker_image) DOCKER_IMAGE=$2 ;;
  --training_data_source) TRAINING_DATA_SOURCE=$2 ;;
  --validation_data_source) VALIDATION_DATA_SOURCE=$2 ;;
  --input_dest) INPUT_DEST=$2 ;;
  --extra_flags)
    for extra_flag in $(echo $2 | tr ";" "\n"); do
      EXTRA_FLAGS="$EXTRA_FLAGS --$extra_flag"
    done
    ;;
  --) shift; break ;;
  esac
  shift 2
done

MODEL_CONFIG_SOURCE="s3://thetradedesk-mlplatform-us-east-1/libs/audience/resources/"

echo "deleting any old data"
rm -rf $INPUT_DEST

echo "copying training data: $TRAINING_DATA_SOURCE"
aws s3 cp ${TRAINING_DATA_SOURCE} $INPUT_DEST/train --recursive --include "*.tfrecord.gz" --quiet
echo "copying validation data: $VALIDATION_DATA_SOURCE"
aws s3 cp ${VALIDATION_DATA_SOURCE} $INPUT_DEST/validation --recursive --include "*.tfrecord.gz" --quiet

echo "copying model config: $MODEL_CONFIG_SOURCE"
aws s3 cp ${MODEL_CONFIG_SOURCE} $INPUT_DEST/resources --recursive --quiet

if [ "$MODEL_WEIGHT_SOURCE" != "" ]
then
  echo "copying model weights: $MODEL_WEIGHT_SOURCE"
  MODEL_WEIGHT_DEST=$INPUT_DEST/model_checkpoint
  MODEL_WEIGHT_FLAG="--model_weight_path=$MODEL_WEIGHT_DEST"
  aws s3 cp ${MODEL_WEIGHT_SOURCE} $MODEL_WEIGHT_DEST --recursive --quiet
fi

echo "pulling image and running train steps"
docker run \
      $GPUS_FLAG \
      -v ${INPUT_DEST}:/opt/application/input/ \
      $DOCKER_IMAGE \
      "--env=${ENV}" \
      "--run_train=true" \
      "--model_creation_date=${DATE}" \
      $MODEL_WEIGHT_FLAG \
      "--flagfile=/opt/application/input/resources/model_params.cfg" \
      $EXTRA_FLAGS

echo "finished train, cleaning up data"

rm -r $INPUT_DEST/train

echo "finished cleaning up data"
