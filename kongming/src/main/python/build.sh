#!/bin/bash

# Helper script for local building and pushing
# Prerequisite: run docker login before running this
# To run: "./build.sh imageVersion"

set -o errexit

DOCKER_IMAGE_VERSION="test"
if [ ! -z "$1" ]
then
  DOCKER_IMAGE_VERSION=$1
fi

KONGMING_TRAINING_IMAGE="kongming/training-gpu"
DOCKER_INTERNAL_REGISTRY="dev.docker.adsrvr.org"

eval docker build -f Dockerfile.prod -t ${KONGMING_TRAINING_IMAGE}:${DOCKER_IMAGE_VERSION} .
eval docker tag ${KONGMING_TRAINING_IMAGE}:${DOCKER_IMAGE_VERSION} ${DOCKER_INTERNAL_REGISTRY}/${KONGMING_TRAINING_IMAGE}:${DOCKER_IMAGE_VERSION}
eval docker push ${DOCKER_INTERNAL_REGISTRY}/${KONGMING_TRAINING_IMAGE}:${DOCKER_IMAGE_VERSION}

echo "done pushing"
