#ARG FROM_IMAGE_NAME=nvcr.io/nvidia/tensorflow:21.02-tf2-py3
#FROM ${FROM_IMAGE_NAME}
FROM nvidia/cuda:11.0-cudnn8-devel-ubuntu18.04
CMD nvidia-smi

RUN apt-get update && apt-get install --no-install-recommends --no-install-suggests -y curl
RUN apt-get install unzip
RUN apt-get -y install python3.8
RUN apt-get -y install python3-pip

WORKDIR /opt/application/

RUN pip3 install --upgrade pip
RUN pip3 install --quiet --no-cache-dir \
    'tensorflow===2.4.0' \
    'tensorflow-probability===0.12.2' \
    'boto3' \
    'prometheus_client' \
    'awscli' \
    'pandas' \
    'tensorflow-serving-api'



COPY ./src/main/python/plutus ./plutus
COPY ./src/main/python/setup.cfg ./
COPY ./src/main/python/main.py ./


ENTRYPOINT ["python3", "./main.py"]
