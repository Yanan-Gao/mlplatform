ARG FROM_IMAGE_NAME=nvcr.io/nvidia/tensorflow:21.02-tf2-py3
FROM ${FROM_IMAGE_NAME}

WORKDIR /opt/application/

RUN pip3 install --upgrade pip
RUN pip3 install --quiet --no-cache-dir \
    'tensorflow===2.4.0' \
    'tensorboard' \
    # 'petastorm' \
    # 'pyspark==3.0.1' \
    'tensorflow-probability===0.12.2' \
    'boto3'


COPY ./src/main/python/ ./

RUN ls && pwd

ENTRYPOINT ["python3", "./main.py"]
