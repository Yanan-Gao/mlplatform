FROM amazoncorretto:8

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

# ADD https://repo1.maven.org/maven2/com/linkedin/sparktfrecord/spark-tfrecord_2.12/0.3.0/spark-tfrecord_2.12-0.3.0.jar /opt/application/spark/jars/

WORKDIR /opt/application/

RUN pip3 install --upgrade pip
RUN pip3 install --quiet --no-cache-dir \
    'tensorflow===2.4.0' \
    'tensorboard' \
    'petastorm' \
    'pyspark==3.0.1' \
    'tensorflow-probability' \
    'boto3'
    
COPY ./ ./
COPY app/main.py .
RUN zip -r project.zip app
# RUN find ./ -type f
# RUN pwd


ENTRYPOINT ["spark-submit", "--master", "yarn", "--py-files", "./project.zip", "--files", "./config/config.ini", "--packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.0", "./app/main.py"]

# CMD ["python3", "main.py"]