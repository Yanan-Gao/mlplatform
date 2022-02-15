FROM ubuntu:20.04

ENV SPARK_VERSION=3.2.1
ENV HADOOP_VERSION=3.2

# Install java dependencies 
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-8-jdk openjdk-8-jre
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV PATH $PATH:/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/bin:/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin


RUN set -ex && \
    sed -i 's/http:\/\/deb.\(.*\)/https:\/\/deb.\1/g' /etc/apt/sources.list && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt-get install -y bash tini libc6 libpam-modules krb5-user libnss3 procps wget && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* &&\
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN mkdir /opt/spark/python
# TODO: Investigate running both pip and pip3 via virtualenvs
RUN apt-get install -y python && \
    apt-get install -y python3 python3-pip && \
    # We remove ensurepip since it adds no functionality since pip is
    # installed on the image and it just takes up 1.6MB on the image
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools
    # You may install with python3 packages by using pip3.6
    # Removed the .cache to save space
    #rm -r /root/.cache && rm -rf /var/cache/apt/*

# export COURSIER_CACHE=/root/.cache/coursier/

RUN mv spark/jars /opt/spark/jars && \
    mv spark/bin /opt/spark/bin && \
    mv spark/sbin /opt/spark/sbin && \
    mv spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/ && \
    mv spark/kubernetes/dockerfiles/spark/decom.sh /opt/ && \
    mv spark/examples /opt/spark/examples && \
    mv spark/kubernetes/tests /opt/spark/tests && \
    mv spark/data /opt/spark/data && \
    mv spark/python/pyspark /opt/spark/python/pyspark && \
    mv spark/python/lib /opt/spark/python/lib

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
# ENV PYTHONHASHSEED 1

# Install Scala and sbt only when testing
ENV SCALA_VERSION 2.12.15
ENV SCALA_DEB https://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.deb

RUN \
    wget --quiet --output-document=scala.deb $SCALA_DEB && \
    dpkg -i scala.deb && \
    rm -f *.deb

WORKDIR /opt/spark/work-dir

# Install Scala Build Tool sbt
#ENV SBT_VERSION 1.5.5
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    wget -O- "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt=1.5.5 && \
    sbt sbtVersion

ENV SPARK_HOME /opt/spark
ENV PYSPARK_PYTHON /usr/bin/python3

# These could be moved out of the base image
RUN pip3 install -U pyspark==${SPARK_VERSION} scikit-learn pandas numpy pickle-mixin pyarrow

# packages required only for testing
#RUN pip3 install -U vaderSentiment

RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh

RUN rm -rf /opt/spark/work-dir/*
#COPY ./ /opt/spark/work-dir/

ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
# USER ${spark_uid}

