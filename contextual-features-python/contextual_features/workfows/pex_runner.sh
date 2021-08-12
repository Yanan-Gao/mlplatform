#!/bin/bash
# Wrapper to execute a PEX archive via an EMR Step
# Step type: Custom JAR
# JAR location: s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar
# Arguments:
#      s3://.../pex-executer.sh 
#      s3://.../env.pex 
#      s3://.../weights-dir
#      HADOOP_HOME=/usr/lib/hadoop 
#      SPARK_HOME=/usr/lib/spark 
#      ./env.pex -m pyspark.workfows.standard_categories_workflow
aws s3 cp $1 .
aws s3 sync $2 model_weights
chmod +x $(basename -- $1);
shift;
shift;
eval "$@"