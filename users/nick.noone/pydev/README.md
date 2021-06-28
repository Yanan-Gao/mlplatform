* a word of warning- this project was created to prove to myself that i could run a container on emr, it is not pretty or well documented
* ecr repo ive been using nvn-emr-test

Here we have the basic set up create a docker container for a pyspark app.
Much inspiration was taken from this article:`https://becominghuman.ai/real-world-python-workloads-on-spark-emr-clusters-3c6bda1a1350`
With emr 6 aws started to provide support for using images mostly to manage dependencies but 
with the intention that would still manually push your python code up to the cluster or 
do something else less than ideal like copy your files to s3.  Here we sort of forego that and give our cluster access 
ecr by including: 

`{
  "classification": "container-executor",
  "properties": {},
  "configurations": [
    {
      "classification": "docker",
      "properties": {
        "docker.privileged-containers.registries": "local,centos,003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test",
        "docker.trusted.registries": "local,centos,003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test"
      },
      "configurations": []
    }
  ]
}`

in the cluster configuration. once the cluster has provisioned if you are just doing dev mode you can ssh in to the master node and 
run:

`sudo yum install -y docker && \
sudo systemctl start docker && \
sudo chmod 666 /var/run/docker.sock && \
DOCKER_IMAGE_NAME=003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test:latest && \
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 003576902480.dkr.ecr.us-east-1.amazonaws.com
`

followed by: 
`docker run -v /usr/bin/:/usr/bin/ -v /etc/hadoop/conf:/etc/hadoop/conf -v /etc/spark/conf:/etc/spark/conf -v /usr/lib/spark:/usr/lib/spark -v /usr/share/aws:/usr/share/aws -v /usr/lib/hadoop/etc/hadoop:/usr/lib/hadoop/etc/hadoop -e SPARK_HOME=/usr/lib/spark -e HADOOP_CONF_DIR=/etc/hadoop/conf -e PYTHONPATH=$SPARK_HOME/python -e PYSPARK_PYTHON=/usr/bin/python3 -e PYSPARK_DRIVER_PYTHON=/usr/bin/python3 -e HADOOP_HOME=/usr/lib/hadoop --network host $DOCKER_IMAGE_NAME
`

to productionize this method we would need to create a custom step with a little bash script. which i have not not done yet

The entry point to the container is a spark-submit command where i defined a package i needed that i was having issues pulling
in from elsewhere. The example in the medium article his CMD in the container just `python3 mypyfile.py`