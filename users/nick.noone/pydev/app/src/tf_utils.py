import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import tensorflow as tf
import tensorflow.keras as keras
import boto3

import json
from pathlib import Path
from datetime import datetime

import configparser

from app.src.tf_model import parse_sparse, get_sparse_input_model, open_fpa_nll


class DataSource:
    def __init__(self):
        """Create a spark context and session"""
        config = configparser.ConfigParser()
        config.read('./config/config.ini')
        master_url = config['spark']['master_url']
        app_name = config['spark']['app_name']
        data_locaesh = config['spark']['s3_data_locaesh']
        model_features = json.loads(config.get('spark', 'model_features'))
        model_targets = json.loads(config.get('spark', 'model_targets'))

        print(model_targets)
        print(f"Creating Spark context to {master_url}...")
        #
        #
        conf = SparkConf().setAppName(app_name)\
        #     .set("spark.jars", "file:///root/.ivy2/jars/com.linkedin.sparktfrecord_spark-tfrecord_2.12-0.3.0.jar")
        #
        #
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder \
             .config(conf=conf) \
             .getOrCreate()
        self.data_locaesh = data_locaesh
        self.model_features = model_features
        self.model_targets = model_targets
        print("Created Spark context...")

    def get_data(self):
        df_tfr = self.spark.read.format("tfrecord").option("recordType", "Example")\
             .load(
              self.data_locaesh).cache()

        #
        # print("\n=================\n" + str(df_tfr.count()) + "\n=================\n")
        #
        # df_tfr.write.format('tfrecord').option("recordType" , "Example").save("/tfrecords/")

        # p = Path(self.data_locaesh)
        # raw_ds = tf.data.TFRecordDataset([str(f.resolve()) for f in list(p.glob("*.gz"))], compression_type="GZIP")
        #
        # ds = raw_ds.prefetch(
        #     tf.data.AUTOTUNE
        # ).map(
        #     parse_sparse,
        #     num_parallel_calls=tf.data.AUTOTUNE,
        #     deterministic=False
        # ).batch(
        #     2 ** 13
        # )
        #
        # logs = "/local_disk0/logs/" + datetime.now().strftime("%Y%m%d-%H%M%S")
        #
        # tboard_callback = tf.keras.callbacks.TensorBoard(log_dir=logs,
        #                                                  histogram_freq=1,
        #                                                  profile_batch='500,520')
        #
        # model = get_sparse_input_model(units=128)
        # model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.01), loss=open_fpa_nll)
        # history = model.fit(ds, epochs=1, steps_per_epoch=10, verbose=1, callbacks=[tboard_callback])
        #
        # print(history)
        #
        # os.system("mkdir -p model")
        #
        # model.save("./model/")
        #
        # os.system("ls ./model/")
        #
        # S3 = boto3.resource('s3')
        # # def uploadDirectory(path):
        # #     for root, dirs, files in os.walk(path):
        # #         for file in files:
        # #             S3.Bucket('thetradedesk-mlplatform-us-east-1').upload_file(os.path.join(root, file), "users/nick.noone/pc/{file}".format(file=file))
        # #
        # # uploadDirectory('./model/')


       # return S3.Bucket('thetradedesk-mlplatform-us-east-1').upload_file("./model/saved_model.pb", "users/nick.noone/pc/saved_model.pb")
        df_tfr.show()
        return df_tfr