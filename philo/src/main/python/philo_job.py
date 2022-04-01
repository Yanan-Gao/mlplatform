# Databricks notebook source
access_key = dbutils.secrets.get(scope = "jiaxing.pi", key = "access-key")
secret_key = dbutils.secrets.get(scope = "jiaxing.pi", key = "secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# COMMAND ----------

# MAGIC %md ### Need to set parameter in the following cell

# COMMAND ----------


# starting date of the testing period
cutoff = 20220207

test_data = True
save_to_s3 = True

# tensorflow model parameters
epochs = 20
batch_size = 1024
eval_batch_size = None
prefetch_num = 10


profile_batches=[100, 200]
early_stopping_patience = 5
#steps_per_epoch = 3000

# path to the tfrecords files
data_path = "/dbfs/FileStore/jiaxing/ctr_data/tfrecords"
philo_path = "/Workspace/Repos/jiaxing.pi@thetradedesk.com/ctr-model/philo/"

# path to the output and model file, if save_to_s3 is true, directly save to s3, otherwise save to model_path first, then copy to s3
model_path = "/dbfs/FileStore/jiaxing/ctr_model"

# aws bucket to save the model file
aws_bucket_name = "thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/stage/models/"

mount_name = "philo_models"

# COMMAND ----------

# MAGIC %md ### Mount s3 bucket to dbfs

# COMMAND ----------


try:
    dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
except:
    print("Already Mounted")

# COMMAND ----------

# aws_bucket_name = "thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/stage/models/"
# mount_name = "plutus"
# dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
# display(dbutils.fs.ls("/mnt/%s" % mount_name))
# dbutils.fs.ls('dbfs:/mnt/plutus/2021111221/')
# dbutils.fs.cp('dbfs:/FileStore/jiaxing/ctr_model/model/2022-01-17-22-24', 'dbfs:/mnt/philo/philo/v=1/stage/models/202201172224', recurse=True)

# COMMAND ----------

import numpy as np
import pandas as pd
import tensorflow as tf
import sys
if philo_path not in sys.path:
    sys.path.append(philo_path)
from philo.models import model_builder
from philo.features import DEFAULT_MODEL_FEATURES, DEFAULT_MODEL_TARGET
from philo.data import list_tfrecord_files, datasets, get_map_function, tfrecord_dataset
from philo.layers import custom_objects
from philo.utils import get_callbacks, generate_results
from datetime import datetime

from pathlib import Path
from pytz import timezone

import mlflow.tensorflow
mlflow.tensorflow.autolog()

# COMMAND ----------

input_path = f'{data_path}/cutoff_{cutoff}'
eastern = timezone('US/Eastern')
model_datetime = f"{datetime.now(eastern).strftime('%Y%m%d%H%M')}"
log_path = f'{model_path}/log/{model_datetime}/'
if save_to_s3:
    model_save_path = f"/mnt/{mount_name}/{model_datetime}/"
else:
    model_save_path = f"{model_path}/model/{model_datetime}/"
checkpoint_base_path = f"{model_path}/checkpoints/{model_datetime}/"



# COMMAND ----------

files = list_tfrecord_files(input_path)
data = datasets(files, batch_size, DEFAULT_MODEL_FEATURES, DEFAULT_MODEL_TARGET)

# COMMAND ----------

# train = tf.data.TFRecordDataset(files["train"]).batch(batch_size=batch_size).map(get_map_function(DEFAULT_MODEL_FEATURES, DEFAULT_MODEL_TARGET)).prefetch(prefetch_num)
# validate = tf.data.TFRecordDataset(files["validate"]).batch(batch_size=batch_size).map(get_map_function(DEFAULT_MODEL_FEATURES, DEFAULT_MODEL_TARGET)).prefetch(prefetch_num)
# test = tf.data.TFRecordDataset(files["test"]).batch(batch_size=batch_size).map(get_map_function(DEFAULT_MODEL_FEATURES, DEFAULT_MODEL_TARGET)).prefetch(prefetch_num)

# COMMAND ----------

model = model_builder(DEFAULT_MODEL_FEATURES)
model.compile("adam", "binary_crossentropy",
              metrics=['binary_crossentropy', tf.keras.metrics.AUC()], )
callbacks = get_callbacks(log_path, profile_batches, early_stopping_patience, checkpoint_base_path)

# COMMAND ----------

history = model.fit(data["train"],
                    epochs=epochs,
                    #steps_per_epoch=FLAGS.steps_per_epoch,
                    verbose=1,
                    callbacks=callbacks,
                    validation_data=data["validate"],
                    validation_steps=None,
                    validation_batch_size=1024
                   )

# COMMAND ----------

model.save(model_save_path)
#tf.keras.models.load_model(model_save_path, custom_objects=custom_objects)

# COMMAND ----------

# MAGIC %load_ext tensorboard
# MAGIC %tensorboard --logdir $log_path

# COMMAND ----------

if test_data:
    try:
        test = data['test']
        comparisons = generate_results(model, test)
        comparisons['BidRequestId'] = comparisons['BidRequestId'].str.decode("utf-8")
        comparisons.to_csv(f'{model_path}/results/epoch{epochsb}_{model_datetime}.csv', index=False)
    except KeyError:
        print(f"There is no test data")

# COMMAND ----------



# COMMAND ----------


