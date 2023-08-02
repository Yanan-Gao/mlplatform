# Databricks notebook source
from collections import Counter
import random

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import tensorflow as tf

import mlflow

import demo_nn as demo

# https://stackoverflow.com/questions/67017306/unable-to-save-keras-model-in-databricks
# need to mount s3 so it appears as local storage, tf expects local storage
# https://kb.databricks.com/dbfs/how-to-specify-dbfs-path /dbfs + ...
#mnt_source = "s3a://thetradedesk-useast-hadoop/Data_Science/bryan/"
mnt_source = "s3a://thetradedesk-mlplatform-us-east-1/"
#mnt_path = f"/mnt/bryan"
mnt_path = f"/mnt/pythia_demo"
try:
    dbutils.fs.mount(mnt_source, mnt_path, extra_configs = {
    "fs.s3a.canned.acl":"BucketOwnerFullControl",
    "fs.s3a.acl.default":"BucketOwnerFullControl",
    "fs.s3a.credentialsType":"AssumeRole",
    "fs.s3a.stsAssumeRole.arn":"arn:aws:iam::003576902480:role/ttd_cluster_compute_adhoc"
    })
except:
    print(f'{mnt_source} already mounted to {mnt_path}/')

# COMMAND ----------

dbutils.widgets.dropdown('target', 'Gender', ['Age', 'Gender', 'AgeGender'])
dbutils.widgets.text("date_to_process", "2023-07-19")
dbutils.widgets.text("numdays_train", "3")
dbutils.widgets.text("run_id", "0000")
dbutils.widgets.text('drop_power_users', '1000')
dbutils.widgets.text('nu_train', '1e6') # how many unique users to keep per country
dbutils.widgets.text('mr_train', '16667') # minimum rows per agegender label per hour_part

# dpu format: 1000.1, where anything after the decimal place is used to mark variants of models
dpu = dbutils.widgets.get('drop_power_users')
full_train = True if '0000' in dpu else False
target = dbutils.widgets.get("target")
# look back numdays + 2 from date_to_process
date_to_process = dbutils.widgets.get("date_to_process")
numdays_train = int(dbutils.widgets.get("numdays_train"))
run_id = dbutils.widgets.get('run_id')
run_id = str(random.randint(1000,9999)) if run_id == '0000' else run_id
nu = dbutils.widgets.get('nu_train')
mr = dbutils.widgets.get('mr_train')

# nn parameters
dbutils.widgets.text('feature_dim_factor', '1.3')
dbutils.widgets.text('num_decision_steps', '5')
dbutils.widgets.text('dense_layer_dim', '72')
dbutils.widgets.text('output_dim', '72')
dbutils.widgets.text('feature_dim', '72')
dbutils.widgets.text('patience', '8')
dbutils.widgets.text('epochs', '100')

feature_dim_factor = float(dbutils.widgets.get('feature_dim_factor'))
num_decision_steps = int(dbutils.widgets.get('num_decision_steps'))
dense_layer_dim = int(dbutils.widgets.get('dense_layer_dim'))
output_dim = int(dbutils.widgets.get('output_dim'))
feature_dim = int(dbutils.widgets.get('feature_dim'))
patience = int(dbutils.widgets.get('patience'))
epochs = int(dbutils.widgets.get('epochs'))

# COMMAND ----------

if full_train:
    param_path = 'unresampled'
else:
    param_path = f"dpu={dpu.split('.')[0]}/nu={nu}/mr={mr}"
#tfrecord_dates = dbutils.fs.ls(f'{mnt_path}/DATPERF-2811/tfrecords/{param_path}/country_hash/')
tfrecord_dates = dbutils.fs.ls(f'{mnt_path}/features/data/pythia/demo/v=1/dev/tfrecords/{param_path}/country_hash/')
parts = 0
countries = Counter()
date_country = dict()
for d in tfrecord_dates:
    df = spark.read.option('header', 'true').csv(d.path).toPandas()
    countries.update(df.Country.tolist())
    parts += 1
    date_country[d.name.split('=')[-1][:-1]] = dict(zip(df.Country,df.mapped))

print(f'Days processed: {parts}, Countries: {len(countries)}')
print(countries.most_common())

dbutils.widgets.dropdown('country_to_model', 'United States', ['All'] + sorted([c for c in countries if c]))
country_to_model = dbutils.widgets.get("country_to_model")

# COMMAND ----------

# DBTITLE 1,Parameters
if target == "Age":
    mlflow.set_experiment(experiment_id=2600402547498109)
elif target == "Gender":
    mlflow.set_experiment(experiment_id=2600402547498110)
elif target == "AgeGender":
    mlflow.set_experiment(experiment_id=2600402547498111)
mlflow.tensorflow.autolog()

mlflow.log_param("drop_power_users", dpu)
mlflow.log_param("num_users_per_country", nu)
mlflow.log_param("min_rows_per_label", mr)
mlflow.log_param("country", country_to_model)
mlflow.log_param("target", target)
mlflow.log_param("feature_dim_factor", feature_dim_factor)
mlflow.log_param("num_decision_steps", num_decision_steps)
mlflow.log_param("dense_layer_dim", dense_layer_dim)
mlflow.log_param("output_dim", output_dim)
mlflow.log_param("feature_dim", feature_dim)
mlflow.log_param("patience", patience)
mlflow.log_param("epochs", epochs)

# https://docs.databricks.com/files/index.html 
# local code, local driver node -> dbfs root (/dbfs) -> mnt, for tensorflow or local python

#tfrecord_dates = dbutils.fs.ls(f'{mnt_path}/DATPERF-2811/tfrecords/{param_path}/agegender/')
tfrecord_dates = dbutils.fs.ls(f'{mnt_path}/features/data/pythia/demo/v=1/dev/tfrecords/{param_path}/agegender/')
# train + validation
assert(len(tfrecord_dates) >= numdays_train+2)
paths = []
for d in tfrecord_dates:
    date = d.name.split('=')[-1][:-1]
    path = '/dbfs' + d.path.split(':')[1]
    if country_to_model != 'All':
        paths.append(path + f'Country={date_country[date][country_to_model]}/')
    else:
        paths += [path + f'Country={c}/' for c in list(date_country[date].values())]

# backwards from selected date
test_dir = [paths.pop()] # test is not used in this notebook
val_dir = [paths.pop()]
train_dir = [paths.pop() for i in range(numdays_train)]

if full_train:
    train_path = f'dpu={dpu}'
else:
    train_path = f'dpu={dpu}/nu={nu}/mr={mr}'
#model_filepath = f"/dbfs{mnt_path}/DATPERF-2881/models/target={target}/{train_path}/Country={country_to_model}/Date={date_to_process}/"
model_filepath = f"/dbfs{mnt_path}/models/dev/pythia/demoModel/models/target={target}/{train_path}/Country={country_to_model}/Date={date_to_process}/"

#checkpoint_base = f"/dbfs{mnt_path}/DATPERF-2881/weights/{run_id}_checkpoint/"
checkpoint_base = f"/dbfs{mnt_path}/models/dev/pythia/demoModel/weights/{run_id}_checkpoint/"
checkpoint_filepath = checkpoint_base + "weights.{epoch:02d}-{val_loss:.2f}"

batch_size = demo.batch_size

# COMMAND ----------

print(val_dir)
print(train_dir)
print(model_filepath)

# COMMAND ----------

weights = None
if full_train:
    df = spark.read.format("tfrecords").load(','.join([t[5:] for t in train_dir]))
    # https://danvatterott.com/blog/2019/11/18/balancing-model-weights-in-pyspark/
    y_collect = df.groupBy(target).count().collect()
    unique_y = [x[target] for x in y_collect]
    total_y = sum([x["count"] for x in y_collect])
    unique_y_count = len(y_collect)
    bin_count = [x["count"] for x in y_collect]
    weights = {i: ii for i, ii in zip(unique_y, total_y / (unique_y_count * np.array(bin_count)))}

# COMMAND ----------

history, model = demo.main(train_dir, val_dir, checkpoint_filepath, model_filepath, 
         target,
         feature_dim_factor=feature_dim_factor,
         num_decision_steps=num_decision_steps,
         dense_layer_dim=dense_layer_dim,
         output_dim=output_dim,
         feature_dim=feature_dim,
         weights=weights,
         patience=patience,
         epochs=epochs)

# COMMAND ----------

plt.plot(history.history['loss'], 'o-')
plt.plot(history.history['val_loss'], 'x-')
plt.legend(['train', 'val'])
plt.grid(1)
plt.title('loss')
plt.show()

# COMMAND ----------

if target == 'Gender':
    plt.plot(history.history['auc'], 'o-')
    plt.plot(history.history['val_auc'], 'x-')
    plt.legend(['train', 'val'])
    plt.grid(1)
    plt.title('AUC')
    plt.show()

# COMMAND ----------

model.summary()

# COMMAND ----------

# debug code to print out a batch of records from the parser
# confirmed: you get the same records when using one machine
'''
val_files = demo.get_tfrecord_files(val_dir)

tf_parse = demo.feature_parser('Age')
val1 = demo.tfrecord_dataset(val_files, batch_size * 10, tf_parse.parser(), train=False, deterministic=True).as_numpy_iterator()

tf_parse = demo.feature_parser('Gender')
val2 = demo.tfrecord_dataset(val_files, batch_size * 10, tf_parse.parser(), train=False, deterministic=True).as_numpy_iterator()

tf_parse = demo.feature_parser('AgeGender')
val3 = demo.tfrecord_dataset(val_files, batch_size * 10, tf_parse.parser(), train=False, deterministic=True).as_numpy_iterator()

for el1, el2, el3 in zip(val1,val2,val3):
    if not all((el1[1] + el2[1]*10) == el3[1]):
        print('Did not match!')
'''
