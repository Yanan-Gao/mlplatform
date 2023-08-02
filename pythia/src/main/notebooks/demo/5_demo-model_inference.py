# Databricks notebook source
from collections import Counter
import random

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions import lit, col

import tensorflow as tf
gpus = len(tf.config.list_physical_devices('GPU'))
from tqdm import tqdm

#from sklearn.metrics import roc_curve, auc, precision_recall_curve
#from sklearn.metrics import classification_report, ConfusionMatrixDisplay, confusion_matrix
#from sklearn.metrics import f1_score, precision_score, recall_score

import demo_nn as demo
batch_size = demo.batch_size*10

from delta.tables import DeltaTable
spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled',False)
# https://docs.databricks.com/delta/tune-file-size.html
spark.conf.set('spark.databricks.delta.optimizeWrite.enabled',True)

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
dbutils.widgets.text('drop_power_users', '1000')
dbutils.widgets.dropdown('drop_power_users_override', 'true', ['true','false'])
dbutils.widgets.text('nu_train', '1e6')
dbutils.widgets.text('mr_train', '16667')
dbutils.widgets.text('nu_test', '1e6')
dbutils.widgets.text('mr_test', '16667')

target = dbutils.widgets.get("target")
target_cardinality = {
    'Age':10,
    'Gender':1,
    'AgeGender':20
}[target]
# dpu format: 1000.1, where anything after .1 is ignored to load tfrecords
# after the decimal place is used to mark variants of models
dpu = dbutils.widgets.get('drop_power_users')
full_train = True if '0000' in dpu else False
full_test = dbutils.widgets.get("drop_power_users_override") == 'true'
nutr = dbutils.widgets.get('nu_train')
mrtr = dbutils.widgets.get('mr_train')
nute = dbutils.widgets.get('nu_test')
mrte = dbutils.widgets.get('mr_test')

# COMMAND ----------

if full_train:
    train_path = f'dpu={dpu}'
else:
    train_path = f'dpu={dpu}/nu={nutr}/mr={mrtr}'
    
# look into the folder where models are saved. the date corresponds to the test date in tfrecords
#model_countries = dbutils.fs.ls(f'{mnt_path}/DATPERF-2881/models/target={target}/{train_path}/')
model_countries = dbutils.fs.ls(f'{mnt_path}/models/dev/pythia/demoModel/models/target={target}/{train_path}/')
model_countries = sorted([c.name.split('=')[1][:-1] for c in model_countries])

dbutils.widgets.dropdown('country_to_model', model_countries[0], model_countries)
country_to_model = dbutils.widgets.get("country_to_model")

#model_dates = dbutils.fs.ls(f'{mnt_path}/DATPERF-2881/models/target={target}/{train_path}/Country={country_to_model}/')
model_dates = dbutils.fs.ls(f'{mnt_path}/models/dev/pythia/demoModel/models/target={target}/{train_path}/Country={country_to_model}/')
model_dates = [d.name.split('=')[1][:-1] for d in model_dates]

dbutils.widgets.dropdown("date_to_process", model_dates[0], model_dates)
date_to_process = dbutils.widgets.get("date_to_process")

if full_test:
    # test on the entire, unresampled geronimo dataset
    test_path = 'unresampled'
else:
    # test on the entire, resampled geronimo dataset
    test_path = f'dpu=1000/nu={nute}/mr={mrte}'

# grab hash table between two-digit code and country
#tfrecord_dates = f'{mnt_path}/DATPERF-2811/tfrecords/{test_path}/country_hash/Date={date_to_process}/'
tfrecord_dates = f'{mnt_path}/features/data/pythia/demo/v=1/dev/tfrecords/{test_path}/country_hash/Date={date_to_process}/'
df = spark.read.option('header', 'true').csv(tfrecord_dates).toPandas()
date_country = dict(zip(df.Country,df.mapped))

#pred_save_path = f"{mnt_path}/DATPERF-2883/test/target={target}/{train_path}/{test_path}/Country={country_to_model}/Date={date_to_process}/"
pred_save_path = f"{mnt_path}/models/dev/pythia/demoModel/test/target={target}/{train_path}/{test_path}/Country={country_to_model}/Date={date_to_process}/"

# https://docs.databricks.com/files/index.html 
# local code, local driver node -> dbfs root (/dbfs) -> mnt
mnt_path = '/dbfs' + mnt_path

#model_filepath = f"{mnt_path}/DATPERF-2881/models/target={target}/{train_path}/Country={country_to_model}/Date={date_to_process}/"
model_filepath = f"{mnt_path}/models/dev/pythia/demoModel/models/target={target}/{train_path}/Country={country_to_model}/Date={date_to_process}/"

#tfrecord_dates = f'{mnt_path}/DATPERF-2811/tfrecords/{test_path}/agegender/Date={date_to_process}/Country={date_country[country_to_model]}/'
tfrecord_dates = f'{mnt_path}/features/data/pythia/demo/v=1/dev/tfrecords/{test_path}/agegender/Date={date_to_process}/Country={date_country[country_to_model]}/'
test_dir = [tfrecord_dates]

# COMMAND ----------

# DBTITLE 0,Performance on a test set
print(train_path)
print(test_path)

model = tf.keras.models.load_model(model_filepath, compile=True)

# for the short lived time when I trained a model and saved it with logits output
add_activation = False
if '.' in dpu and dpu.split('.')[1].isdigit() and (int(dpu.split('.')[1]) >= 17 and int(dpu.split('.')[1]) <= 25):
    add_activation = True

if add_activation:
    if target == 'Gender':
        activation = tf.keras.activations.deserialize('sigmoid')
    else:
        activation = tf.keras.activations.deserialize('softmax')
    # output layer before flatten
    model.layers[-2].activation = activation

# COMMAND ----------

if gpus:
    print('GPU inference')
    tf_parse = demo.feature_parser(target)
    
    def get_results(f):
        # mini function to get the results given a list of files
        test = demo.tfrecord_dataset(f, batch_size, tf_parse.parser(index=True), train=False, deterministic=True)
        #cols = ["Country", "SupplyVendor", "Browser", "OperatingSystemFamily", "RenderingContext", "DeviceMake"]
        labels = demo.get_labels_pandas(test, col=['index']).reset_index().drop('level_0', axis=1)
        pred = model.predict(test, verbose=0)
        return pd.concat((labels,pd.DataFrame(pred)),axis=1).astype('float64')

    # batch files
    test_files = demo.get_tfrecord_files(test_dir)
    n = 5
    test_files = [test_files[i:i + n] for i in range(0, len(test_files), n)]

    df_test = get_results(test_files.pop(0))
    df_result = spark.createDataFrame(df_test)
    df_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(pred_save_path)

    # go through tfrecords files one by one if testing on geronimo
    if test_files:
        for f in tqdm(test_files):
            df_test = get_results(f)
            df_result = spark.createDataFrame(df_test)
            df_result.write.format("delta").mode("append").save(pred_save_path)
else:
    print('CPU inference')

    from pyspark.sql.types import DoubleType, StructType, StructField
    from pyspark.sql.functions import pandas_udf, struct
    from collections.abc import Iterator

    model_features = demo.model_features
    feature_names = []
    for f in model_features:
        feature_names.append(f.name)

    df = spark.read.format("tfrecords").load(test_dir[0][5:])
    # make a country column w/ default value of 0 (see demo_nn.py)
    df = df.withColumn('Country', lit(0))
    # mimic tf with default value of 0 for lat/long null
    df = df.fillna(0.0, subset=["Latitude", "Longitude"])
    df_farray = df.select('index', target, struct(*feature_names).alias("Features"))

    schema = StructType([StructField(str(i), DoubleType()) for i in range(target_cardinality)])
    column_name = [str(i) for i in range(target_cardinality)]

    #bc_model_weights = sc.broadcast(model.get_weights())
    @pandas_udf(schema)
    def predict_batch_udf(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        #model = demo.init_model(model_features, target=target)
        #model.set_weights(bc_model_weights.value)
        model = tf.keras.models.load_model(model_filepath, compile=True)
        if add_activation:
            model.layers[-2].activation = activation
        for data_batch in batch_iter:
            pred = model.predict(dict(data_batch))
            yield pd.DataFrame(pred, columns=column_name)

    # spark loads labels as long but tfparser in demo_nn.py converts to float32 (why?)
    # convert to double because spark mllib wants double for f1 score
    df_result = (df_farray.select('index', target, predict_batch_udf("Features").alias('Pred'))
                 .withColumn(target, col(target).cast('double'))
                 .withColumnRenamed(target, 'LabelValue'))
    df_result = df_result.select('index', 'LabelValue', col('Pred.*')).toDF('index', 'LabelValue', *column_name)

    df_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(pred_save_path)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, pred_save_path)
if gpus:
    deltaTable.optimize().executeCompaction()
if full_test:
    deltaTable.optimize().executeZOrderBy('index')
deltaTable.vacuum(0)
