# Databricks notebook source
# MAGIC %md
# MAGIC # Import and setup

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

from collections import Counter
import os
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from tqdm import tqdm

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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

def load_closest_date(path, date):
    files = dbutils.fs.ls(path)
    dates = [d.name.split('=')[1][:-1] for d in files] # "Date=YYYY-mm-dd"
    delta = [abs(datetime.strptime(d,'%Y-%m-%d')-datetime.strptime(date,'%Y-%m-%d')) for d in dates]
    delta = [dt.total_seconds() for dt in delta]
    return files[delta.index(min(delta))].path

# COMMAND ----------

dbutils.widgets.text('date_to_process', '2023-07-19')
dbutils.widgets.text('country_to_model', 'United States')
# specify the test set parameters
dbutils.widgets.dropdown('drop_power_users_override', 'false', ['true','false'])
dbutils.widgets.text('drop_power_users', '1000')
dbutils.widgets.text('nu_test', '1e6')
dbutils.widgets.text('mr_test', '16667')

date_to_process = dbutils.widgets.get('date_to_process')
country_to_model = dbutils.widgets.get('country_to_model')
# get test set parameters
full_test = dbutils.widgets.get("drop_power_users_override") == 'true'
# test: should always be 1000
dpute = dbutils.widgets.get('drop_power_users')
nute = dbutils.widgets.get('nu_test')
mrte = dbutils.widgets.get('mr_test')

# for thresholding binary values
thres = 0.5

# COMMAND ----------

max_index = F.udf(lambda x: x.index(max(x)))

def threshold(df, thres=0.5):
    if df.shape[1] > 1:
        lbls = [float(i_) for i_ in range(df.shape[1])]
        df = df.idxmax(axis=1).astype(np.short)
    else:
        lbls = [0.,1.]
        df[df >= thres] = 1
        df[df < thres] = 0
    return df.squeeze(), lbls

# COMMAND ----------

first = ['AgeGender','Age+Gender']
second = ['AgeGender','Age','Gender']
layers = [first, second]

index = pd.MultiIndex.from_product(layers)
# df metrics
dfm = pd.DataFrame(columns=index)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get benchmark scores

# COMMAND ----------

owdi_path = 's3://ttd-datprd-us-east-1/application/radar/data/backstage/SelectedModelMetric/'
df_owdi_selected_metrics = spark.read.parquet(load_closest_date(owdi_path, date_to_process)).select('provider', 'weightedFMeasure', 'Country', 'LabelName', 'UserIdType').toPandas()

# shortName <-> longName
country_codes = spark.read.option('header', 'true').csv('dbfs:/FileStore/bryan/theTradeDesk_provisioning2_Country.csv').toPandas()
country_code = country_codes.ShortName[country_codes.LongName == country_to_model].iat[0]

for t in ['Age','Gender']:
    sm = df_owdi_selected_metrics[(df_owdi_selected_metrics['Country']==country_code) &
                                  (df_owdi_selected_metrics['LabelName']==t)]
    sm = sm.assign(provider = sm['provider'] + '_' + sm['UserIdType'])
    for ix, row in sm.iterrows():
        dfm.loc[row['provider'], (slice(None), t)] = row['weightedFMeasure']

# COMMAND ----------

# make a list of all directories to be analyzed
if full_test:
    test_path = 'unresampled'
else:
    test_path = f'dpu={dpute}/nu={nute}/mr={mrte}'
test_path += f"/Country={country_to_model}/Date={date_to_process}/"

#path = f"/dbfs{mnt_path}/DATPERF-2883/test/"
path = f"/dbfs{mnt_path}/models/dev/pythia/demoModel/test/"
paths = [x[0]+'/' for x in os.walk(path) if x[2] and '_delta_log' not in x[0]]
result_paths = [p[5:] for p in paths if test_path in p]
#result_paths = [p for p in result_paths if 'dpu=0000' in p]
#result_paths = [p for p in result_paths if int(p.split('/')[6].split('.')[1]) >= 30 and int(p.split('/')[6].split('.')[1]) <= 34]
print('\n'.join([p.replace(path[5:],'') for p in result_paths]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyze test results

# COMMAND ----------

# default f1 maps to weightedFMeasure, see
# https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.scala
# compare job runs 73014820 and 72900523
evaluator = MulticlassClassificationEvaluator(predictionCol='LabelPred', labelCol='LabelValue')
evaluator.setMetricName('weightedFMeasure')

for rp in tqdm(result_paths):
    df = spark.read.format("delta").load(rp)
    labels = df.columns[2:]

    params = rp.split('/')
    target_idx = ['target=' in field for field in result_paths[0].split('/')].index(True)
    target = params[target_idx].split('=')[1]
    model_string = '/'.join(params[target_idx+1:target_idx+4]) if 'dpu=0000' not in params[target_idx+1] else params[target_idx+1]
    if model_string not in dfm.index:
        dfm.loc[model_string] = [None]*6

    if 'Age' in target:
        df = df.withColumn("data", F.array(labels))
        df = df.withColumn('LabelPred', max_index(df.data).cast('double')).drop('data', *labels)
    else:
        df = df.withColumn('LabelPred', F.when(F.col("0") >= thres, 1).otherwise(0).cast('double')).drop('0')
    f1 = evaluator.evaluate(df)

    if target == 'AgeGender':
        dfm.loc[model_string][0] = f1

        df = df.withColumn('AgePred', F.col('LabelPred') % 10)
        df = df.withColumn('AgeValue', F.col('LabelValue') % 10)
        dfm.loc[model_string][1] = evaluator.setPredictionCol('AgePred').setLabelCol('AgeValue').evaluate(df)

        df = df.withColumn('GenderPred', F.floor(F.col('LabelPred') / 10).cast('double'))
        df = df.withColumn('GenderValue', F.floor(F.col('LabelValue') / 10).cast('double'))
        dfm.loc[model_string][2] = evaluator.setPredictionCol('GenderPred').setLabelCol('GenderValue').evaluate(df)

        # reset evaluator columns
        evaluator.setPredictionCol('LabelPred').setLabelCol('LabelValue')
    elif target == 'Age':
        dfm.loc[model_string][4] = f1
    elif target == 'Gender':
        dfm.loc[model_string][5] = f1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Age+Gender vs AgeGender

# COMMAND ----------

# DBTITLE 0,Age+Gender inference
#metrics_path = f'/dbfs/FileStore/bryan/demo/'
metrics_path = f'/dbfs{mnt_path}/models/dev/pythia/demoModel/metrics/'

missing = []
for train in tqdm([m for m in dfm.index.to_numpy() if 'dpu' in m]):
    ag_paths = [p for p in result_paths if train in p.replace(test_path, '')]
    a_path = [p for p in ag_paths if 'Age/' in p]
    ag_path = [p for p in ag_paths if 'AgeGender/' in p]
    g_path = [p for p in ag_paths if 'Gender/' in p]
    a_path = a_path.pop() if a_path else None
    ag_path = ag_path.pop() if ag_path else None
    g_path = g_path.pop() if g_path else None

    if a_path:
        dfa = spark.read.format("delta").load(a_path).withColumnRenamed('LabelValue','Age')
        lbls = dfa.columns[2:]
        dfa = dfa.withColumn("data", F.array(lbls))
        dfa = dfa.withColumn('AgePred', max_index(dfa.data).cast('double')).drop('data', *lbls)

    if g_path:
        dfg = spark.read.format("delta").load(g_path).withColumnRenamed('LabelValue','Gender')
        dfg = dfg.withColumn('GenderPred', F.when(F.col("0") >= thres, 1).otherwise(0).cast('double')).drop('0')
    
    apg = pd.DataFrame(data={'count':[0]*20})
    if a_path and g_path:
        dfapg = dfa.join(dfg, 'index')
        dfapg = (dfapg.withColumn('LabelValue', F.col('Age') + 10*F.col('Gender'))
                .withColumn('LabelPred', F.col('AgePred') + 10*F.col('GenderPred')))
        dfm.loc[train][3] = evaluator.evaluate(dfapg)
        # set the index to the agegender labels so they will line up when doing arithmetic
        apg = dfapg.groupby('LabelPred').count().toPandas().set_index('LabelPred').sort_index()

    ag = pd.DataFrame(data={'count':[0]*20})
    mragg = pd.DataFrame(data={'Test':[0]*20})
    if ag_path:
        dfag = spark.read.format("delta").load(ag_path).withColumnRenamed('LabelValue','AgeGender')
        lbls = dfag.columns[2:]
        dfag = dfag.withColumn("data", F.array(lbls))
        dfag = dfag.withColumn('AgeGenderPred', max_index(dfag.data).cast('double')).drop('data', *lbls)
        ag = dfag.groupby('AgeGenderPred').count().toPandas().set_index('AgeGenderPred').sort_index()
        mragg = dfag.groupby('AgeGender').count().toPandas().set_index('AgeGender').sort_index()
        mragg = mragg.rename(columns={'count':'Test'})
    
    # merge ag and apg pd df's
    dfp = apg.rename(columns={'count':'Separate'}).merge(ag.rename(columns={'count':'Combined'}), left_index=True, right_index=True, how='outer')
    dfp = dfp.fillna(0)
    dfp.index.name = 'AgeGender'
    dfp = dfp.merge(mragg, left_index=True, right_index=True)
    mr = mragg['Test']

    if (dfp.Separate==0).any() and a_path and g_path:
        missing.append(f'{train} Age+Gender is missing {dfp.index[dfp.Separate==0].tolist()} labels!')
    if (dfp.Combined==0).any() and ag_path:
        missing.append(f'{train} AgeGender is missing {dfp.index[dfp.Combined==0].tolist()} labels!')

    # figures
    fig, ax = plt.subplots(1, 2, figsize=(12,4))
    plt.subplots_adjust(wspace=.125)
    #dfp.plot.bar(ax=ax[1], width=.7)
    ax[1].stairs(dfp['Separate'], np.arange(21)-0.5, lw=2, label='Separate')
    ax[1].stairs(dfp['Combined'], np.arange(21)-0.5, lw=2, label='Combined')
    plt.xticks(rotation=90)
    ax[1].step(mr.index.to_numpy(), mr, 'k-', lw=2, label='Test', where='mid')
    ax[1].set_ylabel('Count')
    leg = ax[1].legend(bbox_to_anchor=(1,1.025), loc='upper left')

    dfp[['SepError', 'ComError']] = 0
    if a_path and g_path:
        dfp['SepError'] = (dfp['Separate'] - dfp['Test'])/dfp['Test']
    if ag_path:
        dfp['ComError'] = (dfp['Combined'] - dfp['Test'])/dfp['Test']
    (100*dfp[['SepError', 'ComError']]).plot.bar(ax=ax[0], width=.95)
    ax[0].set_ylabel('Percent error')
    #ax[0].get_legend().remove()

    fig.suptitle(train)
    for a in ax:
        a.set_xticks(ticks=np.arange(20), labels=[str(i) for i in range(20)])
        a.set_xlim((-1,20))
        a.tick_params(bottom=True, top=True, left=True, right=True, direction='in', length=6)

    if ag_path:
        dfp_path = metrics_path + 'dfp_' + ag_path.replace(path[5:],'').replace('/','-').replace(' ','')[:-1] + '.csv'
        dfp.to_csv(dfp_path)
print('\n'.join(missing))

# COMMAND ----------

# confirmatory line of code to see that index lines up
#display(dfapg.select('index','AgeGender').join(dfag.select('index','AgeGender'),'index'))

# COMMAND ----------

dfm_path = metrics_path + 'dfm_' + test_path.replace('/','-').replace(' ','')[:-1] + '.csv'
dfm.to_csv(dfm_path)

# COMMAND ----------

dfm
