# Databricks notebook source
# currently, we process all entries within a month from backstage:
# suboptimal choice vs looking back thirty days
owdi_path = "s3://ttd-datprd-us-east-1/application/radar/data/backstage/SelectedModelMetric/"
files = dbutils.fs.ls(owdi_path)
ymd = [e.name.split('=')[1] for e in files]
ym = sorted(set(['-'.join(e.split('-')[:2]) for e in ymd]))

dbutils.widgets.dropdown('YYYY-mm to process',ym[-1],ym)
ymtp = dbutils.widgets.get('YYYY-mm to process')

# base_path = 's3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811'
base_path = 's3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/v=1/dev'
out_path = f'{base_path}/owdi_metrics'

# COMMAND ----------

import pandas as pd
import datetime

# COMMAND ----------

# DBTITLE 1,Find the provider with the highest F1 score by count
df_agg_all = []
for f in files:
  if ymtp in f.name:
    print(f.name)
    df_metrics_owdi_selected = spark.read.parquet(f.path).toPandas()
    # hack that converts each instance of best provider to '1' so it can be summed
    df_agg = df_metrics_owdi_selected[['Country', 'ProviderId', 'LabelName', 'UserIdType',  'weightedFMeasure']] \
    .groupby(['Country', 'ProviderId', 'LabelName', 'UserIdType']) \
    .count()
    df_agg_all.append(df_agg)

df_sum = df_agg_all[0]
for df in df_agg_all[1:]:
  # 'cartesian' add, when fill_vaue is selected
  df_sum = df_sum.add(df, axis='index', fill_value=0)

# COMMAND ----------

# the first of best ProviderId and UserIdType (alphabetical)
# assumes provider/userid's are the same, if they have the same max Fscore counts
print(df_sum.sort_values('weightedFMeasure', ascending=False)
.reset_index()
.groupby(['Country', 'LabelName']).first().to_string())

# COMMAND ----------

# this analysis sums over UserIdTypes, and grabs the provider that provides the highest Fmeasures across the most IdTypes
print(df_sum.groupby(['Country', 'ProviderId', 'LabelName'])
.sum()
.sort_values(by='weightedFMeasure', ascending=False)
.reset_index()
.groupby(['Country' , 'LabelName']).first()
.sort_values('weightedFMeasure', ascending=False)
.sort_index().to_string())

# COMMAND ----------

df_raw_all = []
for f in files:
  if ymtp in f.name:
    print(f.name)

    df_metrics_owdi = spark.read.parquet(f.path).toPandas()
    df_metrics_owdi = df_metrics_owdi[['Country', 'ProviderId', 'LabelName', 'UserIdType',  'weightedFMeasure']]
    df_metrics_owdi['date'] = f.name.split('=')[1][:-1] # format is 'Date=2023-03-03/'

    # take the highest F1 by country and age/gender
    df_agg = (df_metrics_owdi.groupby(['Country', 'LabelName'])
                             .agg(percn=('weightedFMeasure','max')).mul(.9)
                             .reset_index()) # groupby produces multiindex
    df_summ = pd.merge(df_agg, df_metrics_owdi, on=['Country',  'LabelName'])
    # only take any measures that are within 90% of the highest F1
    df_summ = df_summ[df_summ['weightedFMeasure'] >= df_summ['percn']]

    df_raw_all.append(df_summ)
df_raw_all_agg = pd.concat(df_raw_all)

dfn = df_raw_all_agg
# a provider that does well across more UserIdTypes is better than a provider that only does one UserIdType
dfnp_counts = dfn.groupby(['Country', 'LabelName', 'ProviderId'])['weightedFMeasure'].count().sort_values(ascending=False)
# select provider by dropping duplicates and grabbing only the first of Country/LabelName mix
dfnp_summary = dfnp_counts.reset_index().drop_duplicates(['Country', 'LabelName'], keep='first').set_index(['Country', 'LabelName']).sort_index()

# COMMAND ----------

print(dfnp_summary.to_string())
# hack lucid to be the provider for the US
dfnp_summary.loc['US', 'ProviderId'] = 'lucid'

# COMMAND ----------

(spark.createDataFrame(df_raw_all_agg)
.coalesce(1)
.write.mode('overwrite')
.parquet(f'{out_path}/raw/Date={ymtp}/'))

(spark.createDataFrame(dfnp_summary.reset_index())
.coalesce(1)
.write.mode('overwrite')
.option('header','true')
.csv(f'{out_path}/summary/Date={ymtp}/'))
