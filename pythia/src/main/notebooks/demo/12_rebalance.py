# Databricks notebook source
# MAGIC %md
# MAGIC # Filter and resample Geronimo
# MAGIC This notebook uses a pre-computed list of high-quality demo providers to select the relevant labeled users from OWDI. These users are then joined with their browsing behaviour from the Geronimo dataset to form training data. Some countries are lost because they can't provide AgeGender labels(?)
# MAGIC
# MAGIC The second process then upsamples or downsamples countries that have a minimum amount of records so the age/gender labels are evenly balanced.
# MAGIC
# MAGIC Note: Geronimo is severely skewed to the US

# COMMAND ----------

# date to process
dbutils.widgets.text("date_to_process", '2023-07-19')
date_to_process = dbutils.widgets.get("date_to_process")

# hour to process
y = date_to_process.split('-')[0]
m = date_to_process.split('-')[1]
d = date_to_process.split('-')[2]
dbutils.widgets.text("hour_part", '0')
h = dbutils.widgets.get("hour_part")

# include thirdpartytargetingdataIds?
dbutils.widgets.dropdown('thirdparty', 'false', ['true', 'false'])
tpd = dbutils.widgets.get('thirdparty') == 'true'

# generate a test set instead of resampling (only owdi/geronimo join)
dbutils.widgets.dropdown('make_test_set', 'false', ['true', 'false'])
mts = dbutils.widgets.get('make_test_set') == 'true'

# keep the top n'th percentile, given in thousandths (1000 = 100%)
dbutils.widgets.text('drop_power_users', '1000')
dpu = dbutils.widgets.get('drop_power_users')
pdpu = float(dpu)/1000 
assert(pdpu <= 1)

# dataset size, processed per hourPart
dbutils.widgets.text('num_users', '1e6')
nu = dbutils.widgets.get('num_users')
dbutils.widgets.text('min_rows_per_label', '16667')
min_rows_per_label = float(dbutils.widgets.get('min_rows_per_label'))
mr = dbutils.widgets.get('min_rows_per_label')

dbutils.widgets.text("run_id", "0000")
run_id = dbutils.widgets.get('run_id')

# COMMAND ----------

# original notebook: /Users/sunil.ahuja@thetradedesk.com/demo/combine-age-gender-labels
from pyspark.sql.functions import xxhash64, col, count, percentile_approx
from pyspark.sql.functions import broadcast, rand, first, lit
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import pandas as pd
import numpy as np

#spark.sparkContext.setCheckpointDir('s3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811/scratch/')
spark.conf.set("spark.sql.shuffle.partitions","auto")

# io cache enabled by default on certain clusters
print(f"spark.databricks.adaptive.autoOptimizeShuffle.enabled: {spark.conf.get('spark.databricks.adaptive.autoOptimizeShuffle.enabled')}")
print(f"spark.databricks.optimizer.adaptive.enabled: {spark.conf.get('spark.databricks.optimizer.adaptive.enabled')}")
print(f"spark.databricks.io.cache.enabled: {spark.conf.get('spark.databricks.io.cache.enabled')}")
print(f"spark.sql.adaptive.skewJoin.enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
print(f"spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# parameters
rand_seed = 42
min_rows_per_country = 120000
param_path = f'dpu={dpu}/nu={nu}/mr={mr}'

geronimo_path = f's3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year={y}/month={m}/day={d}/hourPart={h}/'

#base_path = 's3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811'
base_path = 's3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/v=1/dev'
demo_path = f"{base_path}/owdi_preprocess/{run_id}"
final_path = f"{base_path}/scratch/{param_path}/Date={date_to_process}/hourPart={h}/"

# COMMAND ----------

# ['UserId', 'Country', 'ProviderId', 'LabelName', 'LabelValue']
df_demo = spark.read.parquet(demo_path)

# koa v4 feature - anything available at bid time
try:
    dbutils.fs.ls(geronimo_path)
except:
    dbutils.notebook.exit("Hour part does not exist (3 hr vs 1 hr buckets)")
    
df_geronimo = spark.read.parquet(geronimo_path)
df_geronimo = df_geronimo.drop('BidRequestId')
if not tpd:
    df_geronimo = df_geronimo.drop('ThirdPartyTargetingDataIds', 'GroupThirdPartyTargetingDataIds')
    
# these null/invalid tdid's can contribute a lot to skew joins
df_geronimo = df_geronimo.filter(col('UIID').isNotNull() & (col('UIID') != '00000000-0000-0000-0000-000000000000'))

print(f'df_geronimo partitions: {df_geronimo.rdd.getNumPartitions()}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocess Geronimo
# MAGIC Discard power users beyond a certain percentile from geronimo. Note: dropped traffic includes null/0000, so the statistics printed here are not indicative of bots. Use the skew_check notebook for an accurate measure

# COMMAND ----------

if pdpu < 1 and not mts:
    df_pu = (df_geronimo.select('Country','UIID')
    .groupby('Country','UIID')
    .agg(count('*').alias('Activity'))).cache()

    df_pu2 = (df_pu.groupby('Country').agg(
    percentile_approx('Activity', .50).alias('p50'),
    percentile_approx('Activity', .75).alias('p75'),
    percentile_approx('Activity', .95).alias('p95'),
    percentile_approx('Activity', .99).alias('p99'),
    percentile_approx('Activity', .995).alias('p995'),
    percentile_approx('Activity', .999).alias('p999'),
    percentile_approx('Activity', pdpu).alias('keep_p'),
    F.max('Activity').alias('MaxActivity'),
    F.sum('Activity').alias('TotalActivity'))).cache()
    display(df_pu2.sort('TotalActivity', ascending=False))

    df_pu3 = (df_pu.join(df_pu2.select('Country','keep_p'),'Country')
    .where(f'Activity > keep_p')).cache()

    df_pu4 = df_pu2.select('Country','TotalActivity')
    display(df_pu3.groupby('Country').agg(F.sum('Activity').alias('BotActivity'))
    .join(df_pu4,'Country')
    .withColumn('BotFraction', col('BotActivity')/col('TotalActivity'))
    .sort('TotalActivity', ascending=False))

    df_pu5 = df_pu.groupby('Country').agg(count('*').alias('TotalUsers'))
    display(df_pu3.groupby('Country').agg(count('*').alias('BotUsers'))
    .join(df_pu5,'Country')
    .withColumn('BotFraction', col('BotUsers')/col('TotalUsers'))
    .sort('TotalUsers', ascending=False))

# COMMAND ----------

if pdpu < 1 and not mts:
    print(f'{df_geronimo.count():,}')
    print(f'Dropping traffic from top {pdpu*100} percentile of TDIDs')
    # by selecting top percentile of bots/users, we can use antijoin + broadcast
    df_geronimo = df_geronimo.join(broadcast(df_pu3.select('UIID')),'UIID','anti')
    df_pu.unpersist()
    df_pu2.unpersist()

df_geronimo_count = df_geronimo.count()

# COMMAND ----------

# stop execution early to save a test set
# only filtering is Ids that have the combined agegender labels
df_geronimo = df_geronimo.withColumn("UIIDHash", xxhash64(col("UIID"))).drop("UIID")
if mts:
    final_path = final_path.replace(param_path, 'unresampled')

    df = df_geronimo.join(df_demo, "UIIDHash", "inner")
    df_count = df.count()
    
    print(f'Before/after OWDI join: {df_geronimo_count:,}/{df_count:,}')
    print(f'Percent geronimo retained: {df_count/df_geronimo_count*100:.3}')

    # coalesce is better than repartition, because the skew puts all of US onto one worker/executor
    (df.coalesce(50)
     .write.partitionBy('Country')
     .mode('overwrite')
     .format('parquet')
     .save(final_path))

# COMMAND ----------

if mts:
    dbutils.notebook.exit("Generating a test set, no additional resampling will be done")

# COMMAND ----------

# MAGIC %md
# MAGIC # Join with OWDI

# COMMAND ----------

# join users to geronimo (bidfeedback/bidrequest/browsing history) data
df = df_geronimo.join(df_demo, "UIIDHash", "inner").cache()
df_count = df.count()

print(f'Percent geronimo retained: {df_count/df_geronimo_count*100:.3}')
print(f'geronimo joined with users count: {df_count:,}, geronimo count: {df_geronimo_count:,}')
df_partitions = df.rdd.getNumPartitions()
print(f'df partitions: {df_partitions}')
print(df.columns,len(df.columns))

if pdpu < 1 and not mts:
    df_pu3.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC # Resample

# COMMAND ----------

# this groupby aggregates replicates of tdid (i.e. browsing history)
dfp = (df.withColumn('salt', (rand() * df_partitions).cast(IntegerType()))
  .groupBy('salt', 'Country', 'AgeGender')
  .agg(count('*').alias('salt_count'))
  .groupBy('Country', 'AgeGender')
  .agg(F.sum('salt_count').alias('count'))
  .drop('salt')).toPandas()
#dfp = df.groupby('Country', 'AgeGender').count().toPandas()

display(dfp.sort_values('count', ascending=False))

# COMMAND ----------

# is fillna necessary? or is it for other columns? agegender is already filtered
# pivots agegender to column, then looks along columns to find the 90% quintile
print(dfp.set_index(['Country', 'AgeGender'])
.unstack()
.fillna(0)
.quantile(0.9, axis=1)
.sort_values(ascending=False).to_string())

# COMMAND ----------

dfp['count'].quantile([0, 0.5, 0.75, 0.9])

# COMMAND ----------

# drop records that do not have a country
df_min_count = dfp.dropna()
# only keep countries that have a minimum amount of rows
br_x_country = df_min_count.groupby('Country')['count'].sum().sort_values(ascending=False)
br_x_country = br_x_country[br_x_country > min_rows_per_country]
# if there are any na, only drop the one associated with the country/agegender
#df_min_count_subset = df_min_count.set_index(['Country', 'AgeGender']).loc[idx[br_x_country.index,:],:].dropna()

#df_min_count = dfp.groupby(['Country', 'AgeGender']).min('count').reset_index() # drops records without countries
#br_x_country = df_min_count.set_index(['Country', 'AgeGender']).unstack().sum(axis=1).sort_values(ascending=False)
#br_x_country = br_x_country[br_x_country > min_rows_per_country]
# dropna removes any country with any nan? seems harsh?
df_min_count_subset = df_min_count.set_index(['Country', 'AgeGender']).unstack().loc[br_x_country.index].dropna().stack()

df_min_count_subset['min_count'] = min_rows_per_label
df_min_count_subset['retention_fraction'] = df_min_count_subset['min_count']/df_min_count_subset['count']

df_min_count_subset_downsample = df_min_count_subset[df_min_count_subset['retention_fraction'] <= 1]
df_min_count_subset_upsample = df_min_count_subset[df_min_count_subset['retention_fraction'] > 1]

# COMMAND ----------

print(df_min_count_subset_upsample.to_string())

# COMMAND ----------

print(df_min_count_subset_downsample.to_string())

# COMMAND ----------

df_min_count_upsample_spark = spark.createDataFrame(df_min_count_subset_upsample.reset_index()).select('Country', 'AgeGender', 'retention_fraction')
df_min_count_downsample_spark = spark.createDataFrame(df_min_count_subset_downsample.reset_index()).select('Country', 'AgeGender', 'retention_fraction')

# associate 'retention fraction' with every record
df_to_upsample = df.join(broadcast(df_min_count_upsample_spark), on=['Country', 'AgeGender'])
# e.g. {('United States', 19): 1.1084262564011615, ... }
sampling_frac = df_min_count_subset_upsample['retention_fraction'].to_dict()

df_to_upsample_resampled = df_to_upsample.rdd.map(
    lambda row: ((row['Country'], row['AgeGender']), row)
).sampleByKey(
    withReplacement=True, fractions=sampling_frac
).map(
    lambda row: row[1]
)

dftest_resampled_df = spark.createDataFrame(df_to_upsample_resampled, df_to_upsample.schema) 

df_to_downsample = df.join(broadcast(df_min_count_downsample_spark), on=['Country', 'AgeGender'])
df_to_downsample = df_to_downsample.withColumn('rand', rand(seed=rand_seed))
df_to_downsample_resampled = df_to_downsample.filter('rand <= retention_fraction')

# COMMAND ----------

df_final = (dftest_resampled_df.drop('retention_fraction')
.union(df_to_downsample_resampled.drop('retention_fraction', 'rand')))

display(df_final.groupBy('Country', 'AgeGender').count().orderBy('Country', 'AgeGender'))

# COMMAND ----------

(df_final.coalesce(50)
 .write.partitionBy('Country')
 .mode('overwrite')
 .format('parquet')
 .save(final_path))
