# Databricks notebook source
# MAGIC %md
# MAGIC # Get AgeGender labels from OWDI
# MAGIC This notebook preprocesses an OWDI list of labeled users for Id's that have an AgeGender label, then saves it for a later join with geronimo.

# COMMAND ----------

dbutils.widgets.text("date_to_process", '2023-07-19')
date_to_process = dbutils.widgets.get("date_to_process")

# generate a test set instead of resampling (only owdi/geronimo join)
dbutils.widgets.dropdown('make_test_set', 'false', ['true', 'false'])
mts = dbutils.widgets.get('make_test_set') == 'true'

# dataset size parameters
dbutils.widgets.text('num_users', '1e6')
nu = dbutils.widgets.get('num_users')
# max number of users per country
num_users = float(dbutils.widgets.get('num_users'))

dbutils.widgets.text("run_id", "0000")
run_id = dbutils.widgets.get('run_id')

# use the data in the 'export' folder from owdi instead of backstage
dbutils.widgets.dropdown('owdi_export', 'false', ['true', 'false'])
owdi_exp = dbutils.widgets.get('owdi_export') == 'true'

# The distance was indirectly standardised by Agrippa's establishment of a standard Roman foot (Agrippa's own) in 29 BC,[9] and the definition of a pace as 5 feet.

# COMMAND ----------

# original notebook: /Users/sunil.ahuja@thetradedesk.com/demo/combine-age-gender-labels
from pyspark.sql.functions import xxhash64, col, broadcast, rand, first, count
import pandas as pd

# parameters
rand_seed = 42

owdi_demo_path = 's3://ttd-datprd-us-east-1/application/radar/data'
if owdi_exp:
    demo_path = f'{owdi_demo_path}/export/general/Demographic/Date={date_to_process}/'
else:
    demo_path = f"{owdi_demo_path}/backstage/EngineeredUserWithLabel/Date={date_to_process}/"

base_path = 's3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811'
#base_path = 's3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/v=1/dev'
owdi_path = f"{base_path}/owdi_preprocess/{run_id}/"
def save_owdi(df):
    if run_id == '0000': return
    (df.withColumn("UIIDHash" , xxhash64(col("UserId")))
    .drop('UserId', 'Country')
    .coalesce(64)
    .write
    .mode('overwrite')
    .format('parquet')
    .save(owdi_path))

# COMMAND ----------

df_demo = spark.read.parquet(demo_path)

if owdi_exp:
    df_demo = (df_demo.select('UserId', 'Country', 'Gender', 'AgeRangeStart')
               .filter("GenderSource = 'seed' and AgeSource == 'seed'")
               .withColumnRenamed('Gender', 'GenderString'))
    
    # map the age ranges / gender strings to labels 0-9 and 0,1 
    age = [[18,0], [21,1], [25,2], [30,3], [35,4], [40,5], [45,6], [50,7], [55,8], [65,9]]
    gender = [['F',0], ['M',1]]

    Age = pd.DataFrame(age, columns=['AgeRangeStart', 'Age'])
    Gender = pd.DataFrame(gender, columns=['GenderString', 'Gender'])

    dfa = spark.createDataFrame(Age)
    dfg = spark.createDataFrame(Gender)
    
    df_demo_pivoted = (df_demo
                       .join(broadcast(dfa), 'AgeRangeStart')
                       .join(broadcast(dfg), 'GenderString')
                       .select('UserId','Country', 'Age', 'Gender'))
else: 
    # backstage: ['UserId', 'Country', 'ProviderId', 'LabelName', 'LabelValue']
    # a list of countries with the best provider and weighted f measure
    ym = '-'.join(date_to_process.split('-')[:2])
    base_path = 's3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811'
    #base_path = 's3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/v=1/dev'
    metrics_path = f'{base_path}/owdi_metrics/summary/Date={ym}/'

    print(f'Using the summary {ym}')
    df_demo_provider = spark.read.option('header', 'true').csv(metrics_path).drop('weightedFMeasure')

    # only take records from labeled users that are of the highest quality
    df_demo_hq = df_demo.join(broadcast(df_demo_provider), ['Country', 'LabelName', 'ProviderId']).cache()

    display(df_demo_hq.groupby('Country','ProviderId','LabelName').count()
            .sort('Country', 'LabelName'))

    # Combine age and gender labels
    df_demo_pivoted = (df_demo_hq
                       .groupby(['Country', 'UserId'])
                       .pivot('LabelName').agg(first('LabelValue'))
                       .drop('ProviderId'))  # providerid is not a geronimo feature
    df_demo_hq.unpersist()

df_demo_count = df_demo_pivoted.count()
print(f'df_demo partitions: {df_demo.rdd.getNumPartitions()}')

# COMMAND ----------

# what fraction of user labels are 'bad' (no age OR gender label)
# this filtering step eliminates a lot of countries
df_demo_ag = df_demo_pivoted.filter(col('Age').isNotNull() & col('Gender').isNotNull())
df_demo_ag = df_demo_ag.withColumn('AgeGender', col('Age') + 10*col('Gender')).cache()
df_demo_ag_count = df_demo_ag.count()
display(df_demo_ag.groupby('Country','AgeGender').count().sort('Country','AgeGender'))

df_country_count_bef_filter = df_demo_pivoted.groupby('Country').agg(count('Country').alias('count_bef_filter'))
df_country_count_aft_filter = df_demo_ag.groupby('Country').agg(count('Country').alias('count_aft_filter'))
df_country_counts = df_country_count_bef_filter.join(df_country_count_aft_filter, 'Country', 'outer')
df_country_counts = df_country_counts.withColumn('fraction_retained', col('count_aft_filter')/col('count_bef_filter'))
display(df_country_counts)
countries_before = df_demo_pivoted.select('Country').distinct().count()
countries_retained = df_country_counts.select('Country').distinct().count()
print(f'Countries in owdi set before/after filter: {countries_before}/{countries_retained}')

# COMMAND ----------

if mts:
    save_owdi(df_demo_ag)
    dbutils.notebook.exit("Save all OWDI users, do not resample")

# COMMAND ----------

# retain up to num_users of labeled users for Country + AgeGender combination
df_demo_agg = df_demo_ag.groupby(['Country', 'AgeGender']).agg(count(col('AgeGender')).alias('AgeGenderCount'))
df_demo_agg_pandas = df_demo_agg.toPandas().dropna()
num_users_to_retain = df_demo_agg_pandas['AgeGenderCount'].median()
num_users_to_retain = max(num_users_to_retain, num_users)

# retain a maximum of num_users_to_train users per country for training
df_demo_agg_pandas['AgeGenderCountRetain'] = df_demo_agg_pandas.apply(lambda x: num_users_to_retain if x['AgeGenderCount'] > num_users_to_retain else x['AgeGenderCount'], axis=1)
df_demo_agg_pandas['retention_fraction'] = df_demo_agg_pandas['AgeGenderCountRetain']/df_demo_agg_pandas['AgeGenderCount']
display(df_demo_agg_pandas.sort_values(['Country', 'AgeGender']))

# COMMAND ----------

df_ag_filter = spark.createDataFrame(df_demo_agg_pandas).drop('AgeGenderCount', 'AgeGenderCountRetain')
df_demo_joined = df_demo_ag.join(broadcast(df_ag_filter), ['Country', 'AgeGender'])
df_demo_joined = df_demo_joined.withColumn('rand', rand(seed=rand_seed))

# randomly retain up to num_users_to_retain
df_demo_balanced = df_demo_joined.filter(df_demo_joined['rand'] < df_demo_joined['retention_fraction'])
display(df_demo_balanced.groupby('Country', 'AgeGender').count().sort('Country','AgeGender'))
df_demo_balanced_count = df_demo_balanced.count()

# COMMAND ----------

print(f'export/backstage+hq count: {df_demo_count:,}')
print(f'ag count: {df_demo_ag_count:,}')
print(f'after resample count: {df_demo_balanced_count:,}')
print(f'fractional after hq: {df_demo_ag_count/df_demo_count*100:.3}')
print(f'fraction after resample: {df_demo_balanced_count/df_demo_count*100:.3}')

# COMMAND ----------

save_owdi(df_demo_balanced.drop('retention_fraction', 'rand'))
