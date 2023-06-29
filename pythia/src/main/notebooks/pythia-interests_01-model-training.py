# Databricks notebook source
# Adding the repository parent path for the model libraries to the PATH
import sys
model_libraries_path = "/Workspace/Repos/karen.schettlinger@thetradedesk.com/mlplatform/pythia/src/main/python/"
if model_libraries_path not in sys.path:
    sys.path.append(model_libraries_path)

# COMMAND ----------

# Mounting the S3 directory
from databricks.mount import *

mount("s3a://thetradedesk-mlplatform-us-east-1",
      "/mnt/mlplatform")

# COMMAND ----------

# Importing the interestModel class
from pythia.interestModel.interestModel import *

# COMMAND ----------

# Getting the date (for year & month) from the task parameter settings
date = dbutils.widgets.get("date")
train_year  = int(date[0:4])
train_month = int(date[5:7])
train_day = 1 # always uses the data from the first of the month for training the model

# COMMAND ----------

# Specifying the model input / output paths and some model parameters
data_path         = "/dbfs/mnt/mlplatform/features/data/pythia/interests/v=1/prod/interestModelInput"
model_path_parent = "/dbfs/mnt/mlplatform/models/dev/pythia/interestModel"
model_name        = "interestModel"
target_name       = "Labels"
tabnet_factor     = 0.0

# COMMAND ----------

# Setting parameters to be passed on to the next task following the model training
dbutils.jobs.taskValues.set(key = 'train_year', value = train_year)
dbutils.jobs.taskValues.set(key = 'train_month', value = train_month)
dbutils.jobs.taskValues.set(key = 'train_day', value = train_day)

dbutils.jobs.taskValues.set(key = 'data_path', value = data_path)
dbutils.jobs.taskValues.set(key = 'model_path_parent', value = model_path_parent)
dbutils.jobs.taskValues.set(key = 'model_name', value = model_name)
dbutils.jobs.taskValues.set(key = 'target_name', value = target_name)
dbutils.jobs.taskValues.set(key = 'tabnet_factor', value = tabnet_factor)

# COMMAND ----------

# Printing the training date in the notebook
f"{train_year}-{train_month:02d}-{train_day:02d}"

# COMMAND ----------

# Model training
interest_model = interestModel() # initializing the model

interest_model.train(
    # Input data parameters  
    train_year  = train_year,
    train_month = train_month,
    train_day   = train_day,
    target_name = target_name, # tfrecords column name for the target
    data_path   = data_path,
    batch_size  = 20480,    # training data batch size
    spark = spark,
    # Model parameters
    output_path = model_path_parent,
    model_name  = model_name,
    tabnet_factor = tabnet_factor,
    save_best   = True,
    # Model training hyperparameters
    learning_rate = 0.001,
    class_weights = False,  
    epochs   = 100,
    patience = 10
)
