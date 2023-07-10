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

# Getting parameters passed on from the previous task (model training)

# Training date (validation is day+1 and test is day+2)
train_year  = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_year", default = 2023)
train_month = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_month")
train_day   = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_day")

# Parent path to the test and validation data
data_path = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "data_path", 
    default = "/dbfs/mnt/mlplatform/features/data/pythia/interests/v=1/prod/interestModelInput")

# Label column name
target_name = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "target_name", 
    default = "Labels")

# Model parameters
model_path_parent = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "model_path_parent", 
    default = "/dbfs/mnt/mlplatform/models/dev/pythia/interestModel")
model_name = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "model_name", 
    default = "interestModel")
tabnet_factor = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "tabnet_factor", 
    default = 0.0)

# COMMAND ----------

# Path to the set of matching interest labels
import re
labels_path = f"{data_path}/../interestLabels/{train_year}-{train_month:02d}-{train_day:02d}_labels"
labels_path = re.sub("/dbfs", "", labels_path)

# COMMAND ----------

# Printing the training date in the notebook
f"{train_year}-{train_month:02d}-{train_day:02d}"

# COMMAND ----------

labels_path

# COMMAND ----------

# Importing the interestModel class
from pythia.interestModel.interestModel import *

# COMMAND ----------

# Initializing the model
interest_model = interestModel() 

# COMMAND ----------

# Predictions for the validation data
interest_model.predict(
    # Data Input Parameters
    train_year  = train_year, train_month = train_month, train_day   = train_day,
    data_path   = data_path,
    data_type   = "validation",
    labels_path = labels_path, 
    target_name = target_name,
    # Model Parameters
    model_path_parent = model_path_parent, 
    model_name = model_name, 
    tabnet_factor = tabnet_factor,
    # Environment Parameters
    model_libraries_path = model_libraries_path, 
    sc = sc, spark = spark
)

# COMMAND ----------

# Predictions for the test data
interest_model.predict(
    # Data Input Parameters
    train_year  = train_year, train_month = train_month, train_day   = train_day,
    data_path   = data_path,
    data_type   = "test",
    labels_path = labels_path,   
    target_name = target_name,
    # Model Parameters
    model_path_parent = model_path_parent, 
    model_name = model_name, 
    tabnet_factor = tabnet_factor,
    # Environment Parameters
    model_libraries_path = model_libraries_path, 
    sc = sc, spark = spark
)
