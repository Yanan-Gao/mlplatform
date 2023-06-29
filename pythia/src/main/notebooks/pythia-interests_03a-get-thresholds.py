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

# Getting parameters passed on from a previous task (model training)

# Training date (validation is day+1 and test is day+2)
train_year  = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_year", default = 2023)
train_month = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_month")
train_day   = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "train_day")

# Model parent directory and name
model_path_parent = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "model_path_parent", 
    default = "/dbfs/mnt/mlplatform/models/dev/pythia/interestModel")
model_name = dbutils.jobs.taskValues.get(taskKey = "pythia-interests_01-model-training", key = "model_name", 
    default = "interestModel")

# COMMAND ----------

# Setting input/output paths
import re 

# Path to the taxonomy file
path_taxonomy   = "/mnt/mlplatform/data/prod/pythia/interests/taxMap_withTgtIds.txt"

# Model directory
model_dir = f"{model_path_parent}/{train_year}-{train_month:02d}-{train_day:02d}_{model_name}"
model_dir = re.sub("/dbfs", "", model.dir)

# Paths where the predictions can be found
path_predictions_validation = f"{model_dir}/predictions/validation"
path_predictions_test       = f"{model_dir}/predictions/test"

# Paths for the confusion matrices
path_confMat_validation     = f"{model_dir}/metrics/confusionMatrices_validation"
path_confMat_test           = f"{model_dir}/metrics/confusionMatrices_test"

# Path for the thresholds
path_thresholds             = f"{model_dir}/metrics/thresholds"

# File paths for the precision, recall, scale charts
pdf_path_metrics_validation = f"/dbfs{model_dir}/metrics/thresholdSelectionMetrics_validation.pdf"
pdf_path_metrics_test       = f"/dbfs{model_dir}/metrics/thresholdSelectionMetrics_test.pdf"

# COMMAND ----------

# Library for calculating & saving confusion matrices based on model predictions
from pythia.interestModel.getConfusionMatrices import *

# Library for threshold selection
from pythia.interestModel.thresholdSelection import *

# Library for the functions creating charts
from pythia.interestModel.plotFunctions import *

# COMMAND ----------

# Calculating & saving confusion matrices for the validation data
getConfusionMatrices(
  input_path_predictions = path_predictions_validation, 
  output_path = path_confMat_validation,
  sc = sc,
  spark = spark,
  threshold_digits = 4)

# COMMAND ----------

# Evaluating the threshold selection based on the validation data
thresholdSelection(
  input_path_metrics     = path_confMat_validation,
  input_path_taxonomy    = path_taxonomy,
  output_path_thresholds = path_thresholds,
  spark = spark
) 

# COMMAND ----------

# Plotting the metrics and selected thresholds for the validation data
createPdfWithMetricsForThresholdSelection(
  input_path_metrics    = path_confMat_validation,
  input_path_thresholds = path_thresholds,
  output_pdf_file       = pdf_path_metrics_validation,
  spark = spark)

# COMMAND ----------

# Calculating & saving confusion matrices for the test data
getConfusionMatrices(
  input_path_predictions = path_predictions_test, 
  output_path = path_confMat_test,
  sc = sc,
  spark = spark,
  threshold_digits = 4)

# COMMAND ----------

# Plotting the metrics and selected thresholds for the test data
createPdfWithMetricsForThresholdSelection(
  input_path_metrics    = path_confMat_test,
  input_path_thresholds = path_thresholds,
  output_pdf_file       = pdf_path_metrics_test,
  spark = spark)
