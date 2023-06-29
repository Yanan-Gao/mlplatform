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
model_dir = re.sub("/dbfs", "", model_dir)

# Paths where the predictions can be found
path_predictions_validation = f"{model_dir}/predictions/validation"
path_predictions_test       = f"{model_dir}/predictions/test"

# Directory for all metrics
path_metrics   = f"{model_dir}/metrics"

# Paths to the AUC values
path_aucs_validation  = f"{path_metrics}/AUCs_validation"
path_aucs_test        = f"{path_metrics}/AUCs_test"

# Paths to the PDF files with the barcharts for the AUCs
output_pdf_file_validation  = f"/dbfs{path_metrics}/AUCs_validation.pdf"
output_pdf_file_test        = f"/dbfs{path_metrics}/AUCs_test.pdf"

# Paths to comparative boxplots and summary statistics
output_file_boxplot  = f"/dbfs{path_metrics}/AUC-boxplots_validation-and-test.png"
output_file_summary  = f"/dbfs{path_metrics}/AUC-summary-stats_validation-and-test.tsv"

# COMMAND ----------

# Functions to calculate the AUC by category
from pythia.interestModel.helperFunctionsAUC import *

# Functions to create AUC charts
from pythia.interestModel.plotFunctions import *

# COMMAND ----------

# Calculating and storing the AUCs for the validation data
getAUCsFromPredictions(
    input_path_predictions = path_predictions_validation,
    input_path_taxonomy    = path_taxonomy,
    output_path            = path_aucs_validation,
    spark = spark,
    label_column_prefix      = "label",
    prediction_column_prefix = "pred",
    taxID_col       = "IABAudienceV11_UniqueId", # will be renamed to 'category'
    taxParentID_col = "IABAudienceV11_ParentId", # will be renamed to 'parentId'
    topLevelID_col  = "IABAudienceV11_TopLevelId", # will be renamed to 'topLevelId'
    taxDepth_col    = "IABAudienceV11_Depth",    # will be renamed to 'depth'
    taxName_col     = "IABAudienceV11_FullPath"  # will be renamed to 'interestLabel'
)

# COMMAND ----------

# Creating and storing barcharts of the AUC values for the validation data
createPdfWithAUCMetrics(
    input_path_aucs = path_aucs_validation,
    output_pdf_file = output_pdf_file_validation,
    spark = spark,
    title_prefix = "Validation "
)

# COMMAND ----------

# Calculating and storing the AUCs for the test data
getAUCsFromPredictions(
    input_path_predictions = path_predictions_test,
    input_path_taxonomy    = path_taxonomy,
    output_path            = path_aucs_test,
    spark = spark,
    label_column_prefix      = "label",
    prediction_column_prefix = "pred"
)

# COMMAND ----------

# Creating and storing barcharts of the AUC values for the test data
createPdfWithAUCMetrics(
    input_path_aucs = path_aucs_test,
    output_pdf_file = output_pdf_file_test,
    spark = spark,
    title_prefix = "Test "
)

# COMMAND ----------

# Creating and storing boxplots and summary statistics for validation and test AUCs
createAUCComparisonsValidationAndTest(
    input_path_validation = path_aucs_validation,
    input_path_test       = path_aucs_test,
    output_file_boxplot   = output_file_boxplot,
    output_file_summary_statistics = output_file_summary,
    spark = spark
)
