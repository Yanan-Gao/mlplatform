# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# (Helper) Function to lookup a selection of thresholds for a given category from a model metrics data frame
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Importing required libraries
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# # # # #
# Defining the output schema
schema = StructType([
    StructField("category", IntegerType()),
    StructField("threshold_precision_90", DoubleType()),
    StructField("threshold_precision_recall", DoubleType()),
    StructField("threshold_precision_75", DoubleType()),
    StructField("threshold_precision_50", DoubleType()),
    StructField("threshold_max_f1", DoubleType())
])

# # # # #
@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def getThresholdCandidatesForCategory(dataset:pd.DataFrame
) -> pd.DataFrame:
    # Not pretty with hard coded values here... -> How to change them into parameters for a Pandas UDF?
    category_col   = "category"
    threshold_col  = "threshold"
    precision_col  = "precision" 
    recall_col     = "recall"
    scale_col      = "scale"
    F1_col         = "F1_score"
  
    # Filter the input for only rows (threshold candidates) with positive scale
    # also filters out 0/0=null precision rows
    dataset = dataset[(dataset[scale_col] > 0)]

    # Also, only consider thresholds > 0
    dataset = dataset[(dataset[threshold_col] > 0)]

    # First value of the category column
    category = dataset[category_col].iloc[0] 
  
    # # # # # OPTION 1 # # # # # PRECISION TARGETING 
    # Min. threshold such that the model reaches at least 90% precision
    # if not possible: threshold reaching max. precision
    if dataset.loc[dataset[precision_col] >= 0.9].shape[0] > 0:
        threshold_precision_90 = dataset[dataset[precision_col] >= 0.9][threshold_col].min()
    else:
        threshold_precision_90 = dataset[dataset[precision_col] == (dataset[precision_col]).max()][threshold_col].values[0] 

    # # # # # OPTION 2 # # # # # 
    # Threshold such that precision = recall - or at least minimal distance between them i.e. threshold with min(abs(precision-recall))
    dataset_precision_recall   = dataset[(dataset[recall_col] > 0) & (dataset[precision_col] > 0)] # filtering
    threshold_precision_recall = dataset_precision_recall[(dataset_precision_recall[precision_col]-dataset_precision_recall[recall_col]).abs() == (dataset_precision_recall[precision_col]-dataset_precision_recall[recall_col]).abs().min()][threshold_col].values[0]

    # # # # # OPTION 3 # # # # # MIDDLE GROUND BETWEEN PRECISION AND REACH TARGETING
    # 1) Min. threshold such that the model reaches at least 75% precision if any threshold reaches 75% precision and prevalence is smaller than 0.75
    # 2) else if all precision values are larger than 75% (e.g. because prevalence is larger than 75%), set threshold to reach at most 50% scale
    # 3) else if no threshold reaches 75% precision, set the threshold to reach 75% of max. precision
    precision_min = dataset[precision_col].min()
    precision_max = dataset[precision_col].max()

    if (precision_max > 0.75) and (precision_min < 0.75):
        threshold_precision_75 = dataset[dataset[precision_col] >= 0.75][threshold_col].min()
    else:
        if (precision_min > 0.75) and (dataset[scale_col].min() <= 0.5) :
            threshold_precision_75 = dataset[dataset[scale_col] <= 0.5][threshold_col].min()
        else:
            threshold_precision_75 = dataset[dataset[precision_col] >= 0.75*dataset[precision_col].max()][threshold_col].min()

    # # # # # OPTION 4 # # # # # 
    # Min. threshold such that the model reaches at least 50% precision 
    # if not possible (e.g. because prevalence is larger) set the threshold such that we get max. 80% scale
    if (precision_max > 0.5) and (precision_min < 0.5):
        threshold_precision_50 = dataset[dataset[precision_col] >= 0.5][threshold_col].min()
    else:
        threshold_precision_50 = dataset[dataset[scale_col] <= 0.8][threshold_col].min()

    # # # # # OPTION 5 # # # # # REACH TARGETING
    # Threshold with the maximum F1 score
    threshold_max_f1 = dataset[dataset[F1_col] == (dataset[F1_col]).max()][threshold_col].values[0]
  
    # # # # # OUTPUT # # # # #
    # A one row data frame with all threshold selections for a given category
    return pd.DataFrame([[category] + [threshold_precision_90] + [threshold_precision_recall] + [threshold_precision_75] + [threshold_precision_50] + [threshold_max_f1]])