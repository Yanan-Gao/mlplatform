import pandas as pd
import re
from pyspark.sql.functions import pandas_udf, lit, mean as _mean, sum as _sum
from sklearn.metrics import roc_auc_score

# # # # # # # # # # 
# Helper functions to calculate the AUC for all categories
@pandas_udf("double")
def auc_udf(l: pd.Series, p: pd.Series) -> float:
    return roc_auc_score(l, p)

def auc_dataframe(data_input, label_col_lst, prediction_col_lst, category_col_name = "category", label_column_prefix = "label"):
    all_auc_lst = []
    for l, p in zip(label_col_lst, prediction_col_lst):
        auc = data_input.select(lit(int( re.sub(label_column_prefix + "_", "", l) )).alias(category_col_name), \
                                auc_udf(data_input[l], data_input[p]).alias("AUC"))

        all_auc_lst.append(auc)
    return all_auc_lst

# # # # # # # # # # 
# Function to actually calculate the AUCs for all categories and output them in a dataframe
def getAUCByLabel(data_input, label_columns, prediction_columns, 
                  category_col_name = "category", label_column_prefix = "label"):
    from functools import reduce
    from pyspark.sql.functions import DataFrame
    all_auc= auc_dataframe(data_input, label_columns, prediction_columns, 
                           category_col_name = category_col_name, label_column_prefix = label_column_prefix)
    df_auc = reduce(DataFrame.unionAll, all_auc)
    return df_auc

# # # # # # # # # # 
# Function reading a set of predictions and outputting the AUCs data frame
def getAUCsFromPredictions(
    input_path_predictions, # path to a predictions data set with label columns and prediction columns
    input_path_taxonomy,    # path to the taxonomy mapping the categories to the cleartext names
    output_path,            # output path where to write the AUC metrics
    spark, # need to set spark = spark when calling the function to inherit the spark context
    # Default column names
    label_column_prefix      = "label",
    prediction_column_prefix = "pred",
    taxID_col       = "IABAudienceV11_UniqueId",
    taxParentID_col = "IABAudienceV11_ParentId",
    topLevelID_col  = "IABAudienceV11_TopLevelId",
    taxDepth_col    = "IABAudienceV11_Depth",
    taxName_col     = "IABAudienceV11_FullPath"
):
    # # # # # DATA INPUT # # # # # 
    print("\nLOADING DATA...\n")
    # Loading the input predictions data
    df_predictions = spark.read.option("header", "true").option("sep", "\t").parquet(input_path_predictions)
  
    # Defining column names for the labels and the predictions
    columnsLabels = [x for x in df_predictions.columns if label_column_prefix in x]
    columnsPred   = [x for x in df_predictions.columns if prediction_column_prefix in x]

    # Reading the taxonomy data
    df_tax_in = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(input_path_taxonomy)

    # Selecting relevant columns from the taxonomy
    df_tax = df_tax_in.select(taxID_col, taxParentID_col, topLevelID_col, taxDepth_col, taxName_col)\
        .withColumnRenamed(taxID_col, "category")\
        .withColumnRenamed(taxParentID_col, "parentId")\
        .withColumnRenamed(topLevelID_col, "topLevelId")\
        .withColumnRenamed(taxDepth_col, "depth")\
        .withColumnRenamed(taxName_col, "interestLabel")
  
    # # # # # DATA INPUT # # # # # 
    print("CALCULATING AUC BY CATEGORY...\n")
    # Calculating the AUCs by category
    df_AUCs = getAUCByLabel(df_predictions, columnsLabels, columnsPred, 
                            label_column_prefix = label_column_prefix)
  
    # Joining taxonomy and AUC data frame
    df_AUCs_with_tax = df_tax.join(df_AUCs, ["category"], "right")
  
    # Writing the output to a tsv file
    df_AUCs_with_tax.coalesce(1).write.option("sep", "\t").option("header", "true").csv(output_path)
    print("\nDONE AND DONE!\nOuptut saved to {}\n".format(output_path))