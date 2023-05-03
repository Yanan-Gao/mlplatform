# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to evaluate the potential threshold selection for all categories from a model metrics data frame
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Importing required libraries
from pythia.interestModel.getThresholdCandidatesForCategory import *

# # # # #
def thresholdSelection(
    input_path_metrics,     # path to the confusion matrices + additional metrics data for all categories
    input_path_taxonomy,    # path to the taxonomy mapping the IAB category IDs to the (cleartext) interest labels
    output_path_thresholds, # path to save the threshold selection for all categories
    spark,
    taxID_col       = "IABAudienceV11_UniqueId",
    taxParentID_col = "IABAudienceV11_ParentId",
    taxDepth_col    = "IABAudienceV11_Depth",
    taxName_col     = "IABAudienceV11_FullPath"
):
    # Reading the confusion matrices and model metrics data
    df_metrics = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(input_path_metrics)

    # Reading the taxonomy data
    df_tax_in = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(input_path_taxonomy)

    # Selecting the thresholds according to the rules stated in the code
    df_threshold_selection = df_metrics.groupBy(["category"]).apply(getThresholdCandidatesForCategory).orderBy("category")

    # Joining taxonomy info with the selected thresholds
    df_tax = df_tax_in.select(taxID_col, taxParentID_col, taxDepth_col, taxName_col)\
        .withColumnRenamed(taxID_col, "category")\
        .withColumnRenamed(taxParentID_col, "parentId")\
        .withColumnRenamed(taxDepth_col, "depth")\
        .withColumnRenamed(taxName_col, "interestLabel")
    df_threshold_selection_with_labels = df_tax.join(df_threshold_selection, ["category"], "right")

    # Saving the selected thresholds
    df_threshold_selection_with_labels.coalesce(1).write.option("header", "true").option("sep", "\t").csv(output_path_thresholds)