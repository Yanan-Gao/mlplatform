import numpy as np
import pandas as pd
import re
from pyspark.sql import Window
from pyspark.sql.functions import *

# # # # # # # # # # 
# Function to count the number of Zeros and Ones in the columns of a data frame with only Zeros and Ones
# Sum must be = Count
def counts_by_column(
    dataset: DataFrame, 
    spark, 
    label_prefix = "label_", 
    output_column_0 = "trueZeroCount", 
    output_column_1 = "trueOneCount"
) -> DataFrame:
    from pyspark import SQLContext
    # Total row count
    N = dataset.count()
  
    # Counts (=sum) of the Ones by column
    df_oneCounts = dataset.select([sum(x).alias(str(x)) for x in dataset.columns]) # as data frame
    d = df_oneCounts.collect() # as a row
    lst_oneCounts = [d[0][i] for i in range(len(d[0]))] # as a list of all the one-counts
  
    # Label categories as integers
    lst_categories = [int(re.sub(label_prefix, "", x)) for x in dataset.columns] 
  
    # Consolidate to a dataframe
    output_1s = spark.createDataFrame( pd.DataFrame(np.array([lst_categories, lst_oneCounts]).T, columns = ["category", output_column_1]) )
    output = output_1s.withColumn(output_column_0, lit(N) - output_1s[output_column_1]) # Zeros = N - Ones
  
    return output

# # # # # # # # # # 
# Function to convert the wide data frame with labels and predictions into a long one with just the three columns
# category, label and prediction (pred)
def wide_to_long(dataset, label_cols, pred_cols, label_prefix = "label", prediction_prefix = "pred"):
    from functools import reduce
    all_df = []
    for l, p in zip(label_cols, pred_cols):
        df_sub = dataset.select(lit(int(re.sub(label_prefix + "_", "", l))).alias("category"), 
                                col(l).alias(label_prefix), 
                                col(p).alias(prediction_prefix))
        all_df.append(df_sub)
    
    df_out = reduce(DataFrame.unionAll, all_df)
    return df_out

# # # # # # # # # # 
# Function to calculate a cumulative sum over cusum_var, ordered by order_by_var, partitioned by partition_by_lst 
def cumsum_by_partition(dataset:DataFrame, partition_by_lst, order_by_var, cumsum_var)-> DataFrame:
  
    #Calculate the cumsum cnt
    windowval = ( Window.partitionBy(partition_by_lst)
                  .orderBy(partition_by_lst + [order_by_var])
                  .rangeBetween(Window.unboundedPreceding, 0) )

    output = dataset.withColumn("cumsum", sum(cumsum_var).over(windowval))
  
    return output

# # # # # # # # # # 
# Function to create a data frame that has all the thresholds for all the label categories (as int)
def create_threshold_df(label_cols, threshold_digits, spark, label_prefix = "label_"):
    from functools import reduce
  
    # All threshold values:
    thresholds = np.arange(0, 1, 10**(-threshold_digits)) # some values are a bit off precision, so:
    thresholds = np.array([x.round(threshold_digits) for x in thresholds]) # rounding again to the threshold digits
    pd_thresholds = pd.DataFrame(thresholds, columns = ["threshold"])

    all_df = []
    for l in label_cols:
        category = int(re.sub(label_prefix, "", l))
        df_thr_l = spark.createDataFrame(pd_thresholds).withColumn("category", lit(category)).select("category", "threshold") 
        all_df.append(df_thr_l)
    
    df_out = reduce(DataFrame.unionAll, all_df)
    return df_out

# # # # # # # # # # 
# Function to add more metrics to an existing confusion matrix data frame
def add_metrics_to_confusion_matrix(dataset:DataFrame, TP_col, FP_col, FN_col, TN_col, Total_col, true1_col, pred1_col)-> DataFrame:
    df_out = dataset\
        .withColumn('prevalence', dataset[true1_col] / dataset[Total_col]) \
        .withColumn('recall',     dataset[TP_col] / (dataset[TP_col] + dataset[FN_col])) \
        .withColumn('precision',  dataset[TP_col] / (dataset[TP_col] + dataset[FP_col])) \
        .withColumn('accuracy',  (dataset[TP_col] + dataset[TN_col]) / dataset[Total_col]) \
        .withColumn('F1_score',   lit(2)*dataset[TP_col] / ((lit(2)*dataset[TP_col]) + dataset[FP_col] + dataset[FN_col]))\
        .withColumn('scale',      dataset[pred1_col] / dataset[Total_col])
  
    return df_out