# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to calculate confusion matrices for all thresholds between 0 and 1 based on model predictions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Importing required libraries
from pythia.interestModel.helperFunctions import *

# # # # #
def getConfusionMatrices(
    input_path_predictions,
    output_path,
    sc,
    spark,
    threshold_digits = 4,
    label_column_prefix = "label",
    prediction_column_prefix = "pred"
):
    # # # # # DATA INPUT # # # # # 
    print("Output will be written to " + output_path)
    print("\nLOADING DATA...\n")
    # Loading the input data
    df_predictions = spark.read.option("header", "true").option("sep", "\t").parquet(input_path_predictions)

    # Defining column names for the labels and the predictions
    columnsLabels = [x for x in df_predictions.columns if label_column_prefix in x]
    columnsPred   = [x for x in df_predictions.columns if prediction_column_prefix in x]

    # # # # # DATA INPUT # # # # # 
    print("\nCALCULATING THE CONFUSION MATRICES...\n")
    # Counting all true zeros and ones
    true1_col = "trueOneCount"
    true0_col = "trueZeroCount"
    df_true = counts_by_column( df_predictions.select(columnsLabels), spark = spark,
                                output_column_0 = true0_col, output_column_1 = true1_col)

    # Rounding down all prediction values to the next lower threshold with threshold_digits significant digits (= 'min threshold' values)
    df_predictions_floor = df_predictions.select( columnsLabels + [( floor(col(x)*lit(10**threshold_digits))/lit(10**threshold_digits) ).alias(x) for x in columnsPred] )

    # Creating a long table with just the three columns category, label and (rounded) prediction
    df_predictions_floor_long = wide_to_long(df_predictions_floor, columnsLabels, columnsPred, 
                                             label_prefix = label_column_prefix, 
                                             prediction_prefix = prediction_column_prefix)

    # Counting the number of rounded down predictions (by category + label)
    df_predictions_floor_counts = df_predictions_floor_long.groupBy("category", label_column_prefix, prediction_column_prefix).count()

    # Creating a dataframe with all possible thresholds for all possible categories
    df_thr = create_threshold_df(columnsLabels, threshold_digits, spark=spark).cache()

    # Adding 0/1 labels
    df_threshold_category_label = df_thr.withColumn(label_column_prefix, lit(0)).union(df_thr.withColumn(label_column_prefix, lit(1)))

    # Joining the 'all possible thresholds' data frame with the actual counts
    # Setting missing counts to zero, so all thresholds have a value for all categories
    df_floor_counts_all = df_threshold_category_label.join(df_predictions_floor_counts.withColumnRenamed(prediction_column_prefix, "threshold"), ["category", "threshold", label_column_prefix], "left").fillna(0)

    # Building the cumulative sum because (N-cumsum+count) is the number of positive predictions
    # at the threshold value
    df_cumsum = cumsum_by_partition(df_floor_counts_all, ["category", label_column_prefix], order_by_var = "threshold", cumsum_var = "count")

    # Pivoting the data to have the predicted counts joined next to each other 
    # And also joining in the true positive and true negative counts
    df_cumsum_0 = df_cumsum.filter(df_cumsum.label == 0).drop(label_column_prefix).withColumnRenamed("count", "count_0").withColumnRenamed("cumsum", "cumsum_0")
    df_cumsum_1 = df_cumsum.filter(df_cumsum.label == 1).drop(label_column_prefix).withColumnRenamed("count", "count_1").withColumnRenamed("cumsum", "cumsum_1")
    df_cumsum_wide = df_cumsum_0.join(df_cumsum_1, ["category", "threshold"]).join(df_true, ["category"], "left")

    # # #
    # Calculating the confusion matrix entries
    TP_col = "TP_label1pred1"
    FP_col = "FP_label0pred1"
    FN_col = "FN_label1pred0"
    TN_col = "TN_label0pred0"
    Total_col = "Total"
    pred1_col = "predictedOneCount"
    pred0_col = "predictedZeroCount"
    #true1_col = "trueOneCount" and true0_col = "trueZeroCount" are already defined above

    df_pred1_counts = df_cumsum_wide\
        .withColumn(Total_col, df_cumsum_wide[true0_col] + df_cumsum_wide[true1_col])\
        .withColumn(FP_col, df_cumsum_wide[true0_col] - df_cumsum_wide["cumsum_0"] + df_cumsum_wide["count_0"])\
        .withColumn(TP_col, df_cumsum_wide[true1_col]  - df_cumsum_wide["cumsum_1"] + df_cumsum_wide["count_1"])

    df_pred0_counts = df_pred1_counts\
        .withColumn(TN_col, df_pred1_counts[true0_col] - df_pred1_counts[FP_col])\
        .withColumn(FN_col, df_pred1_counts[true1_col] - df_pred1_counts[TP_col])

    df_confusion_matrix = df_pred0_counts\
        .withColumn(pred1_col, df_pred0_counts[TP_col] + df_pred0_counts[FP_col])\
        .withColumn(pred0_col, df_pred0_counts[TN_col] + df_pred0_counts[FN_col])\
        .select(["category", "threshold", Total_col, 
                 true1_col, true0_col, pred1_col, pred0_col, 
                 TP_col, FP_col, FN_col, TN_col])

    # Adding further metrics to the confusion matrices: precision, recall, accuracy, F1 score, scale
    df_confusion_matrix_with_metrics = add_metrics_to_confusion_matrix(df_confusion_matrix, TP_col, FP_col, FN_col, TN_col, 
                                                                       Total_col, true1_col, pred1_col)\
        .orderBy("category", "threshold")

    # # # # # OUTPUT # # # # # 
    # Saving the data frame with the labels and predictions to S3
    df_confusion_matrix_with_metrics.coalesce(1).write.option("header", "true").option("sep", "\t").csv(output_path)
    print("DONE AND DONE! Output was written to:\n" + output_path)