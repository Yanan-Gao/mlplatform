# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Helper functions for creating charts on model metrics
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Importing required libraries
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create a line plot with vertical lines marking a selection of thresholds
def lineplot_with_thresholds(
    axs, dataset, 
    col_to_plot, color_to_plot, 
    thresholds, th90, thPR, th75, th50, thF1,
    threshold_col = "threshold",
    title_text = ""
):
    # Line plot with x=threshold_col, y=col_to_plot
    axs.plot(dataset[threshold_col].tolist(), dataset[col_to_plot].tolist(), 
             color = color_to_plot, lw = 2, label = col_to_plot)
    axs.set_xlabel("threshold")
  
    # Axes and grid
    axs.set_xlim(0,1)
    axs.set_ylim(0,1)
    axs.grid(axis='y')
  
    # Vertical lines marking the selected thresholds + labels
    axs.axvline(x = thresholds[th90][0], ls = ':', color = 'k')
    axs.axvline(x = thresholds[thPR][0], ls = ':', color = 'k')
    axs.axvline(x = thresholds[th75][0], ls = ':', color = 'k')
    axs.axvline(x = thresholds[th50][0], ls = ':', color = 'k')
    axs.axvline(x = thresholds[thF1][0], ls = ':', color = 'k')
    axs.text(thresholds[th90][0], 0, "$t_{90}$")
    axs.text(thresholds[thPR][0], 0, "$t_{PR}$")
    axs.text(thresholds[th75][0], 0, "$t_{75}$")
    axs.text(thresholds[th50][0], 0, "$t_{50}$")
    axs.text(thresholds[thF1][0], 0, "$t_{F1}$")
  
    axs.set_title(title_text)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create a barplot comparing precision, recall and scale
def barplot_precision_recall_scale(
    axs, dataset, 
    thresholds, th90, thPR, th75, th50, thF1,
    threshold_col = "threshold",
    precision_col = "precision",
    recall_col = "recall",
    scale_col = "scale",
    title_text = ""
):
    # Filtering the rows with the metrics for the selected thresholds
    datasetF1 = dataset[dataset[threshold_col] == thresholds[thF1][0]]
    datasetPR = dataset[dataset[threshold_col] == thresholds[thPR][0]]
    dataset50 = dataset[dataset[threshold_col] == thresholds[th50][0]]
    dataset75 = dataset[dataset[threshold_col] == thresholds[th75][0]]
    dataset90 = dataset[dataset[threshold_col] == thresholds[th90][0]]
  
    # And appending into one dataframe
    df_input = pd.concat([datasetF1, datasetPR, dataset50, dataset75, dataset90])
    df_input["selection_method"] = ["$t_{F1}$", "$t_{PR}$", "$t_{50}$", "$t_{75}$", "$t_{90}$"]
  
    # Side-by-side barchart
    x = np.arange(5)
    width = 0.2
    plt.bar(x-width, df_input[precision_col], width, color="#002F87") # TTD darkblue
    plt.bar(x,       df_input[recall_col],    width, color="#0099FA") # TTD blue
    plt.bar(x+width, df_input[scale_col],     width, color="#F98321") # TTD orange
  
    # Grid
    axs.set_axisbelow(True)
    axs.grid(axis = 'y')
  
    # Axes and legend
    axs.set_ylim(0,1)
    axs.set_xticks(x)
    axs.set_xticklabels(df_input["selection_method"] + "=" + [str(x) for x in df_input["threshold"]])
    axs.legend(['precision', 'recall', 'scale'], loc='upper left')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create a multi-page PDF document, each page with 2x2 plots using both the previous functions
def createPdfWithMetricsForThresholdSelection(
    input_path_metrics,    # input path to the data containing the metrics (precision, recall etc.)
    input_path_thresholds, # input path to the data containing the selected thresholds
    output_pdf_file,       # path to the output pdf file; must start with /dbfs
    spark,                 # required to inherit the spark context
    # Column names in the input datasets:
    category_col = "category",
    label_col = "interestLabel",
    threshold_col = "threshold",
    precision_col = "precision",
    recall_col    = "recall",
    F1_col = "F1_score",
    scale_col = "scale",
    prevalence_col = "prevalence",
    th90 = "threshold_precision_90",
    thPR = "threshold_precision_recall",
    th75 = "threshold_precision_75",
    th50 = "threshold_precision_50",
    thF1 = "threshold_max_f1"
):
    # # #
    # Loading the threshold selection data
    input_thresholds = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(input_path_thresholds)
    input_metrics = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(input_path_metrics)

    # # #
    # Creating the PDF document
    with PdfPages(output_pdf_file) as pdf:
        # Listing all categories
        lst_categories = sorted(input_thresholds.rdd.map(lambda x: x[category_col]).collect())
        n_categories = len(lst_categories)
    
        print("Generating a PDF with precision, recall and scale visualizations for all " + str(n_categories) + " categories.")
        print("Output will be written to:\n" + output_pdf_file + "\n")
    
        i = 0
        for category_id in lst_categories:
      
            # # # # #
            # Defining input data to plot
            dataset    = input_metrics.filter(input_metrics[category_col] == category_id).toPandas()
            thresholds = input_thresholds.filter(input_thresholds[category_col] == category_id).toPandas()
            interest_label = thresholds[label_col][0].replace("Interest > ", "")
    
            # # # # #
            # Graphics parameters for a 15x9-sized 2x2 plot
            fig, axs = plt.subplots(2,2, figsize = (15, 9))
    
            # # # # #
            # Recall and Precision plot
            lineplot_with_thresholds(axs[0,0], dataset, 
                                     col_to_plot = precision_col, color_to_plot = "#002F87",
                                     thresholds = thresholds,
                                     th90=th90, thPR=thPR, th75=th75, th50=th50, thF1=thF1,
                                     title_text = "Precision and Recall" )
            axs[0,0].plot(dataset[threshold_col].tolist(), dataset[recall_col].tolist(), 
                    color = "#0099FA", label = recall_col)
            axs[0,0].legend(loc = "right")

            # # # # #
            # F1 score plot
            lineplot_with_thresholds(axs[0,1], dataset, 
                                     col_to_plot = F1_col, color_to_plot = "#F7D031",
                                     thresholds = thresholds,
                                     th90=th90, thPR=thPR, th75=th75, th50=th50, thF1=thF1,
                                     title_text = "F1 Score")

            # # # # #
            # Scale plot
            lineplot_with_thresholds(axs[1,0], dataset, 
                                     col_to_plot = scale_col, color_to_plot = "#F98321",
                                     thresholds = thresholds,
                                     th90=th90, thPR=thPR, th75=th75, th50=th50, thF1=thF1,
                                     title_text = "Scale")
            prevalence_cat = dataset[prevalence_col][0]
            axs[1,0].axhline(prevalence_cat, ls = "--", color = "k", lw = 1)
            axs[1,0].text(0, prevalence_cat, "prevalence = " + str(round(prevalence_cat,4)))
  
            # # # # #
            # Barplot
            barplot_precision_recall_scale(axs[1,1], dataset,
                                     thresholds = thresholds,
                                     th90=th90, thPR=thPR, th75=th75, th50=th50, thF1=thF1)
            # # # # #
            # Page title
            fig.suptitle(interest_label + " (label_" + str(category_id) + ")")
    
            pdf.savefig()
            plt.close()
      
            # Printing progress to console
            i = i+1
            print("\r" + str(round(100*i/n_categories, 1)) + "% done. ", end = '') 


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create boxplots and return the summary statistics for multiple data series
def createBoxplot(
    data_lst,  # list of data series to be summarized as boxplots
    labels_lst,# labels for these data series
    color_mean   = "#002F87", # TTD blue
    color_median = "#0099FA", # TTD darkblue
    main_title = "",
    plot_hline = True,
    hline_value = 0.5
):
    boxplot_plt = plt.boxplot(data_lst, labels = labels_lst, meanline = True, showmeans = True, 
                              medianprops = dict(color=color_median, linewidth = 1.5), 
                              meanprops = dict(color = color_mean, linewidth=1.5))
    plt.title(main_title)
    if plot_hline:
        plt.axhline(hline_value, color = "black")
    return boxplot_plt


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to grab the stats from a boxplot
def get_box_plot_data(labels, bp):
    rows_list = []

    for i in range(len(labels)):
        dict1 = {}
        dict1['label'] = labels[i]
        dict1['lower_whisker'] = bp['whiskers'][i*2].get_ydata()[1]
        dict1['lower_quartile'] = bp['boxes'][i].get_ydata()[1]
        dict1['mean'] = bp['means'][i].get_ydata()[1]
        dict1['median'] = bp['medians'][i].get_ydata()[1]
        dict1['upper_quartile'] = bp['boxes'][i].get_ydata()[2]
        dict1['upper_whisker'] = bp['whiskers'][(i*2)+1].get_ydata()[1]
        rows_list.append(dict1)

    return pd.DataFrame(rows_list)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function wrapping the previous two in one: creating comparative boxplots and summary stats
def createAUCComparisonsValidationAndTest(
    input_path_validation, # path to the AUC validation data
    input_path_test,       # path to the AUC test data
    output_file_boxplot,   # path to the file with the comparative validation-test-AUCs boxplot
    output_file_summary_statistics, # path to the file with the summary stats from the boxplots
    spark,  # set spark = spark when calling the function to inherit spark context
    auc_column = "AUC"
):
    # Reading the validation and test AUCs as Pandas data frames
    df_AUCs_val = spark.read.option("header","true").option("sep","\t").option("inferSchema","true").csv(input_path_validation).toPandas()
    df_AUCs_test = spark.read.option("header","true").option("sep","\t").option("inferSchema","true").csv(input_path_test).toPandas()

    # Creating boxplots to compare AUCs from the validation and test set - saved to S3
    boxplot_plt = createBoxplot([df_AUCs_val[auc_column], df_AUCs_test[auc_column]], 
                                labels_lst = ["AUCs validation", "AUCs test"],
                                main_title = "AUCs Across All Categories",
                                plot_hline = True, hline_value = 0.5)
    plt.savefig(output_file_boxplot)
    plt.close()

    # Storing the summary statistics for validation and test AUCs
    boxplot_stats = get_box_plot_data(["AUCs validation", "AUCs test"], boxplot_plt)
    boxplot_stats.to_csv(output_file_summary_statistics, 
                         sep="\t", header = True, index = False)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create (AUC) barcharts
def auc_barchart(
    df_AUCs,                         # input data frame with the AUCs and interest labels
    auc_column = "AUC",              # column containing the AUC values
    label_column = "interestLabel",  # column containing the category labels
    drop_string_from_label = "Interest > ", # string to be dropped from the labels in the chart annotations
    low_auc  = 0.7, # default value for a low AUC (bars with values below will be printed in red)
    high_auc = 0.9, # default value for a high AUC (bars with values above will be printed in green)
    main_title = "AUCs" # title
):
    # Height of the bars & x values
    aucs = df_AUCs[auc_column]
    x = range(len(aucs))

    # Mean AUC across all categories
    mean_auc = aucs.mean()

    # x labels
    labels = [re.sub(drop_string_from_label, "", x) for x in df_AUCs[label_column]]
    # Abbreviating a long (top level) label
    labels = [re.sub("Pharmaceuticals, Conditions, and Symptoms", "Pharmac., Conditions etc.", x) for x in labels]

    # Colors of the bars, depending on the value of the AUC
    bar_colors = [{i < low_auc: "#E62325",              # TTD red 3
                   low_auc <= i <= high_auc: "#B2B2B2", # TTD grey 5
                   i > high_auc: "#319E30"              # TTD green 3
                  }[True] for i in aucs]

    # Actual plot
    plt.bar(x, aucs, color = bar_colors, zorder = 2) # bars before the grid but behind the mean
    plt.axhline(mean_auc, color = "#002F87", linestyle = "-") # mean in TTD darkblue
    plt.axhline(high_auc, color = "#319E30", linestyle = "--", lw = 1) # in TTD green 3
    plt.axhline(low_auc, color = "#E62325", linestyle = "--", lw = 1) # in TTD red 3
    plt.axhline(0.5, color = "black", linestyle = "-") # 0.5 in black
    plt.ylim(0,1)
    plt.title(main_title) # title
    plt.grid(axis = 'y', zorder = 0) # grid in the background
    plt.xticks(x, labels, rotation = 90) # rotation = 45, ha = "right")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to create a multi-page PDF document for AUC barcharts
def createPdfWithAUCMetrics(
    input_path_aucs, # input path to the data containing the AUCs to be plotted
    output_pdf_file, # path to the output pdf file; must start with
    spark,           # required to inherit the spark context
    # Column names in the input datasets:
    category_column   = "category",
    label_column      = "interestLabel",
    parentId_column   = "parentId",
    topLevelId_column = "topLevelId",
    depth_column      = "depth",
    auc_column        = "AUC",
    title_prefix      = ""
):
    # # #
    # Loading the AUCs as Pandas data frame
    df_AUCs = spark.read.option("header","true").option("sep","\t").option("inferSchema","true").csv(input_path_aucs).toPandas()

    # Defining the data frame for only the top level categories
    df_AUCs_topLevel = df_AUCs[df_AUCs[depth_column] == 2]
    df_AUCs_lower    = df_AUCs[df_AUCs[depth_column] > 2]

    # # #
    # Creating the PDF document
    with PdfPages(output_pdf_file) as pdf:
        # Listing all top level categories
        lst_top_categories = sorted(df_AUCs_topLevel[category_column].values.tolist())
        n_top_categories   = len(lst_top_categories)
    
        print("Generating a PDF with an AUC barchart for all " + str(n_top_categories) + " top level categories.")

        # Counting all lower level categories
        lst_lower_categories = sorted(df_AUCs_lower[category_column].values.tolist())
        n_lower_categories   = len(lst_lower_categories)
        if n_lower_categories > 0:
            print("Also plotting AUC barcharts for all " + str(n_lower_categories) + " lower level categories - all sublevels for one top level per page.")

        print("Output will be written to:\n" + output_pdf_file + "\n")
    
        # # # # # # # # # #
        # First page: only top level AUCs barchart
        auc_barchart(df_AUCs_topLevel,
                     low_auc = 0.7, high_auc = 0.9,
                     main_title = title_prefix + " AUCs of Top Level Interest Categories")
        pdf.savefig(bbox_inches='tight')
        plt.close()

        # # # # # # # # # #
        if n_lower_categories > 0: # only creating these charts if there is actually data to plot
            for category_id in lst_top_categories:
        
                # # # # #
                # Defining input data to plot
                # i-th interest label = i-th top level interest category
                label_i = str(df_AUCs[df_AUCs[category_column] == category_id][label_column].iloc[0])
                # Top level + all sub-categories for this top level interest category
                df_AUCs_i = df_AUCs[(df_AUCs[topLevelId_column] == category_id)]
                ### Note: If we don't want the top level parent plotted with the subcategories, change this line to:
                ### df_AUCs_i = df_AUCs_lower[(df_AUCs_lower[topLevelId_column] == category_id)]

                # # # # #
                # Actual bar chart
                auc_barchart(df_AUCs_i,
                             drop_string_from_label = label_i + " > ", # dropping the top level interest category from the axis labels
                             low_auc = 0.7, high_auc = 0.9,
                             main_title = title_prefix + " AUCs for '" + label_i + "'")
                pdf.savefig(bbox_inches='tight')
                plt.close()