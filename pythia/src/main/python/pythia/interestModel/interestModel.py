# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Class for the interest model - with methods to train and predict the model
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Importing required libraries
import sys
import os
import datetime
import re
from pyspark.sql.functions import struct

# Libraries required for the distributed prediction UDF
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
from typing import Iterator

# Libraries for representing and saving model stats
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Model libraries
from pythia.dataInput import *
from pythia.dataSchema import *
from pythia.modelPrep import *

class interestModel():

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # INIT
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    def __init__( self ):
        pass
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # TRAINING METHOD
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    def train(
        self,
        
        # Input data parameters
        train_year, train_month, train_day, # integers
        target_name,   # tfrecords column name for the target

        # Data input
        data_path,     # must have subdirectories in the format year={}/month={}/day={}/hourPart={}
        batch_size,    # 20480 or 16384=2^14 # training data batch size
        spark,         # just need to set spark = spark when calling the function to inherit the session
        
        # Model parameters
        output_path,
        save_best = True,
        model_name = "interestModel",
        tabnet_factor = 0.0,

        # Model training hyperparameters
        learning_rate = 0.001,
        class_weights = False,
        epochs = 100,
        patience = 10
    ):
        # # # # # PREP # # # # # 
        # Formatting training and validation data paths
        date_train      = datetime.date(train_year, train_month, train_day)
        date_validation = date_train + datetime.timedelta(days=1)
  
        train_dir = ["{}/year={}/month={:02d}/day={:02d}".format(data_path, date_train.year, date_train.month, date_train.day)]
        val_dir   = ["{}/year={}/month={:02d}/day={:02d}".format(data_path, date_validation.year, date_validation.month, date_validation.day)]

        # Formatting output directories
        model_path_parent = "{}/{}-{:02d}-{:02d}_{}".format(output_path, date_train.year, date_train.month, date_train.day, model_name)
        try:
            os.makedirs(model_path_parent) # fails if it already exists! So we don't overwrite existing results.
            checkpoint_filepath = "{}/weights/".format(model_path_parent) # checkpoint files (weights per epoch)
            model_output_path = "{}/model".format(model_path_parent)      # final model output
            model_stats_output_path = "{}/metrics/".format(model_path_parent) # model stats (auc and loss over epochs)
            os.makedirs(model_stats_output_path)
        except:
            print("{} already exists.".format(model_path_parent))
            sys.exit(1)
  
        # # # # # DATA INPUT # # # # # 
        print("\nLOADING DATA:\n")
        # Listing the files containing the training & validation data
        train_files = get_tfrecord_files(train_dir)
        val_files   = get_tfrecord_files(val_dir)

        # Determining the number of targets
        df_0 = spark.read.format("tfrecords").load(re.sub("/dbfs", "", train_files[0]))
        df_0_labels = df_0.select(target_name)
        target_length = len(df_0_labels.first()[target_name])

        # Defining the schemas for the model features and targets
        model_features = defineDataSchemaFeatures()
        model_targets  = defineDataSchemaTargetInterests(target_name, target_length)
  
        # Defining the data parser
        tf_parse = feature_parser(model_features, model_targets, target_name, target_length, exp_var=False)
  
        # Parsing the training and validation data
        df_train = tfrecord_dataset(train_files, batch_size, tf_parse.parser(), train=True, buffer_size = 1000000)
        df_val   = tfrecord_dataset(val_files, batch_size*10, tf_parse.parser(), train=False)
        print("Read input data with " + str(target_length) + " targets and " + str(len(model_features)) + " features.")

        # NOTE:
        # After using tfdata read the tfrecords, 
        # cnt = data.reduce(np.int64(0), lambda x, _: x + 1).numpy() 
        # can output the buffer_size value but it is very slow!!
        # However, a large buffer size will reduce the efficiency of training and data loading; 
        # here we use fixed buffer_size = 1000000 to make a tradeoff  
        
        # # # # # MODELING # # # # # 
        print("\nMODEL TRAINING:\n")
        # Initializing the model
        model = init_model_interest(model_features, target_length, dense_layer_dim = 64, tabnet_factor = tabnet_factor)
  
        # Compiling the model
        model.compile(optimizer = keras.optimizers.Nadam(learning_rate=learning_rate),
                      loss = tf.keras.losses.BinaryCrossentropy(from_logits=False, label_smoothing=0.001),
                      metrics = [tf.keras.metrics.AUC(multi_label = True, num_labels = target_length)],
                      run_eagerly = False)
  
        # Training the model & saving model weights each epoch
        history = model.fit(df_train,
                            class_weight = class_weights,
                            epochs = epochs,
                            validation_data = df_val,
                            callbacks = get_callbacks(checkpoint_filepath, patience=patience, 
                                                      save_best=save_best), 
                            verbose = True)
  
        # # # # # OUTPUT # # # # # 
        # Saving the model
        model.save(model_output_path)

        # # # # # ADDITIONAL OUTPUT - AUC # # # # #
        # Making sure that the history dictonary keys are as specified below ('auc', 'val_auc', 'loss', 'val_loss')
        # (because sometimes it can e.g. be 'auc1' instead of 'auc' etc.)
        history_dict0 = history.history
        history_dict0 = {"auc" if k[0:3] == "auc" else k:v for k,v in history_dict0.items()}
        history_dict0 = {"val_auc" if k[0:7] == "val_auc" else k:v for k,v in history_dict0.items()}
        history_dict0 = {"loss" if k[0:4] == "loss" else k:v for k,v in history_dict0.items()}
        history_dict0 = {"val_loss" if k[0:8] == "val_loss" else k:v for k,v in history_dict0.items()}
  
        # Saving the loss/AUC history for the model
        history_dict = {"loss_train": history_dict0['loss'],
                        "loss_val":   history_dict0['val_loss'],
                        "auc_train":  history_dict0['auc'],
                        "auc_val":    history_dict0['val_auc']
                       }
        df_history = pd.DataFrame(history_dict)
        df_history.to_csv(model_stats_output_path + "train-val-aucs-and-loss.tsv", sep="\t", index = False)
  
        # Plotting the AUC history for the model
        plt.plot(history.history['auc'], 'x-', color='#B2B2B2')      # training (TTD grey)
        plt.plot(history.history['val_auc'], 'o-', color ='#002F87') # validation (TTD darkblue)
        plt.legend(['training', 'validation'])
        plt.grid(axis='y')
        plt.title('AUC')
        plt.xlabel('epoch')
        plt.savefig(model_stats_output_path + "train-val-aucs-over-epochs.png")
        plt.close()
  
        # Plotting the Loss history for the model
        plt.plot(history.history['loss'], 'x-', color='#B2B2B2') # training (TTD grey)
        plt.plot(history.history['val_loss'], 'o-', color ='#002F87') # validation (TTD darkblue)
        plt.legend(['training', 'validation'])
        plt.grid(axis='y')
        plt.title('Loss')
        plt.xlabel('epoch')
        plt.savefig(model_stats_output_path + "train-val-loss-over-epochs.png")
        plt.close()
  
        print("\nDONE AND DONE!\nOuptut saved to {}\n".format(model_path_parent))


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # PREDICTION METHOD
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    def predict(
        self,
        # Data Input Parameters
        train_year, train_month, train_day, # model date as integers
        data_path, # must have the data subfolders in the right format (year=YYYY/month=MM/day=DD/hourPart=hh)
        data_type, # has to be one of 'test' or 'validation' and determines the date for the data input path
        target_name, # tfrecords column name for the target; usually "Labels"
        labels_path, # file with a list of all target label names in the same order as appearing in the data
        # Model Parameters
        model_path_parent, # must have the subfolder YYYY-MM-DD_<model_name>/model
        model_name,
        tabnet_factor,
        model_libraries_path, # required to properly define the Pandas UDF for prediction on Databricks
        # something like "/Workspace/Repos/karen.schettlinger@thetradedesk.com/mlplatform/pythia/src/main/python"
        sc,   # required to pass the Spark context
        spark # and the Spark session 
    ):
        # # # # # PREP # # # # # 
        # Formatting input paths
        date_train = datetime.date(train_year, train_month, train_day)
  
        model_path_0 = "{}/{}-{:02d}-{:02d}_{}".format(model_path_parent, date_train.year, date_train.month, date_train.day, model_name)
        model_path = "{}/model".format(model_path_0)

        if labels_path[0:4] != "/mnt":
            print("'labels_path' must start with '/mnt' (NOT /dbfs!) for loading with spark.read.")
            labels_path = re.sub("/dbfs", "", labels_path)
            print("Stripping 'labels_path' of '/dbfs' (if present).")
            if labels_path[0:4] != "/mnt":
                print("'labels_path' must start with '/mnt' for loading with spark.read.")
                sys.exit(1)

        if data_path[0:4] != "/mnt":
            print("'data_path' must start with '/mnt' (NOT /dbfs!) for loading tfrecords.")
            data_path = re.sub("/dbfs", "", data_path)
            print("Stripping 'data_path' of '/dbfs' (if present).")
            if data_path[0:4] != "/mnt":
                print("'data_path' must start with '/mnt' for loading tfrecords.")
                sys.exit(1)
        if not data_type in ["test", "validation"]:
            print("'data_type' must be either 'validation' or 'test'.")
            sys.exit(1)
        else:
            if data_type == "validation":
                data_date = date_train + datetime.timedelta(days=1)
            if data_type == "test":
                data_date = date_train + datetime.timedelta(days=2)
    
        data_dir  = "{}/year={}/month={:02d}/day={:02d}".format(data_path, data_date.year, data_date.month, data_date.day)

        # Formatting output directory
        output_path_predictions = "{}/predictions/{}".format(model_path_0, data_type)
        output_path_predictions = re.sub("/dbfs", "", output_path_predictions)
        if os.path.exists(output_path_predictions):
            print("Output path already exists:\n" + output_path_predictions)
            sys.exit(1)
        else:
            print("Output will be written to:\n" + output_path_predictions)

        # # # # # MODEL INPUT # # # # # 
        # Load model and broadcast weights
        model = tf.keras.models.load_model(model_path, custom_objects=None, compile=True, options=None)
        bc_model_weights = sc.broadcast(model.get_weights())
  
        # # # # # DATA INPUT # # # # # 
        print("\nLOADING DATA...\n")
  
        # Loading the data with the label names
        df_columnsLabels = spark.read.option("header","false").csv(labels_path)
        target_length = df_columnsLabels.count()
  
        if target_length == 0:
            print("Check the file in 'labels_path'. 0 target labels detected.")
            sys.exit(1)
  
        print("Read " + str(target_length) + " label names.")
  
        # Loading the input data
        df_data = spark.read.format("tfrecords").load(data_dir + "/*")

        # Replacing potential missing values in lat/long
        df_data = df_data.fillna(0.0, subset=["Latitude", "Longitude"])

        # Listing all model features
        model_features = defineDataSchemaFeatures()
        feature_names = []
        for f in model_features:
            feature_names.append(f.name)
  
        # Restructuring the input data into two columns Labels + Features (in preparation for the prediction UDF)
        df_data_farray = df_data.select(target_name, struct(*feature_names).alias("Features"))
  
        # Info console output
        data_input_targets = len(df_data_farray.first()[target_name])
        print("Read input data with " + str(data_input_targets) + " targets and " + str(len(feature_names)) + " features.")

        # Sanity check
        if data_input_targets != target_length:
            print("The number of targets in the input data needs to match the number of target label names in the labels data.")
            sys.exit(1)

        # # # # # PREDICTIONS # # # # # 
        print("\nCALCULATING PREDICTIONS AND SAVING THE OUTPUT...\n")

        # Schema and numbered columns for the target (in preparation for the prediction UDF)
        schema = StructType([
            StructField(str(i), DoubleType()) for i in range(target_length)
        ])
        column_name = [str(i) for i in range(target_length)]

        # Distributed prediction UDF
        @pandas_udf(schema)
        def predict_batch_udf(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:    
            sys.path.append(model_libraries_path)
            from pythia.modelPrep import init_model_interest
    
            model = init_model_interest(model_features, target_length, tabnet_factor = tabnet_factor)
            model.set_weights(bc_model_weights.value)
            for data_batch in batch_iter:
                pred = model.predict(dict(data_batch))
                yield pd.DataFrame(pred, columns=column_name)
        # Note: I know it's not pretty having the UDF definition inside this method, but I didn't figure our another way that worked.
        # Also, in Databricks you have to import custom modules inside the UDF, even if they are already imported outside.
  
        # Applying the model to calculate predictions in a distributed fashion
        df_predictions_2 = df_data_farray.select(target_name, predict_batch_udf("Features").alias("prediction"))
  
        # # # # # OUTPUT # # # # # 
        # Column names for Labels and Predictions
        columnsLabels = np.array(df_columnsLabels.select("_c0").collect()).reshape(-1)
        columnsPred   = [re.sub("label", "pred", x) for x in columnsLabels]

        # Creating a data frame with a column for each label and each prediction
        df_predictions = df_predictions_2.select([df_predictions_2.Labels[i].alias(columnsLabels[i]) for i in range(len(columnsLabels))] + [df_predictions_2.prediction[str(j)].alias(columnsPred[j]) for j in range(len(columnsPred))])

        # Saving the data frame with the labels and predictions to S3
        df_predictions.write.option("header", "true").option("sep", "\t").parquet(output_path_predictions)
        print("DONE AND DONE! Output was written to:\n" + output_path_predictions)  