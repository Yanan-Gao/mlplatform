from datetime import datetime
import gc
import tensorflow as tf
from id_to_model_trainer import models, features, data, utils
from pyspark.sql import SparkSession
import mlflow
from sklearn import metrics
import pandas as pd
import seaborn as sns
import numpy as np
import os
import random
import matplotlib.pyplot as plt
from id_to_model_trainer.lion_optimizer import Lion

S3_MODELS = "s3://thetradedesk-mlplatform-us-east-1/models"
ENV = "prod"
INPUT_PATH = "./input/"
OUTPUT_PATH = "./output/"
TOPIC = "ae_date0812_large"
LOGS_PATH = "./logs/"
models.GPU_setting()


class AudienceModelExperiment:
    def __init__(self, **kwargs) -> None:
        # model env config
        self.input_paths = INPUT_PATH
        self.graph_input_paths = None
        self.test_input_paths = INPUT_PATH
        self.test_graph_input_paths = None
        self.output_path = OUTPUT_PATH
        self.s3_models = S3_MODELS
        self.env = ENV
        self.topic = TOPIC
        self.model_weight_path = None
        self.model_upload_weights = False
        self.model_creation_date = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.log_path = LOGS_PATH
        self.log_tag = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
        self.profile_batches = [100, 200]
        self.subfolder = "split"
        self.graph = False
        self.train_included_holdout = False
        self.train_included_val = False
        self.test_as_val = False

        # model params initialize
        self.neo_embedding_size = 64
        self.batch_size = 10240
        self.num_epochs = 12
        self.buffer_size = 1000000
        # Very important information: for the protection mechanism from cuda
        # for matmul operation, the limitation size is size(matrix) <= 172800 for cuda9, for cuda10, the size is larger
        # for [N,a,b], the size is N; for [N,M,a,b], the size is N*M
        # so for the model which can handle multiple pixels, the setting of batch_size and eval_batch_size are important
        # otherwise, Blas xGEMMBatched launch failed will pop up
        self.eval_batch_size = 10240
        self.num_decision_steps = 5
        self.feature_dim_factor = 1.3
        self.learning_rate = 0.001  # for lion-> suggest 3e-4
        self.beta_1 = 0.9
        self.beta_2 = 0.99
        self.wd = 0.01  # weight decay or lambda
        self.relaxation_factor = 1.5
        self.optimizer = "lion"
        self.label_smoothing = 0.001
        self.tabnet_factor = 0.15
        self.embedding_factor = 1.0
        self.dropout_rate = 0.2
        self.class_weight = {0: 1.0, 1: 1.0}
        self.seed = 13
        self.fgm = False
        self.fgm_layer = None
        self.fgm_epsilon = 0.08
        self.sum_residual_dropout = False
        self.sum_residual_dropout_rate = 0.4
        self.ignore_index = None
        self.swa = False
        self.swa_start_averaging = 133 * 3
        self.swa_average_period = 15
        self.loss = "binary"
        self.poly_epsilon = 1
        self.mixed_training = False
        # if true, then use seed to control randomness
        self.dev = True

        # callback
        self.save_best = True
        self.early_stopping_patience = 5

    def update_metadata(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def set_seed(self):
        random.seed(self.seed)
        np.random.seed(self.seed)
        tf.random.set_seed(self.seed)
        tf.experimental.numpy.random.seed(self.seed)
        tf.keras.utils.set_random_seed(self.seed)
        # When running on the CuDNN backend, two further options must be set
        os.environ["TF_CUDNN_DETERMINISTIC"] = "1"
        os.environ["TF_DETERMINISTIC_OPS"] = "1"
        # Set a fixed value for the hash seed
        os.environ["PYTHONHASHSEED"] = str(self.seed)
        print(f"Random seed set as {self.seed}")

    def get_data_files(self):
        train_wo_graph_files = data.get_tfrecord_files(
            [
                input_path + f"{self.subfolder}=train_tfrecord/"
                for input_path in self.input_paths
            ]
        )
        val_wo_graph_files = data.get_tfrecord_files(
            [
                input_path + f"{self.subfolder}=val_tfrecord/"
                for input_path in self.input_paths
            ]
        )

        holdout_wo_graph_files = data.get_tfrecord_files(
            [
                input_path + f"{self.subfolder}=holdout_tfrecord/"
                for input_path in self.input_paths
            ]
        )

        test_wo_graph_files = data.get_tfrecord_files(
            [
                test_input_path + f"{self.subfolder}=val_tfrecord/"
                for test_input_path in self.test_input_paths
            ]
        )

        train_wi_graph_files = data.get_tfrecord_files(
            [
                graph_input_path + f"{self.subfolder}=train_tfrecord/"
                for graph_input_path in self.graph_input_paths
            ]
        )
        val_wi_graph_files = data.get_tfrecord_files(
            [
                graph_input_path + f"{self.subfolder}=val_tfrecord/"
                for graph_input_path in self.graph_input_paths
            ]
        )

        holdout_wi_graph_files = data.get_tfrecord_files(
            [
                graph_input_path + f"{self.subfolder}=holdout_tfrecord/"
                for graph_input_path in self.graph_input_paths
            ]
        )

        test_wi_graph_files = data.get_tfrecord_files(
            [
                test_graph_input_path + f"{self.subfolder}=val_tfrecord/"
                for test_graph_input_path in self.test_graph_input_paths
            ]
        )

        # add holdout files to train files
        if self.train_included_holdout:
            final_train_wi_graph_files = train_wi_graph_files + holdout_wi_graph_files
            final_train_wo_graph_files = train_wo_graph_files + holdout_wo_graph_files

        # add val files to train files
        if self.train_included_val:
            final_train_wi_graph_files = train_wi_graph_files + val_wi_graph_files
            final_train_wo_graph_files = train_wo_graph_files + val_wo_graph_files

        # add val and holdout files to train files
        if self.train_included_val and self.train_included_holdout:
            final_train_wi_graph_files = (
                train_wi_graph_files + val_wi_graph_files + holdout_wi_graph_files
            )
            final_train_wo_graph_files = (
                train_wo_graph_files + val_wo_graph_files + holdout_wo_graph_files
            )

        else:
            final_train_wi_graph_files = train_wi_graph_files
            final_train_wo_graph_files = train_wo_graph_files
        # add graph data
        if self.graph:
            train_files = final_train_wi_graph_files + final_train_wo_graph_files
            val_files = val_wi_graph_files + val_wo_graph_files
            test_files = test_wi_graph_files + test_wo_graph_files

        else:
            train_files = final_train_wo_graph_files
            val_files = val_wo_graph_files
            test_files = test_wi_graph_files + test_wo_graph_files

        return train_files, val_files, test_files

    def get_data(self):
        train_files, val_files, test_files = self.get_data_files()

        tf_parser = data.feature_parser(
            features.model_features + features.model_dim_group,
            features.model_targets,
            features.TargetingDataIdList,
            features.Target,
            #             features.graph_tag,
            exp_var=True,
        )

        # use test data as validation
        if self.test_as_val:
            val_files = test_files

        train = data.tfrecord_dataset(
            train_files,
            self.batch_size,
            tf_parser.parser(),
            train=True,
            buffer_size=self.buffer_size,
            seed=self.seed,
        )
        val = data.tfrecord_dataset(
            val_files,
            self.eval_batch_size,
            tf_parser.parser(),
            train=False,
            seed=self.seed,
        )

        test = data.tfrecord_dataset(
            test_files,
            self.eval_batch_size,
            tf_parser.parser(),
            train=False,
            seed=self.seed,
        )

        return train, val, test

    def get_optimizer(self, learning_rate):
        if self.optimizer == "nadam":
            return tf.keras.optimizers.Nadam(learning_rate=learning_rate)
        elif self.optimizer == "adam":
            return tf.keras.optimizers.Adam(learning_rate=learning_rate)
        elif self.optimizer == "lion":
            return Lion(
                learning_rate=learning_rate,
                beta_1=self.beta_1,
                beta_2=self.beta_2,
                wd=self.wd,
            )
        else:
            raise Exception("Optimizer not supported.")

    def get_loss(self):
        if self.loss == "binary":
            return tf.keras.losses.BinaryCrossentropy(
                from_logits=False, label_smoothing=self.label_smoothing
            )
        elif self.loss == "poly":
            return models.poly1_cross_entropy(self.label_smoothing, self.poly_epsilon)
        else:
            raise Exception("Loss not supported.")

    def get_callbacks(self):
        tb_callback = tf.keras.callbacks.TensorBoard(
            log_dir=f"{self.log_path}{self.log_tag}",
            write_graph=True,
            profile_batch=self.profile_batches,
        )

        es_cb = tf.keras.callbacks.EarlyStopping(
            patience=self.early_stopping_patience, restore_best_weights=True
        )

        # checkpoint_base_path = f"{self.output_path}checkpoints/"
        # checkpoint_filepath = (
        #     checkpoint_base_path + "weights.{epoch:02d}-{val_loss:.2f}"
        # )
        # chkp_cb = tf.keras.callbacks.ModelCheckpoint(
        #     filepath=checkpoint_filepath,
        #     save_weights_only=True,
        #     monitor="val_loss",
        #     mode="min",
        #     save_best_only=self.save_best,
        #     save_freq="epoch",
        # )

        return [
            tb_callback,
            es_cb,
        ]  # chkp_cb

    def init_model(self):
        model = models.init_model(
            features.model_features,
            features.model_dim_group,
            self.neo_embedding_size,
            self.feature_dim_factor,
            self.num_decision_steps,
            self.relaxation_factor,
            self.tabnet_factor,
            self.embedding_factor,
            self.dropout_rate,
            self.seed,
            self.sum_residual_dropout,
            self.sum_residual_dropout_rate,
            self.ignore_index,
        )

        return model

    def generate_model(self):
        model = self.init_model()
        model.summary()
        if self.swa:
            # general suggestions: start_averaging=n*(number steps in each epoch) and average_period=0.1 or 0.2*(number steps in each epoch)
            # self.op = tfa.optimizers.SWA(
            #     self.get_optimizer(self.learning_rate),
            #     start_averaging=self.swa_start_averaging,
            #     average_period=self.swa_average_period,
            # )
            pass
        else:
            self.op = self.get_optimizer(self.learning_rate)
        model.compile(
            optimizer=self.op,
            loss=self.get_loss(),
            metrics=[
                tf.keras.metrics.AUC(),
                tf.keras.metrics.Recall(),
                tf.keras.metrics.Precision(),
            ],
        )

        if self.model_weight_path is not None:
            model.load_weights(self.model_weight_path)

        return model

    def get_AUC_dist(self, val, model):
        def AUC(df):
            y = df["target"]
            pred = df["pred"]
            fpr, tpr, thresholds = metrics.roc_curve(y, pred, pos_label=1)
            return metrics.auc(fpr, tpr)

        dfList = []
        for input, target in val:
            pred = model.predict_on_batch(input)
            input["TargetingDataId"] = tf.reshape(input["TargetingDataId"], -1, 1)
            df = pd.DataFrame(input)
            df["pred"] = pred
            df["target"] = target
            # pass flake8 check, get existing spark session
            spark = SparkSession.builder.getOrCreate()
            df_spark = spark.createDataFrame(df)
            dfList.append(df_spark)

        df_final = utils.unionAllDF(*dfList)

        df_agg = (
            df_final.groupby("TargetingDataId")
            .agg(
                utils.auc_udf(df_final["target"], df_final["pred"]).alias("AUC"),
                utils.count_udf(df_final["target"]).alias("count"),
            )
            .toPandas()
        )

        df_agg["pixel_rk"] = df_agg["count"].rank(pct=True)
        df_agg["pixel_size"] = df_agg["pixel_rk"].apply(
            lambda x: "small" if x <= 0.33 else ("mid" if x <= 0.67 else "large")
        )

        return df_agg

    def log_auc_fig(self, dataset, model, prefix):
        # get AUC for each targetingDataId for val data
        df_agg = self.get_AUC_dist(dataset, model)

        plt.figure()
        auc_by_pixel = sns.histplot(df_agg.AUC)
        auc_fig = auc_by_pixel.get_figure()
        mlflow.log_figure(auc_fig, f"{prefix}_auc_pixel_hist.png")
        plt.clf()

        plt.figure()
        auc_by_size = sns.displot(data=df_agg, x="AUC", col="pixel_size")
        auc_fig_by_size = auc_by_size.fig
        mlflow.log_figure(auc_fig_by_size, f"{prefix}_auc_by_pixel_size_hist.png")
        plt.clf()

        df_agg.to_csv(f"{self.output_path}auc/{prefix}_auc.csv")
        mlflow.log_artifact(f"{self.output_path}auc/{prefix}_auc.csv")

    def run_experiment(self, data, features, **kwargs):
        mlflow.tensorflow.autolog()
        self.update_metadata(
            model_creation_date=datetime.now().strftime("%Y%m%d-%H%M%S")
        )
        with mlflow.start_run(run_name=f"{self.topic}_{self.model_creation_date}"):
            if self.mixed_training:
                policy = tf.keras.mixed_precision.Policy("mixed_float16")
                tf.keras.mixed_precision.set_global_policy(policy)
            self.update_metadata(**kwargs)

            # reset seed every time update metadata
            # if using set_seed, it will significant slow down training speed
            # cuz TF_DETERMINISTIC_OPS will degrade x tims of training speed by slowing down tf.data.map
            if self.dev:
                self.set_seed()

            train, val, test = self.get_data()

            model = self.generate_model()

            if self.fgm and self.fgm_layer:
                models.FGM_wrapper(model, self.fgm_layer, self.fgm_epsilon)

            model.fit(
                train,
                epochs=self.num_epochs,
                validation_data=val,
                callbacks=self.get_callbacks(),
                class_weight=self.class_weight,
                verbose=True,
                # validation_freq=3,
                validation_batch_size=self.batch_size * 2,
            )

            if self.swa:
                self.op.assign_average_vars(model.trainable_variables)
                # to handle the batchnormalization, we need to use 1 epoch forward pass to update the weights of bn
                print("Extra epoch run for the Normalization weights update with SWA")
                model.compile(
                    optimizer=self.get_optimizer(0),
                    loss=self.get_loss(),
                    metrics=[
                        tf.keras.metrics.AUC(),
                        tf.keras.metrics.Recall(),
                        tf.keras.metrics.Precision(),
                    ],
                )
                model.fit(
                    train,
                    validation_data=None,
                    epochs=1,
                )

            self.model_path = f"{self.output_path}models/{self.topic}/creation_date={self.model_creation_date}"
            model.save(
                self.model_path,
                # the optimizer's momentum vector or similar history-tracking properties without it -> the retraining model the optimizer like adam will work in different way, warmup will needed
                # include_optimizer=False,
            )
            if self.model_upload_weights:
                self.s3_model_output_path = f"{self.s3_models}/{self.env}/audience/model/experiment/date={self.model_creation_date}"

                data.s3_copy(self.model_path, self.s3_model_output_path)

            # log AUC for each targetingDataId for val and test data
            self.log_auc_fig(val, model, "val")

            self.log_auc_fig(test, model, "test")

            # get test metrics
            test_score = model.evaluate(test)

            # log extra params and metrics
            mlflow.log_params(
                {
                    "neo_embedding_size": self.neo_embedding_size,
                    "train_batch_size": self.batch_size,
                    "num_epochs": self.num_epochs,
                    "buffer_size": self.buffer_size,
                    "eval_batch_size": self.eval_batch_size,
                    "learning_rate": self.learning_rate,
                    "relaxation_factor": self.relaxation_factor,
                    "feature_dim_facor": self.feature_dim_factor,
                    "num_decision_steps": self.num_decision_steps,
                    "tabnet_factor": self.tabnet_factor,
                    "embedding_factor": self.embedding_factor,
                    "optimizer": self.optimizer,
                    "label_smoothing": self.label_smoothing,
                    "seed": self.seed,
                    "dropout_rate": self.dropout_rate,
                    "class_weight": self.class_weight,
                    "fgm": self.fgm,
                    "fgm_layer": self.fgm_layer,
                    "fgm_epsilon": self.fgm_epsilon,
                    "sum_residual_dropout": self.sum_residual_dropout,
                    "sum_residual_dropout_rate": self.sum_residual_dropout_rate,
                }
            )

            mlflow.set_tag("trainingSet", self.input_paths[0] + "split=train_tfrecord/")
            mlflow.set_tag("validationSet", self.input_paths[0] + "split=val_tfrecord/")
            mlflow.set_tag("testSet", self.test_input_paths[0] + "split=val_tfrecord/")

            mlflow.log_metrics(
                {
                    "test_loss": test_score[0],
                    "test_auc": test_score[1],
                    "test_recall": test_score[2],
                    "test_precision": test_score[3],
                }
            )

            del (
                train,
                val,
                test,
                model,
            )
            tf.keras.backend.clear_session()
            gc.collect()
