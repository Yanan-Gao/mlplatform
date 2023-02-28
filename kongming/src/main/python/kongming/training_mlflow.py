from kongming.features import default_model_features, default_model_dim_group, default_model_targets, get_target_cat_map, extended_features, Feature
from kongming.utils import parse_input_files, s3_copy
from kongming.data import tfrecord_dataset, tfrecord_parser, csv_dataset, cache_prefetch_dataset
from kongming.models import dot_product_model, load_pretrained_embedding, auto_encoder_model
from kongming.losses import AELoss
from tensorflow_addons.losses import SigmoidFocalCrossEntropy
import tensorflow as tf
from datetime import datetime

class KongmingModelExperiment:
    def __init__(self, **kwargs):
        INPUT_PATH = "./input/"
        OUTPUT_PATH = "./output/"
        MODEL_LOGS = "./logs/"
        ASSET_PATH = "./assets_string/"
        EMBEDDING_PATH = "./embedding/"

        self.MIN_HASH_CARDINALITY = 301
        self.MAX_CARDINALITY = 10001

        S3_MODELS = "s3://thetradedesk-mlplatform-us-east-1/models"
        ENV = "prod"

        self.assets_path=ASSET_PATH
        self.env = ENV
        self.s3_models=S3_MODELS
        self.input_path = INPUT_PATH
        self.output_path = OUTPUT_PATH
        self.log_path = MODEL_LOGS
        self.log_tag = f"{datetime.now().strftime('%Y-%m-%d-%H')}"
        self.model_creation_date = datetime.now().strftime("%Y%m%d")

        self.activation = 'relu'
        self.string_features = []
        self.int_features = []
        self.sample_weight_col = 'Weight'
        self.model_choice = 'basic'
        self.external_embedding_path = EMBEDDING_PATH
        self.top_mlp_units = [256, 64]
        self.ae_units = [1024, 512, 128, 512, 1024]
        self.ae_embedding_sequence = 4
        self.embedding_model_name = None
        self.dropout_rate =0.3
        self.batchnorm = True
        self.loss_func = 'ce'
        self.batch_size = 4096 * 2
        self.num_epochs = 20
        self.eval_batch_size = 4096*16
        self.early_stopping_patience = 3
        self.profile_batches = [100, 120]
        self.learning_rate=0.001
        self.feature_list=[]
        # ['SupplyVendorId', 'Country', 'DeviceMake', 'DeviceModel', 'DeviceType', 'RenderingContext',
        #                 'sin_hour_day', 'cos_hour_day', 'sin_minute_hour', 'cos_minute_hour', 'sin_hour_week',
        #                'cos_hour_week', 'latitude', 'longitude'] #
        self.feature_exclusion_list = []
        ''''''
        self.feature_append_list = []
        self.extended_features = ["contextual"]
        self.gpu_num = 1
        self.use_csv = True
        self.cache_prefetch_dataset = True
        ''''''

    def update_metadata(self, **kwargs):
        for k,v in kwargs.items():
            self.__setattr__(k,v)


    def get_features_dim_target(self):
        extended_model_features = [f for k in self.extended_features if k in extended_features.keys()
                                   for f in extended_features[k] if len(extended_features[k]) > 0]
        model_features = default_model_features + extended_model_features

        print(model_features)
        feature_list = [f for f in model_features if f.name not in self.feature_exclusion_list]
        features = [f._replace(ppmethod='string_vocab')._replace(type=tf.string)._replace(default_value='UNK')
                    if f.name in self.string_features else f for f in feature_list]

        model_dim = default_model_dim_group._replace(ppmethod='string_mapping')._replace(type=tf.string)._replace(
            default_value='UNK') \
            if default_model_dim_group.name in self.string_features else default_model_dim_group

        if self.model_choice != 'ae':
            targets = default_model_targets
        else:
            # ae model feature, target setup
            targets = []
            for f in features:
                if f.type == tf.string and f.cardinality < self.MIN_HASH_CARDINALITY:
                    targets.append(f._replace(cardinality=f.cardinality * 2))
                elif f.type == tf.int64 and f.cardinality > self.MAX_CARDINALITY:
                    targets.append(f._replace(cardinality=self.MAX_CARDINALITY))
                else:
                    targets.append(f)
        return features, model_dim, targets

    def get_csv_cols(self, features, dim_features, targets, sw_col):
        selected_cols = {f.name: f for f in features}
        selected_cols.update({d.name: d for d in dim_features})
        selected_cols.update({t.name: t for t in targets})

        if sw_col != None:
            selected_cols[sw_col] = Feature(sw_col, tf.float32, cardinality=None, default_value=None)

        return selected_cols

    def get_data(self, features, dim_feature, targets, sw_col):
        #function to return
        if self.use_csv:
            # train_files, val_files = parse_input_files(self.input_path + "split=train/"), parse_input_files(
            #     self.input_path + "split=val/")
            train_files = val_files = ["input/train/part-00000v3.csv"]
            if len(train_files) == 0 or len(val_files) == 0:
                raise Exception("No training or validation files")

            selected_cols = self.get_csv_cols(features, dim_feature, targets, sw_col)

            label_name = "Target"
            train = csv_dataset(train_files, self.batch_size * self.gpu_num, selected_cols, label_name, sw_col)
            val = csv_dataset(val_files, self.eval_batch_size, selected_cols, label_name, sw_col)
        else:
            train_files, val_files = parse_input_files(self.input_path+"split=train/"), parse_input_files(self.input_path+"split=val/")
            train = tfrecord_dataset(train_files,
                                        self.batch_size * self.gpu_num,
                                        tfrecord_parser(features, dim_feature, targets, self.model_choice, self.MAX_CARDINALITY, sw_col)
                                        )
            val = tfrecord_dataset(val_files,
                                        self.eval_batch_size,
                                        tfrecord_parser(features, dim_feature, targets, self.model_choice, self.MAX_CARDINALITY, sw_col)
                                        )
        return train, val


    def get_model(self, features, dim_feature, assets_path, target_size):
        # function for building the model
        if self.model_choice == "basic":
            model = dot_product_model(features,
                              dim_feature,
                              activation="relu",
                              top_mlp_layers=self.top_mlp_units,
                              dropout_rate=self.dropout_rate,
                              batchnorm=self.batchnorm,
                              assets_path=assets_path
                              )
        elif self.model_choice == "ae":
            model = auto_encoder_model(features,
                              ae_units=self.ae_units,
                              dropout_rate=self.dropout_rate,
                              batchnorm=self.batchnorm,
                              assets_path=assets_path,
                              target_size=target_size
                              )
        elif self.model_choice == "basic-ae":
            encoder=load_pretrained_embedding(path=self.external_embedding_path+'model/'+self.embedding_model_name, layer_id=self.ae_embedding_sequence)
            model = dot_product_model(features,
                              dim_feature,
                              activation="relu",
                              top_mlp_layers=self.top_mlp_units,
                              dropout_rate=self.dropout_rate,
                              batchnorm=self.batchnorm,
                              assets_path=assets_path,
                              pretrained_layer=encoder
                              )
        else:
            raise Exception("unknown model type.")
        return model

    def get_loss(self, cat_dict):
        if self.model_choice=='ae':
            loss = {'cat':AELoss(cat_dict), 'cont':'mse'}
            return loss
        else:
            if self.loss_func=='ce':
                return tf.keras.losses.BinaryCrossentropy()
            elif self.loss_func=='fce':
                return SigmoidFocalCrossEntropy()
            else:
                raise Exception("Loss not supported.")

    def get_metrics(self):
        if self.model_choice == 'ae':
            return ['accuracy']
        else:
            return ['AUC']

    def get_callbacks(self):
        tb_callback = tf.keras.callbacks.TensorBoard(log_dir=f"{self.log_path}{self.log_tag}",
                                                     write_graph=True,
                                                     profile_batch=self.profile_batches)

        es_cb = tf.keras.callbacks.EarlyStopping(patience=self.early_stopping_patience,
                                                 restore_best_weights=True)

        checkpoint_base_path = f"{self.output_path}checkpoints/"
        checkpoint_filepath = checkpoint_base_path + "weights.{epoch:02d}-{val_loss:.2f}"
        chkp_cb = tf.keras.callbacks.ModelCheckpoint(
            filepath=checkpoint_filepath,
            save_weights_only=True,
            monitor='val_loss',
            mode='min',
            save_best_only=False,
            save_freq='epoch')

        return [tb_callback, es_cb, chkp_cb]

    def get_ae_target_size(self, dataset):
        # this dataset has one one sample
        if self.model_choice=='ae':
            for examples in dataset:
                cat_size, cont_size = examples[1]['cat'].shape[1], examples[1]['cont'].shape[1]
                break
            return {'cat':cat_size, 'cont':cont_size}
        else:
            return None

    def model_gen(self):

        model_features, model_dim_feature, model_targets = self.get_features_dim_target()

        train, val= self.get_data(model_features, [model_dim_feature], model_targets, self.sample_weight_col)

        # for f, labels, w in train.take(1):
        #     for data_key, data_value in f.items():
        #         print(f"{data_key}     :    {data_value}", data_value.shape)

        if self.cache_prefetch_dataset:
            train = cache_prefetch_dataset(train)
            val = cache_prefetch_dataset(val)

        target_size = self.get_ae_target_size(train.take(1))

        model = self.get_model(model_features, model_dim_feature, self.assets_path, target_size)

        model.summary()

        cat_dict = get_target_cat_map(model_targets, self.MAX_CARDINALITY) if self.model_choice=='ae' else None

        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
                      loss=self.get_loss(cat_dict),
                      metrics=self.get_metrics())
                      # loss_weights=None,
                      # run_eagerly=None,
                      # steps_per_execution=None)

        return model, train, self.num_epochs, self.get_callbacks(), val, self.eval_batch_size


