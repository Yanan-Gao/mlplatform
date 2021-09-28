import argparse
import numpy as np
import tensorflow as tf
from tensorflow.keras import backend as K
from datetime import datetime

from plutus.features import get_model_features, get_model_targets

from plutus.losses import google_fpa_nll

from plutus.models import dlrm_model

# import boto3






def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_date', type=str, default='2021-08-21')
    parser.add_argument('--batch_size' , type=int, default=2**12)
    parser.add_argument('--total_samples', type=int, default=1452150679 // 7)
    parser.add_argument('--excluded_features', type=str, default='')
    parser.add_argument('--learning_rate', type=float, default=0.00001)
    args = parser.parse_args()

    dataset_train, dataset_val, dataset_test, batch_per_epoch = read_data(args.data_date)

    if args.excluded_features != '':
        excluded_features = args.excluded_features.split(",")
    else:
        excluded_features = []


    model_features = get_model_features(excluded_features)
    model_targets = get_model_targets()



    log_dir = f"./logs/{datetime.now().strftime('%Y-%m-%d-%H')}"
    print(log_dir)

    es_cb = tf.keras.callbacks.EarlyStopping(patience=2, restore_best_weights=True)
    tb_callback = tf.keras.callbacks.TensorBoard(log_dir=log_dir,
                                                 write_graph=True,
                                                 profile_batch='100, 120')
    m = dlrm_model(model_features)
    m.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=args.learning_rate), loss=google_fpa_nll)

    trainable_count = np.sum([K.count_params(w) for w in m.trainable_weights])
    non_trainable_count = np.sum([K.count_params(w) for w in m.non_trainable_weights])

    print('Total params: {:,}'.format(trainable_count + non_trainable_count))
    print('Trainable params: {:,}'.format(trainable_count))
    print('Non-trainable params: {:,}'.format(non_trainable_count))
    S3 = boto3.resource('s3')


    history_full = m.fit(
         dataset_train,
         epochs=3,
         validation_data=dataset_val,
         verbose=1,
         callbacks=[es_cb, tb_callback])

    m.save("./model/")
    S3.Bucket('thetradedesk-mlplatform-us-east-1').upload_file("./model/saved_model.pb",
                                                                      "users/nick.noone/pc/saved_model.pb")


if __name__ == '__main__':
    main()



