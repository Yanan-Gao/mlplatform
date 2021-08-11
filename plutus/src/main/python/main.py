# Databricks notebook source
import os
from datetime import datetime


import tensorflow as tf
import numpy as np
from tensorflow import keras
from tensorflow.keras import backend as K


from lib import features
from lib.features import get_dataset_from_files
from model import get_full_model
import boto3

import argparse

from lib.tfutils import google_fpa_nll



from pathlib import Path



def process_files(train_files, val_files, batch_size):
    dataset_train = get_dataset_from_files(train_files, batch_size)
    dataset_val = get_dataset_from_files(val_files, batch_size)
    # dataset_test = get_dataset_from_files(files_test)
    return dataset_train, dataset_val



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_dir', type=str, default='home/hadoop/')
    parser.add_argument('--batch_size' , type=int, default=2**12)
    parser.add_argument('total_samples', type=int, default=1452150679 // 7)
    args = parser.parse_args()

    train_src_files = [str(f.resolve()) for f in list(Path(args.src_dir + 'tfrecords/train/').glob("*.gz"))]
    val_src_files = [str(f.resolve()) for f in list(Path(args.src_dir + 'tfrecords/validation/').glob("*.gz"))]

    dataset_train, dataset_val = process_files(train_src_files, val_src_files, args.batch_size)

    log_dir = f"./logs/{datetime.now().strftime('%Y-%m-%d-%H')}"
    print(log_dir)

    es_cb = keras.callbacks.EarlyStopping(patience=2, restore_best_weights=True)
    tb_callback = tf.keras.callbacks.TensorBoard(log_dir=log_dir,
                                                 write_graph=True,
                                                 profile_batch='100, 120')
    tf.keras.backend.clear_session()
    m = get_full_model(features.model_features)
    m.compile(optimizer=keras.optimizers.Adam(learning_rate=0.0001), loss=google_fpa_nll)

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



