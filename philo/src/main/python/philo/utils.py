import tensorflow as tf
import numpy as np
import pandas as pd


def get_callbacks(log_path, profile_batches, early_stopping_patience, checkpoint_base_path):
    """get callbacks during model training process

    Args:
        log_path (string): path to the logging folder
        profile_batches (list): Profile the batch(es) to sample compute characteristics.
        early_stopping_patience (int): number of epochs to decide early stop
        checkpoint_base_path (string): check point saving path

    Returns:
        list: list of settings
    """
    tb_callback = tf.keras.callbacks.TensorBoard(log_dir=log_path,
                                                 write_graph=True,
                                                 profile_batch=profile_batches)

    es_cb = tf.keras.callbacks.EarlyStopping(patience=early_stopping_patience,
                                             restore_best_weights=True)

    # checkpoint_base_path = f"{output_path}checkpoints/"
    checkpoint_filepath = checkpoint_base_path + "weights.{epoch:02d}-{val_loss:.2f}"
    chkp_cb = tf.keras.callbacks.ModelCheckpoint(
        filepath=checkpoint_filepath,
        save_weights_only=True,
        monitor='val_loss',
        mode='min',
        save_best_only=False,
        save_freq='epoch')

    return [tb_callback, es_cb, chkp_cb]



def generate_results(model, predict_data):
    """generate testing results

    Args:
        model (tf.python.keras.engine.functional.Functional): tf model
        predict_data (tf.data): tf data pipeline

    Returns:
        pd.DataFrame: testing results
    """
    pred_all = []
    label_all = []
    br_all = []
    #count = 1
    for data, label, br_id in predict_data:
        # count+=1
        # print(count)
        prediction = model.predict(data)
        pred_all.append(prediction)
        label_all.append(label.numpy())
        br_all.append(br_id.numpy())
    pred = np.vstack(pred_all)
    labels = np.vstack(label_all)
    br_id = np.vstack(br_all)
    comparisons = pd.DataFrame(
        {'BidRequestId': br_id.flatten(), 'labels': labels.flatten(), 'prediction': pred.flatten()})
    return comparisons
