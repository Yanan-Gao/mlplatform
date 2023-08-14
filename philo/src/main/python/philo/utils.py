import tensorflow as tf
import numpy as np
import pandas as pd
import warnings
import yaml


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
        monitor='val_auc',
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
    for data, label, br_id in predict_data:
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


def load_partial_weights(new_model, old_model, inclusion=[], exclusion=[]):
    """
    load partial weights from the old model, if inclusion and exclusion are both empty,
    will copy all the weights from old_model, if both are not empty, will only use the
    inclusion list, this function will directly operate on the new_model, therefore, won't
    return anything
    Args:
        new_model: new model that will be further trained
        old_model: old_model from previous training
        inclusion: included layer names for reusing
        exclusion: excluded layer names for reusing

    Returns:
        None
    """
    if inclusion:
        if exclusion:
            warnings.warn("Can only have either inclusion or exclusion, will only use the inclusion list here")
        include_layer(new_model, old_model, inclusion)

    elif exclusion:
        exclude_layer(new_model, old_model, exclusion)

    else:
        # if both list are empty, copy all the available weight, it is equivalent to not exclude any
        warnings.warn("no value for inclusion or exclusion list, copy the entire model weights from the old model")
        exclude_layer(new_model, old_model, exclusion)


def exclude_layer(new_model, old_model, exclusion):
    """
        load partial weights from the old model, exclude the ones from exclusion list.
        Args:
            new_model: new model that will be further trained
            old_model: old_model from previous training
            exclusion: excluded layer names for reusing

        Returns:
            None
        """
    for i in new_model.layers:
        layer_name = i.name
        if layer_name not in exclusion:
            try:
                weights = old_model.get_layer(layer_name).get_weights()
                i.set_weights(weights)
                print(f"Copied weights for {i.name}")
            except ValueError as ve:
                print(f'error, for layer {i}, {ve}, skipping')


def include_layer(new_model, old_model, inclusion):
    """
        load partial weights from the old model, exclude the ones from exclusion list.
        Args:
            new_model: new model that will be further trained
            old_model: old_model from previous training
            inclusion: excluded layer names for reusing

        Returns:
            None
        """
    for i in inclusion:
        try:
            weights = old_model.get_layer(i).get_weights()
            try:
                new_model.get_layer(i).set_weights(weights)
                print(f"Copied weights for {i}")
            except ValueError as ve:
                print(f'error, for layer {i}, {ve}, skipping')
                continue
        except ValueError as ve:
            print(f'error, for layer {i}, {ve}, skipping')
            continue


def align_model_feature(input_layers, model_features):
    """
    align the model_features order based on model input layers
    Args:
        input_layers: list of input layers from previous trained model
        model_features: model_features that is extracted from the features.json

    Returns: re-aligned model features

    """
    model_input_order = [i.name for i in input_layers]
    feature_dict = {i.name: i for i in model_features}
    return [feature_dict.get(i) for i in model_input_order if feature_dict.get(i) is not None]


def read_yaml(path, region, key_name="region_specific_params"):
    """
    read yaml file for configuration
    Args:
        key_name: which key to look for region specific parameters
        path: path to the yaml file
        region: model region

    Returns: dictionary of settings

    """
    with open(path, 'r') as f:
        default_settings = yaml.safe_load(f)
    for k, v in default_settings.items():
        if isinstance(v, dict):
            default_settings[k] = extract_model_specific_settings(v, region, key_name)
    return default_settings


def extract_model_specific_settings(params, region, key_name="region_specific_params"):
    """
    each dictionary shall have a key of model_specific_params,
    which contains the key for the parameter that is set differently for
    different models
    Args:
        key_name: which key to look for region specific parameters
        params: parameter dictionary
        region: model region

    Returns:
        dictionary of original with the specific params set to the region model

    """
    if not params[key_name]:
        return params
    params_cp = params.copy()
    for i in params_cp[key_name]:
        params_cp[i] = params[i][region]
    return params_cp


def save_yaml(path, params):
    """
    save yaml file to local disk
    Args:
        path: path to the local disk
        params: params to be saved

    Returns: None

    """
    with open(path, 'w') as yaml_file:
        yaml.dump(params, yaml_file, default_flow_style=False)
