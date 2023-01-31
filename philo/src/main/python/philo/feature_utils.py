# Note: a copy of this file exists in almost every model folder. At some point
# it would be nice if we could turn it into a python package in nexus instead

from collections import namedtuple
import json
import tensorflow as tf
from tensorflow.python.framework import dtypes

Feature = namedtuple("Feature",
                     "name, sparse, type, cardinality, default_value, enabled, embedding_dim, qr_embed, qr_collisions")

Target = namedtuple("Feature", "name, type, default_value, enabled, binary")

DEFAULT_EMB_DIM = 16


def get_features_from_json(json_path, exclude_features=[], default_emb_dim=DEFAULT_EMB_DIM, get_adgroup_feature=False):
    """
    Loads model features from json definition and converts it to Feature named tuples,
    excluding features if specified.
    Args:
        get_adgroup_feature: if get adgroup feature list, return an additional list
        json_path: path to the json file containing feature definitions
        exclude_features: list of features that are not used
        default_emb_dim: default embedding dimension size

    Returns: list of features, Optional, adgroup_feature_list
    """

    with open(json_path) as f:
        # todo: error handling?
        features = json.load(f)['ModelFeatureDefinitions'][0]['FeatureDefinitions']

    feature_list = []
    adgroup_feature_list = []
    for f in features:
        if f["Name"] not in exclude_features:
            cardinality, sparse, tf_type, default_value, emb_dim = (f["Cardinality"], True, tf.int64, 0, default_emb_dim) \
                if f["Cardinality"] > 0 else (1, False, tf.float32, 0.0, None)

            # type might have been specified directly
            tf_type = dtypes.as_dtype(f["TFType"]) if "TFType" in f else tf_type

            qr_embed = f["QREmbed"] if "QREmbed" in f else False
            qr_collisions = f["QRCollisions"] if "QRCollisions" in f and f["QRCollisions"] > 0 else None

            feature_list.append(Feature(f["Name"], sparse, tf_type, cardinality, default_value, True, emb_dim, qr_embed, qr_collisions))
            if f["AdgroupFeature"]==1:
                adgroup_feature_list.append(f["Name"])
    if get_adgroup_feature:
        return feature_list, adgroup_feature_list

    return feature_list
