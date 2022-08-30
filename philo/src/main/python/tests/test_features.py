import pytest
import numpy as np
from philo.features import *
import tensorflow as tf
from philo.models import deep_fm

feature_def = get_feature_definitions("feature_example.json")
features = [Feature("sparse_feat", True, tf.int64, 100, 0, True, DEFAULT_EMB_DIM),
            Feature("dense_feat", False, tf.float32, 1, 0.0, True, None)]
json_features = [{"Name": "sparse_feat", "Cardinality": 100},
                 {"Name": "dense_feat", "Cardinality": 0}]


@pytest.mark.parametrize(
    'excluded_features, model_features',
    [([], features), (map(lambda x: x.name, features), [])]
)
def test_get_model_features(excluded_features, model_features):
    assert get_model_features("feature_example.json", excluded_features) == model_features


def test_get_default_model_features():
    assert get_features_settings(json_features) == features


def test_get_feature_definitions():
    assert get_feature_definitions("feature_example.json") == json_features
