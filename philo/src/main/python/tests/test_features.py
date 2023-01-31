import pytest

from philo.feature_utils import *

feature_def = get_features_from_json("feature_example.json")
features = [Feature("sparse_feat", True, tf.int64, 100, 0, True, DEFAULT_EMB_DIM, False, None),
            Feature("dense_feat", False, tf.float32, 1, 0.0, True, None, False, None)]
exclude_features = ["dense_feat"]
json_features = [{"Name": "sparse_feat", "Cardinality": 100},
                 {"Name": "dense_feat", "Cardinality": 0}]
adgroup_feature = ['dense_feat']


@pytest.mark.parametrize(
    'excluded_features, get_adgroup_feature, model_features',
    [([], False, features), (map(lambda x: x.name, features), False, []),
     ([], True, (features, adgroup_feature))]
)
def test_get_model_features(excluded_features, get_adgroup_feature, model_features):
    assert get_features_from_json("feature_example.json", excluded_features,
                                  get_adgroup_feature=get_adgroup_feature) == model_features


