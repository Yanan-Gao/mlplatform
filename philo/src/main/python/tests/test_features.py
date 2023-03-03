import pytest

from philo.feature_utils import *

feature_def = get_features_from_json("feature_example.json")
features = [Feature("sparse_feat", True, tf.int64, 100, 0, True, DEFAULT_EMB_DIM, False, None, 0),
            Feature("dense_feat", False, tf.float32, 1, 0.0, True, None, False, None, 0)]
exclude_features = ["dense_feat"]
json_features = [{"Name": "sparse_feat", "Cardinality": 100},
                 {"Name": "dense_feat", "Cardinality": 0}]
adgroup_feature = ['dense_feat']


varlen_features = [Feature("sparse_feat", True, tf.int64, 100, 0, True, DEFAULT_EMB_DIM, False, None, 0),
            Feature("dense_feat", False, tf.float32, 1, 0.0, True, None, False, None, 0),
            Feature("varlen_feat", True, tf.int64, 700, 0, True, DEFAULT_EMB_DIM, False, None, 700)]
varlen_exclude_features = ["varlen_feat"]


@pytest.mark.parametrize(
    'excluded_features, get_adgroup_feature, model_features',
    [([], False, features), (map(lambda x: x.name, features), False, []),
     ([], True, (features, adgroup_feature))]
)
def test_get_model_features(excluded_features, get_adgroup_feature, model_features):
    assert get_features_from_json("feature_example.json", excluded_features,
                                  get_adgroup_feature=get_adgroup_feature) == model_features

@pytest.mark.parametrize(
    'excluded_features, get_adgroup_feature, model_features',
    [([], False, varlen_features), (map(lambda x: x.name, varlen_features), False, []),
     ([], True, (varlen_features, adgroup_feature)), (varlen_exclude_features, False, features)]
)
def test_get_varlen_model_features(excluded_features, get_adgroup_feature, model_features):

    assert get_features_from_json("varlen_feature_example.json", excluded_features,
                                  get_adgroup_feature=get_adgroup_feature) == model_features

