# Created by jiaxing.pi at 12/9/22

# Enter description here

import pytest
from philo.models import deep_fm, deep_fm_dual_tower, deep_fm_dual_tower_neo
from philo.neo import extract_neo_model, extract_fm_model, combine_neo_results, recalibrate_model, predict_neo_results
from philo.features import SparseFeat
from tests.utils import get_test_data, SAMPLE_SIZE, create_weights_for_layer
from tensorflow.keras.layers import Embedding
import numpy as np


@pytest.mark.parametrize(
    'dense_feature_num, sparse_feature_num',
    [(1, 4),  #
     (0, 4)
     ]  # (True, (32,), 3), (False, (32,), 1)
)
def test_extract_neo_model(dense_feature_num, sparse_feature_num):
    sample_size = SAMPLE_SIZE
    x, _, feature_columns = get_test_data(sample_size, sparse_feature_num=sparse_feature_num,
                                          dense_feature_num=dense_feature_num, sequence_feature=[])
    sparse_feat = list(filter(lambda feat: isinstance(feat, SparseFeat), feature_columns))
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    a_data = ({i: j for i, j in x.items() if i in adgroup_feature_name})
    b_data = ({i: j for i, j in x.items() if i not in adgroup_feature_name})
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    model = deep_fm(feature_columns, feature_columns, l2_reg_linear=0.0001)
    embed_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim > 1, model.layers))
    one_hot_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim == 1, model.layers))
    for i in embed_list:
        i.set_weights(create_weights_for_layer(i))
    for i in one_hot_list:
        i.set_weights(create_weights_for_layer(i))
    a_neo, b_neo = extract_neo_model(model, 'deepfm', feature_columns, feature_columns, adgroup_feature_name, 0.0001)
    fm_model = extract_fm_model(model)
    a_neo_predict = a_neo.predict(a_data)
    b_neo_predict = b_neo.predict(b_data)
    linear_logit_op, fm_vector_op = combine_neo_results(a_neo_predict, b_neo_predict, combined_value=False)
    fm_linear, fm_vector = fm_model.predict(x)

    assert pytest.approx(fm_linear, rel=1e-4) == linear_logit_op
    assert pytest.approx(fm_vector, rel=1e-4) == fm_vector_op


@pytest.mark.parametrize(
    'dense_feature_num, sparse_feature_num',
    [(1, 4),  #
     (0, 4)
     ]  # (True, (32,), 3), (False, (32,), 1)
)
def test_recalibrate_model(dense_feature_num, sparse_feature_num):
    sample_size = SAMPLE_SIZE
    x, _, feature_columns = get_test_data(sample_size, sparse_feature_num=sparse_feature_num,
                                          dense_feature_num=dense_feature_num, sequence_feature=[])
    model = deep_fm(feature_columns, feature_columns, l2_reg_linear=0.0001)
    model_r = recalibrate_model(model)

    # check if the weights are retained correctly
    fm_model = extract_fm_model(model)
    fm_model_r = extract_fm_model(model_r)
    fm_linear, fm_vector = fm_model.predict(x)
    fm_linear_r, fm_vector_r = fm_model_r.predict(x)

    assert pytest.approx(fm_linear, rel=1e-5) == fm_linear_r
    assert pytest.approx(fm_vector, rel=1e-5) == fm_vector_r


@pytest.mark.parametrize(
    'dense_feature_num, sparse_feature_num',
    [(1, 4),  #
     (0, 4)
     ]  # (True, (32,), 3), (False, (32,), 1)
)
def test_predict_neo_results(dense_feature_num, sparse_feature_num):
    # Testing the latest model structure - dual tower
    sample_size = SAMPLE_SIZE
    x, _, feature_columns = get_test_data(sample_size, sparse_feature_num=sparse_feature_num,
                                          dense_feature_num=dense_feature_num, sequence_feature=[])
    sparse_feat = list(filter(lambda feat: isinstance(feat, SparseFeat), feature_columns))
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    a_data = ({i: j for i, j in x.items() if i in adgroup_feature_name})
    b_data = ({i: j for i, j in x.items() if i not in adgroup_feature_name})
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    model = deep_fm_dual_tower(feature_columns, feature_columns, adgroup_feature_name, l2_reg_linear=0.0001)
    embed_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim > 1, model.layers))
    one_hot_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim == 1, model.layers))
    for i in embed_list:
        i.set_weights(create_weights_for_layer(i))
    for i in one_hot_list:
        i.set_weights(create_weights_for_layer(i))
    a_neo, b_neo = extract_neo_model(model, "deepfm_dual", feature_columns, feature_columns, adgroup_feature_name,
                                     0.0001)
    a_neo_predict = a_neo.predict(a_data)
    b_neo_predict = b_neo.predict(b_data)
    neo_pred_final = predict_neo_results(a_neo_predict, b_neo_predict, model.layers[-1].get_weights())

    origin_pred_final = model.predict(x)

    assert pytest.approx(origin_pred_final, rel=1e-3) == neo_pred_final


@pytest.mark.parametrize(
    'dense_feature_num, sparse_feature_num',
    [(1, 4),  #
     (0, 4)
     ]  # (True, (32,), 3), (False, (32,), 1)
)
def test_predict_neo_results_with_dual_model(dense_feature_num, sparse_feature_num):
    # Testing the latest model model - dual tower outputting 3 models in one go
    sample_size = SAMPLE_SIZE
    x, _, feature_columns = get_test_data(sample_size, sparse_feature_num=sparse_feature_num,
                                          dense_feature_num=dense_feature_num, sequence_feature=[])
    sparse_feat = list(filter(lambda feat: isinstance(feat, SparseFeat), feature_columns))
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    a_data = ({i: j for i, j in x.items() if i in adgroup_feature_name})
    b_data = ({i: j for i, j in x.items() if i not in adgroup_feature_name})
    adgroup_feature_name = [i.name for i in sparse_feat[:2]]
    model, a_neo, b_neo = deep_fm_dual_tower_neo(feature_columns, feature_columns, adgroup_feature_name,
                                                 l2_reg_linear=0.0001)

    origin_pred_final = model.predict(x)

    a_neo_predict = a_neo.predict(a_data)
    b_neo_predict = b_neo.predict(b_data)
    neo_pred_final = predict_neo_results(a_neo_predict, b_neo_predict, model.layers[-1].get_weights())

    assert pytest.approx(origin_pred_final, rel=1e-3) == neo_pred_final
