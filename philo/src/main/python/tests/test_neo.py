# Created by jiaxing.pi at 12/9/22

# Enter description here

import pytest
from philo.models import deep_fm
from philo.neo import extract_neo_model, extract_fm_model, combine_neo_results
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
    a_neo, b_neo = extract_neo_model(model, feature_columns, feature_columns, adgroup_feature_name, 0.0001)
    fm_model = extract_fm_model(model)
    a_neo_predict = a_neo.predict(a_data)
    b_neo_predict = b_neo.predict(b_data)
    linear_logit_op, fm_vector_op = combine_neo_results(a_neo_predict, b_neo_predict, combined_value=False)
    fm_linear, fm_vector = fm_model.predict(x)

    assert pytest.approx(fm_linear, rel=1e-5) == linear_logit_op
    assert pytest.approx(fm_vector, rel=1e-5) == fm_vector_op
