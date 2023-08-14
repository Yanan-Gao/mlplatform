# Created by jiaxing.pi at 11/9/22
import pytest
from philo.utils import load_partial_weights, extract_model_specific_settings
import numpy as np
from tests.utils import make_test_model

name_size1 = {'dl1': 4, 'dl2': 8}
name_size2 = {'dl1': 4, 'dl2': 8, 'dl3': 4}
old_model = make_test_model(name_size1)
params1 = {'region_specific_params': ['batch_size', 'learning_rate'],
           'batch_size': {'apac': 2, 'emea': 4, 'namer': 8},
           'learning_rate': {'apac': 0.0001, 'emea': 0.0005, 'namer': 0.0001}}
params1_apac = {'region_specific_params': ['batch_size', 'learning_rate'],
                'batch_size': 2,
                'learning_rate': 0.0001}
params2 = {'region_specific_params': [],
           'batch_size': 2, 'learning_rate': 0.001}


@pytest.mark.parametrize(
    'name_size, include_list, exclude_list',
    [(name_size1, ['dl1', 'dl2'], []), (name_size2, ['dl1', 'dl2'], ['dl3']),
     (name_size2, [], ['dl3']), (name_size1, [], []), (name_size1, ['dl1'], [])]
)
def test_load_partial_weights(name_size, include_list, exclude_list):
    new_model = make_test_model(name_size)
    load_partial_weights(new_model, old_model, include_list, exclude_list)
    if include_list:
        for i in include_list:
            old_weight = old_model.get_layer(i).get_weights()
            new_weight = new_model.get_layer(i).get_weights()
            all([np.allclose(x, y) for x, y in zip(old_weight, new_weight)])
    elif exclude_list:
        for i in new_model.layers:
            if i.name not in exclude_list:
                old_weight = old_model.get_layer(i.name).get_weights()
                new_weight = i.get_weights()
                all([np.allclose(x, y) for x, y in zip(old_weight, new_weight)])
    else:
        for i, j in zip(new_model.layers, old_model.layers):
            old_weight = i.get_weights()
            new_weight = j.get_weights()
            all([np.allclose(x, y) for x, y in zip(old_weight, new_weight)])


@pytest.mark.parametrize(
    'params, region, output',
    [(params1, 'apac', params1_apac), (params2, 'apac', params2)]

)
def test_extract_model_specific_settings(params, region, output):
    assert extract_model_specific_settings(params, region) == output
