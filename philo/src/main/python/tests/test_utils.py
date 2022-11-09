# Created by jiaxing.pi at 11/9/22
import pytest
from philo.utils import load_partial_weights, exclude_layer, include_layer
import numpy as np
from tests.utils import make_test_model

name_size1 = {'dl1': 4, 'dl2': 8}
name_size2 = {'dl1': 4, 'dl2': 8, 'dl3': 4}
old_model = make_test_model(name_size1)


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
