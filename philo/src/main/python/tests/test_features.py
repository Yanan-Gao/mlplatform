import pytest
import numpy as np
from philo.features import *
from philo.models import deep_fm



@pytest.mark.parametrize(
    'excluded_features, features',
    [([], DEFAULT_MODEL_FEATURES), (map(lambda x: x.name, DEFAULT_MODEL_FEATURES), [])]
)
def test_get_model_features(excluded_features, features):
    assert get_model_features(excluded_features) == features


