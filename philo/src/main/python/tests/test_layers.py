import pytest
import numpy as np
import tensorflow as tf
from tensorflow.keras.layers import PReLU

from tensorflow.keras.utils import CustomObjectScope
from philo.layers import Dice, DNN, PredictionLayer, FM, Hash, Linear, CrossNet, CIN  # , NoMask
from tests.utils import layer_test

tf.keras.backend.set_learning_phase(True)

BATCH_SIZE = 5
FIELD_SIZE = 4
EMBEDDING_SIZE = 3
SEQ_LENGTH = 10


def test_dice():
    with CustomObjectScope({'Dice': Dice}):
        layer_test(Dice, kwargs={},
                   input_shape=(2, 3))


# def test_no_mask():
#     with CustomObjectScope({'NoMask': NoMask}):
#         layer_test(NoMask, kwargs={}, input_shape=(2, 3))


@pytest.mark.parametrize(
    'mode, use_bias, input_shape',
    [(mode, use_bias, (2, 3))
     for mode in [0, 1]
     for use_bias in [True, False]
     ]
    + [(2, use_bias, [(1, 2), (1, 2)])
       for use_bias in [True, False]
       ]
)
def test_linear_mode(mode, use_bias, input_shape):
    with CustomObjectScope({'Linear': Linear}):
        layer_test(Linear, kwargs={'mode': mode, 'use_bias': use_bias}, input_shape=input_shape)


@pytest.mark.parametrize(
    'num_buckets,mask_zero,vocabulary_path,input_data,expected_output',
    [
        (3 + 1, False, None, ['lakemerson'], None),
        (3 + 1, True, None, ['lakemerson'], None),
        (
                3 + 1, False, "vocabulary_example.csv", [['lake'], ['johnson'], ['lakemerson']],
                [[1], [3], [0]])
    ]
)
def test_hash(num_buckets, mask_zero, vocabulary_path, input_data, expected_output):
    with CustomObjectScope({'Hash': Hash}):
        layer_test(Hash,
                   kwargs={'num_buckets': num_buckets, 'mask_zero': mask_zero, 'vocabulary_path': vocabulary_path},
                   input_dtype=tf.string, input_data=np.array(input_data, dtype='str'),
                   expected_output_dtype=tf.int64, expected_output=expected_output)


# @pytest.mark.parametrize(
#     'reduce_sum',
#     [reduce_sum
#      for reduce_sum in [True, False]
#      ]
# )
# def test_inner_product_layer(reduce_sum):
#     with CustomObjectScope({'InnerProductLayer': InnerProductLayer}):
#         layer_test(InnerProductLayer, kwargs={
#             'reduce_sum': reduce_sum}, input_shape=[(BATCH_SIZE, 1, EMBEDDING_SIZE)] * FIELD_SIZE)


@pytest.mark.parametrize(
    'hidden_units,use_bn',
    [(hidden_units, use_bn)
     for hidden_units in [(), (10,)]
     for use_bn in [True, False]
     ]
)
def test_dnn(hidden_units, use_bn):
    with CustomObjectScope({'DNN': DNN}):
        layer_test(DNN, kwargs={'hidden_units': hidden_units, 'use_bn': use_bn, 'dropout_rate': 0.5},
                   input_shape=(BATCH_SIZE, EMBEDDING_SIZE))


@pytest.mark.parametrize(
    'task,use_bias',
    [(task, use_bias)
     for task in ['binary', 'regression']
     for use_bias in [True, False]
     ]
)
def test_prediction_layer(task, use_bias):
    with CustomObjectScope({'PredictionLayer': PredictionLayer}):
        layer_test(PredictionLayer, kwargs={'task': task, 'use_bias': use_bias}, input_shape=(BATCH_SIZE, 1))


@pytest.mark.xfail(reason="dim size must be 1 except for the batch size dim")
def test_prediction_layer_invalid():
    # with pytest.raises(ValueError):
    with CustomObjectScope({'PredictionLayer': PredictionLayer}):
        layer_test(PredictionLayer, kwargs={'use_bias': True}, input_shape=(BATCH_SIZE, 2, 1))


def test_fm():
    with CustomObjectScope({'FM': FM}):
        layer_test(FM, kwargs={}, input_shape=(
            BATCH_SIZE, FIELD_SIZE, EMBEDDING_SIZE))


@pytest.mark.parametrize(

    'layer_num',

    [0, 1]

)
def test_CrossNet(layer_num, ):
    with CustomObjectScope({'CrossNet': CrossNet}):
        layer_test(CrossNet, kwargs={
            'layer_num': layer_num, }, input_shape=(2, 3))


@pytest.mark.parametrize(
    'layer_size,split_half',
    [((10,), False), ((10, 8), True)
     ]
)
def test_CIN(layer_size, split_half):
    with CustomObjectScope({'CIN': CIN}):
        layer_test(CIN, kwargs={"layer_size": layer_size, "split_half": split_half}, input_shape=(
            BATCH_SIZE, FIELD_SIZE, EMBEDDING_SIZE))
