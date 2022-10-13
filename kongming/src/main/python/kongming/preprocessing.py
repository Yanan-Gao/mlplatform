import keras
from keras.layers import Embedding
import pandas as pd
from kongming.layers import VocabLookup
from kongming.features import *

#regular input, assuming shiftmod already done
def get_regular_input(feature):
    return keras.Input(shape=(1,), dtype=feature.type, name=f"{feature.name}")

#string lookup after string input
def get_string_layer(input, asset_root='./conversion-python/assets_string/', lookup_col=0):
    vocab = pd.read_csv(asset_root+input.name+'.txt', header=None)
    deduped_vocab=vocab[lookup_col].unique()
    max_tokens=deduped_vocab.size+1
    lup = keras.layers.experimental.preprocessing.StringLookup(max_tokens=max_tokens, vocabulary=list(deduped_vocab))
    return lup

def get_mapping_layer(input, asset_root='./conversion-python/assets_string/', key_dtype=tf.string, value_dtype=tf.string):
    lookup_out = VocabLookup(vocab_path=asset_root+input.name+'.txt', key_dtype=key_dtype, value_dtype=value_dtype)
    return lookup_out

def int_embedding(name, vocab_size=10000, emb_dim=40, dtype=tf.int32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    em = keras.layers.Embedding(input_dim=vocab_size, output_dim=emb_dim, name=f"{name}_embedding")
    f = keras.layers.Flatten(name=f"{name}_flatten")
    d = keras.layers.Dropout(seed=42, rate=0.3, name=f"{name}_dropout")
    return i, d(f( em((i)) ))

def float_feature(name, dtype=tf.float32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    return i, i

def string_embedding(name, em_size, assets_path):
    i = keras.Input(shape=(1,), dtype=tf.string, name=f"{name}")
    l = get_string_layer(i, assets_path)
    em = keras.layers.Embedding(input_dim=l.max_tokens, output_dim=em_size, name=f"{name}_embedding")
    f = keras.layers.Flatten(name=f"{name}_flatten")
    return i, f(em(l(i)))

def string_map_embedding(name, em_size, assets_path):
    i = keras.Input(shape=(1,), dtype=tf.string, name=f"{name}")
    l = get_mapping_layer(i, assets_path)
    ll = get_string_layer(i, assets_path, 1)
    em = keras.layers.Embedding(input_dim=ll.max_tokens, output_dim=em_size, name=f"{name}_embedding")
    f = keras.layers.Flatten(name=f"{name}_flatten")
    return i, f(em(ll(l(i))))


def int_map_embedding(name, assets_path, vocab_size=10000, emb_dim=40):
    i = keras.Input(shape=(1,), dtype=tf.int64, name=f"{name}")
    l = get_mapping_layer(i, assets_path, key_dtype=tf.int64, value_dtype=tf.int64)
    em = keras.layers.Embedding(input_dim=vocab_size, output_dim=emb_dim, name=f"{name}_embedding")
    f = keras.layers.Flatten(name=f"{name}_flatten")
    d = keras.layers.Dropout(seed=42, rate=0.3, name=f"{name}_dropout")
    return i, d(f(em(l(i))))
