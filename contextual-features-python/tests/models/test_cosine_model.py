from contextual_features import __version__
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util
from contextual_features.models.cosine_model import CosineModel
from pyspark.sql.functions import col, concat_ws, substring
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, col, concat_ws, unix_timestamp, arrays_zip, hash
from pyspark.sql import Row 
from typing import Iterator, Tuple
import pandas as pd

def test_version():
    assert __version__ == '0.1.0'

def test_model_init():
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", [1], ["Sport"], 0.0)
    print(cm.label_embeddings)

def test_encode(spark):
    iab_ids = [0,1]
    iab_names = ["Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 0.0, device='cpu')

    source_data = [
        ("Sports",), 
        ("Business",)
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["text"]
    )

    actual = cm.transform(source_df, "text", "text", 2).collect()
    assert all(a["text"] == a["preds"]["class"][0] for a in actual)
    assert all(abs(a["preds"]["score"][0] - 1.0) < 1e-3 for a in actual)


def test_encode_title(spark):
    iab_ids = [0,1]
    iab_names = ["Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 0.0, device='cpu')

    source_data = [
        ("Roger Federrer wins grand slam", ),
        ("Financial support during coronavirus", ),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["text"]
    )

    actual = cm.transform(source_df, "text", "text", 2).collect()
    print(actual)
    expected = [(source_data[0][0], iab_names[0]), (source_data[1][0], iab_names[1])]

    assert [(a["text"], a["preds"]["class"][0]) for a in actual] == expected


def test_encode_title_and_text(spark):
    iab_ids = [0,1]
    iab_names = ["Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 0.5, device='cpu')

    source_data = [
        ("Not a useful title", "Sports"),
        ("Business", "Not useful text"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["title", "text"]
    )

    actual = cm.transform(source_df, "text", "title", 1).collect()
    expected = [("Sports", "Sports"), ("Not useful text", "Business")]

    assert [(a["text"], a["preds"]["class"][0]) for a in actual] == expected
    assert all(abs(a["preds"]["score"][0] - 1.0) < 1e-3 for a in actual)

def test_repeating_index(spark):
    iab_ids = [0,0,1]
    iab_names = ["asdfplk", "Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 1.0, device='cpu')

    source_data = [
        ("Not a useful title", "Sports"),
        ("Business", "Not useful text"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["title", "text"]
    )

    actual = cm.transform(source_df, "text", "title", 1).collect()
    expected = [("Sports", "asdfplk"), ("Not useful text", "Business")]
    assert list(cm._un) == ["asdfplk", "Business"]
    assert [(a["text"], a["preds"]["class"][0]) for a in actual] == expected
    assert all(abs(a["preds"]["score"][0] - 1.0) < 1e-3 for a in actual)


def test_repeating_index_top_2(spark):
    iab_ids = [0,0,1]
    iab_names = ["Sports", "Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 2.0, device='cpu')

    source_data = [
        ("Not a useful title", "Sports"),
        ("Business", "Not useful text"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["title", "text"]
    )

    actual = cm.transform(source_df, "text", "title", 2).collect()
    expected_top = [("Sports", "Sports"), ("Not useful text", "Business")]
    expected_second = [("Sports", "Business"), ("Not useful text", "Sports")]

    assert [(a["text"], a["preds"]["class"][0]) for a in actual] == expected_top
    assert [(a["text"], a["preds"]["class"][1]) for a in actual] == expected_second
    assert all(abs(a["preds"]["score"][0] - 1.0) < 1e-3 for a in actual)

def test_null_or_empty_text(spark):
    iab_ids = [0,0,1]
    iab_names = ["Sports", "Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 2.0, device='cpu')

    source_data = [
        ("123", ""),
        ("", "321"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["title", "text"]
    )

    actual = cm.transform(source_df, "title", "text", 2).collect()
    expected_top = [("123", "UNK"), ("","UNK")]
    expected_second = [("", "UNK"), ("321","UNK")]

    assert [(a["title"], a["preds"]["class"][0]) for a in actual] == expected_top
    assert [(a["text"], a["preds"]["class"][1]) for a in actual] == expected_second
    assert all(abs(a["preds"]["score"][0] - (-1.0)) < 1e-3 for a in actual)

def test_binary_col(spark):
    iab_ids = [0,1,2,3,4]
    iab_names = ["Puzzle Games", "Action Games", "Social Networking", "Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 2.0, device='cpu')

    source_data = [
        ("Not a useful title", "Sports"),
        ("Business", "Not useful text"),
        ("Card Games", "A strategic card game"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["title", "text"]
    )

    actual = cm.transform(source_df, "text", "title", 2).collect()

    expected_top = [("Not a useful title", "Sports"), ("Business", "Business"), ("Card Games", "Puzzle Games")]
    expected_second = [("Not a useful title", "Action Games"), ("Business", "Sports"), ("Card Games", "Action Games")]
    binary_lengths = [1,1,2]
    assert [(a["title"], a["preds"]["class"][0]) for a in actual] == expected_top
    assert [(a["title"], a["preds"]["class"][1]) for a in actual] == expected_second
    assert [sum(1 for b in a["preds"]["finalized"] if b) for a in actual] == binary_lengths

def test_merge_keeps_duplicates():
    # need to keep duplicates so that we don't end up with ragged arrays
    # duplicates occur when the model abstains from predicting
    m1 = [
        [5, 5],
        [-1, -1]
    ]
    merged = CosineModel.merge_preds([m1, m1], 2)
    assert merged == [(5,5), (-1,-1)]

def test_struct_formation(spark):
    iab_ids = [0,1,2,3,4]
    iab_names = ["Puzzle Games", "Action Games", "Social Networking", "Sports", "Business"]
    cm = CosineModel("huggingface/paraphrase-MiniLM-L3-v2", iab_ids, iab_names, 2.0, device='cpu')

    source_data = [
        ("Not a useful title", "Sports"),
        ("Business", "Not useful text"),
        ("Card Games", "A strategic card game"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        ["Url", "text"]
    )

    actual = cm.transform(source_df, "Url", "text", 2)

    actual\
        .select(
            col("Url").alias("Url"),
            hash(col("Url")).alias("UrlHash"),
            unix_timestamp().alias("CreatedDateTimeUtc"), 
            hash(col("Url"), col("text")).alias("ContentHash"),
            F.transform(
                arrays_zip(
                    col("preds.id"),
                    col("preds.class").alias("Name"),
                    col("preds.score"), 
                    col("preds.finalized")
                ),
                lambda x: F.struct(x.id.alias("Id"), x.Name.alias("Name"), x.score.alias("Score"), x.finalized.alias("Finalized"))
            ).alias("Categories")).printSchema()
