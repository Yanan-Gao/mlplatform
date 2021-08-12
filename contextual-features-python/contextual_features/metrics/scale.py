from pyspark.sql import SparkSession
from contextual_features.models.cosine_model import CosineModel
from pyspark.sql.functions import col, concat_ws, array, substring, length
from datetime import timedelta, date
import os
import sys

def main():
    # pex_file = os.path.basename([path for path in sys.path if path.endswith(".pex")][0])
    # os.environ["PYSPARK_PYTHON"] = "./" + pex_file

    spark = (SparkSession.builder
                .master("yarn")
                .appName("standard-categories")
                .config("spark.jars.packages", "com.swoop:spark-alchemy_2.12:1.0.1,net.agkn:hll:1.6.0")
                .getOrCreate())

    # get date
    today = date.today()
    date_ = (today - timedelta(days=1)).strftime("%Y%m%d")

    # read classifications
    df = spark.read.parquet("s3://thetradedesk-fileshare/ai-lab/data/feature_store/test/airflow/standard_categories/{}".format(date_))

    # read avails 7-day traffic
    # Should this be run in the future? I.e give a website at least 4 days after classification to know true scale

    # sample pairs

    # average cosine-sim

    # write coherence per-category




if __name__ == "__main__":
    main()