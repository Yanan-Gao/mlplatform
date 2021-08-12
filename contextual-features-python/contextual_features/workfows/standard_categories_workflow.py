from pyspark.sql import SparkSession
from contextual_features.models.cosine_model import CosineModel
from pyspark.sql.functions import col, concat_ws, array, substring, length, hash, unix_timestamp, arrays_zip
import pyspark.sql.functions as F
from datetime import timedelta, date
import os
import sys


def main(fileIn, fileOut, overwrite, top_k):
    """ Standard Categories workflow """
    pex_file = os.path.basename([path for path in sys.path if path.endswith(".pex")][0])
    os.environ["PYSPARK_PYTHON"] = "./" + pex_file

    spark = (SparkSession.builder
             .master("yarn")
             .appName("standard-categories")
             .config("spark.submit.deployMode", "client")
             .config("spark.yarn.dist.files", "./env.pex")
             .config("spark.pyspark.driver.python", "./env.pex")
             .config("spark.pyspark.python", "./env.pex")
             .config("spark.executorEnv.PEX_ROOT", "./tmp")
             .config("spark.yarn.appMasterEnv.PEX_ROOT", "./tmp")
             .config("spark.rapids.memory.gpu.allocFraction", 0)
             .config("spark.rapids.memory.gpu.pooling.enabled", 0)
             .config("spark.executor.resource.gpu.discoveryScript",
                     "/usr/lib/spark/scripts/gpu/getGpusResources.sh")
             .config("spark.worker.resource.gpu.discoveryScript",
                     "/usr/lib/spark/scripts/gpu/getGpusResources.sh")

             .config("spark.worker.resource.gpu.amount", 1)
             .config("spark.executor.resource.gpu.amount", 1)
             .config("spark.executor.cores", 8)
             .config("spark.task.cpus", 4)
             .config("spark.task.resource.gpu.amount", "0.5")

             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.sql.execution.arrow.maxRecordsPerBatch", 512)
             .getOrCreate())

    # get date
    today = date.today()
    date_ = (today - timedelta(days=2)).strftime("%Y%m%d")

    # get iab
    iab_df = spark.read.option("header",True).csv("s3://thetradedesk-fileshare/ai-lab/data/feature_store/test/iab/IAB22.csv")\
        .select(
            col("Id"),
            concat_ws(", ", array("Path1", "Path2", "Path3")).alias("text")
        )
    iab_ids, iab_names = zip(*[(p[0],p[1]) for p in iab_df.collect() if p[0] != ''])

    # load model
    cm = CosineModel("./model_weights", iab_ids, iab_names, 0.47, device=0)

    # run model
    english_content = spark.read.parquet(fileIn)

    # transform s3 data
    standard_categories_df = cm.transform(english_content ,"Url", "TextContent", top_k)

    mode = "overwrite" if overwrite else "errorifexists"
    # write s3 data
    standard_categories_df\
        .select(
            col("OriginalUrl").alias("Url"),
            hash(col("OriginalUrl")).alias("UrlHash"),
            unix_timestamp().alias("CreatedDateTimeUtc"), 
            hash(col("Url"), col("TextContent")).alias("ContentHash"),
            F.transform(
                arrays_zip(
                    col("preds.id"),
                    col("preds.class").alias("Name"),
                    col("preds.score"), 
                    col("preds.finalized")
                ),
                lambda x: F.struct(x.id.alias("Id"), x.Name.alias("Name"), x.score.alias("Score"), x.finalized.alias("Finalized"))
            ).alias("Categories"))\
        .write.mode(mode).parquet(fileOut)


if __name__ == "__main__":
    import argparse

    # get date
    today = date.today()
    date_ = (today - timedelta(days=2)).strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="calculate cosine model on fileIn and stores at fileOut")

    parser.add_argument("--fileIn", help="s3 or hdfs parquet input", type=str, 
        default="s3://thetradedesk-fileshare/ai-lab/data/feature_store/test/airflow/standard_categories_input/date={}".format(date_))

    parser.add_argument("--fileOut", help="s3 or hdfs parquet output", type=str, 
        default="s3://thetradedesk-fileshare/ai-lab/data/feature_store/test/airflow/standard_categories/date={}".format(date_))

    parser.add_argument("-o", "--overwrite", action="store_true", help="overwrite existing dataset")
    parser.add_argument("-k", "--topk", help="top k categories to store",type=int, default=10)

    args = parser.parse_args()

    main(args.fileIn, args.fileOut, args.overwrite, args.topk)
