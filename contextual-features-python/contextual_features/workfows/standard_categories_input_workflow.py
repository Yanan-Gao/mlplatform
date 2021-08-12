from pyspark.sql import SparkSession
from contextual_features.models.cosine_model import CosineModel
from pyspark.sql.functions import col, concat_ws, array, substring, length, hash, unix_timestamp, arrays_zip
import pyspark.sql.functions as F
from datetime import timedelta, date
import os
import sys


def main(date_,fileOut, overwrite):
    """ Standard Categories workflow """
    pex_file = os.path.basename([path for path in sys.path if path.endswith(".pex")][0])
    os.environ["PYSPARK_PYTHON"] = "./" + pex_file


    spark = (SparkSession.builder
             .master("yarn")
             .appName("standard-categories-input")
        #      .config("spark.submit.deployMode", "client")
             .config("spark.yarn.dist.files", "./env.pex")
             .config("spark.pyspark.driver.python", "./env.pex")
             .config("spark.pyspark.python", "./env.pex")
             .config("spark.executorEnv.PEX_ROOT", "./tmp")
             .config("spark.yarn.appMasterEnv.PEX_ROOT", "./tmp")

             .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
             .config("spark.executor.resource.gpu.discoveryScript",
                     "/usr/lib/spark/scripts/gpu/getGpusResources.sh")
             .config("spark.worker.resource.gpu.discoveryScript",
                     "/usr/lib/spark/scripts/gpu/getGpusResources.sh")
             .config("spark.sql.sources.useV1SourceList","")
             .config("spark.executor.extraLibraryPath","/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/extras/CUPTI/lib64:/usr/local/cuda/compat/lib:/usr/local/cuda/lib:/usr/local/cuda/lib64:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native")
             .config("spark.sql.shuffle.partitions","400")
             .config("spark.sql.files.maxPartitionBytes","256m")

             .config("spark.rapids.sql.concurrentGpuTasks","2")
             .config("spark.worker.resource.gpu.amount", 1)
             .config("spark.executor.resource.gpu.amount", 1)
             .config("spark.executor.cores", 8)
             .config("spark.task.cpus", 1)
             .config("spark.task.resource.gpu.amount", "0.125")
            #  .config("spark.rapids.memory.pinnedPool.size","2G")
             .config("spark.locality.wait","0s")

             .getOrCreate())

    mode = "overwrite" if overwrite else "errorifexists"
    # read s3 data
    content = spark.read.parquet("s3://ttd-datapipe-data/parquet/cxt_content/v=1/date={}/".format(date_))
    df = spark.read.parquet("s3://ttd-datapipe-data/parquet/cxt_tokenized_content/v=1/date={}/".format(date_))
    df.select("Url", "Language").filter(col("Language") == "eng").repartition("Url") \
        .join(content.select("Url", "TextContent").repartition("Url"), "Url") \
        .select(col("Url").alias("OriginalUrl"), substring("Url", 1, 500).alias("Url"), substring("TextContent", 1, 1000).alias("TextContent")) \
        .distinct() \
        .write \
        .mode(mode) \
        .parquet(fileOut)



if __name__ == "__main__":
    import argparse

    # get date
    from datetime import timedelta, date
    today = date.today()
    date_ = (today - timedelta(days=2)).strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="filter english corpus for input to cosine model")

    parser.add_argument("--date", help="day to prepare input for", type=str, default=date_)
    parser.add_argument("--fileOut", help="s3 or hdfs parquet output", type=str, 
        default="s3://thetradedesk-fileshare/ai-lab/data/feature_store/test/airflow/standard_categories_input/date={}".format(date_))

    parser.add_argument("-o", "--overwrite", action="store_true", help="overwrite existing dataset")

    args = parser.parse_args()

    main(args.date, args.fileOut, args.overwrite)
