# import pandas as pd
# from pyspark.sql.functions import pandas_udf
# from sklearn.metrics import roc_auc_score
import os


# @pandas_udf("double")
# def auc_udf(true: pd.Series, pred: pd.Series) -> float:
#     return roc_auc_score(true, pred)


# @pandas_udf("double")
# def count_udf(v: pd.Series) -> float:
#     return v.count()


# def unionAllDF(*dfs):
#     first, *_ = dfs
#     return first.sql_ctx.createDataFrame(
#         first.sql_ctx._sc.union([df.rdd for df in dfs]), first.schema
#     )


def s3_copy(src_path, dest_path, quiet=True):
    sync_command = f"aws s3 cp {src_path} {dest_path} --recursive"
    if (quiet):
        sync_command = sync_command + " --quiet"
    os.system(sync_command)
    return sync_command


def s3_move(src_path, dest_path, quiet=True):
    sync_command = f"aws s3 mv --recursive {src_path} {dest_path}"
    if (quiet):
        sync_command = sync_command + " --quiet"
    os.system(sync_command)
    return sync_command
