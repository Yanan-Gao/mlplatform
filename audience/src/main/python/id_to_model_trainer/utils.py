import pandas as pd
from pyspark.sql.functions import pandas_udf
from sklearn.metrics import roc_auc_score


@pandas_udf("double")
def auc_udf(true: pd.Series, pred: pd.Series) -> float:
    return roc_auc_score(true, pred)


@pandas_udf("double")
def count_udf(v: pd.Series) -> float:
    return v.count()


def unionAllDF(*dfs):
    first, *_ = dfs
    return first.sql_ctx.createDataFrame(
        first.sql_ctx._sc.union([df.rdd for df in dfs]), first.schema
    )
