import pandas as pd
import tensorflow as tf
from sklearn import metrics
from tensorflow.python.data.ops.dataset_ops import DatasetV2

def auc(df: pd.DataFrame):
    y=df['target']
    pred=df['pred']
    fpr, tpr, thresholds = metrics.roc_curve(y, pred, pos_label=1)
    return metrics.auc(fpr, tpr)


def get_auc_dist(val: DatasetV2,
                 model: tf.keras.Model,
                 auc_col_name: str = "auc",
                 count_col_name: str = "count"):
  dfList = []
  for item,t,w in val:
      xi = item
      pi = model.predict_on_batch(xi)
      df = pd.DataFrame({'AdGroupId': xi['AdGroupId']})
      df['pred']=pi
      df['target']=t
      dfList.append(df)
  df_final=pd.concat(dfList,axis=0)
  df_agg=df_final\
      .groupby('AdGroupId')\
      .apply(lambda df: pd.Series({auc_col_name:auc(df), count_col_name:df.shape[0]}))\
      .reset_index()
  return df_agg

