from collections import namedtuple

from absl import app, flags
from kongming.features import default_model_features, default_model_dim_group, Feature
from kongming.data import parse_input_files, tfrecord_dataset, parse_scoring_data, s3_copy
import tensorflow as tf
import pandas as pd
import os

# setting up training configuration
FLAGS = flags.FLAGS

# DEFAULTS
SCORE_SET_PATH = "./scoreset/"

PRED_PATH = "./prediction/"

MODEL_PATH = "./output/model/"

S3_OFFLINE_PRED = "s3://thetradedesk-mlplatform-us-east-1/measurement/kongming/offline"
# path

flags.DEFINE_string('score_set_path', default=SCORE_SET_PATH,
                    help=f'Location of offline scoring set (TFRecord). Default {SCORE_SET_PATH}')
flags.DEFINE_string('s3_offline_path', default=S3_OFFLINE_PRED,
                    help=f'Location of offline prediction on S3. Default {S3_OFFLINE_PRED}')
#TODO: we will need to copy multiple days worth of data over as well.
flags.DEFINE_list('score_dates', default=['20220502'], help='list of date strings for score path.')
flags.DEFINE_string('model_path', default=MODEL_PATH,
                    help=f'Location of saved model.')
flags.DEFINE_string('pred_path', default=PRED_PATH, help='output file location for predicted offline results.')
flags.DEFINE_string('colname_bidrequest', default='BidRequestIdStr', help='name of the bidrequestId column.')
flags.DEFINE_string('colname_adgroup', default='AdGroupIdStr', help='name of the adgroupId string column.')
#flags.DEFINE_list("string_features", default=[], help="String features for vocab lookup")

#config
flags.DEFINE_integer("scoring_batch_size", default=2**21, help="batch size for scoring")



def get_features_dim_target(additional_str_grain_map):

    features = [f._replace(ppmethod='string_vocab')._replace(type=tf.string)._replace(default_value='UNK')
                if f.name in FLAGS.string_features else f for f in default_model_features]

    features.append(Feature(additional_str_grain_map.BidRequestId, tf.string, None, '', None , 'simple'))
    features.append(Feature(additional_str_grain_map.AdGroupId, tf.string, None, '', None , 'simple'))

    model_dim = default_model_dim_group._replace(ppmethod='string_mapping')._replace(type=tf.string)._replace(
        default_value='UNK') \
        if default_model_dim_group.name in FLAGS.string_features else default_model_dim_group
    return features, model_dim


def get_scoring_data(features, dim_feature, additional_str_grain_map, date_for_score):
    #function to return
    score_files = parse_input_files(f"{FLAGS.score_set_path}date={date_for_score}/")
    score = tfrecord_dataset(score_files,
                                FLAGS.scoring_batch_size,
                                parse_scoring_data(features, dim_feature, additional_str_grain_map)
                                )
    return score

def predict(model, input):

  dfList = []
  for data, br_id, ag_id in input:
      xi = data
      pi = model.predict_on_batch(xi)
      df = pd.DataFrame({'BidRequestId':br_id.numpy().astype(str)})
      df['AdGroupId']=ag_id.numpy().astype(str)
      df['pred']=pi
      dfList.append(df)
  df_final=pd.concat(dfList, axis=0)

  return df_final

def get_model(model_path, to_compile=True):
    model_folder = model_path + os.listdir(model_path)[0]
    model = tf.keras.models.load_model(model_folder, compile=to_compile)
    return model

def main(argv):
    GrainNameMap = namedtuple('AdditionalGrainNameMap', ['BidRequestId', 'AdGroupId'])
    additional_str_grain_map = GrainNameMap(FLAGS.colname_bidrequest, FLAGS.colname_adgroup)
    model_features, model_dim_feature = get_features_dim_target(additional_str_grain_map)

    for date in FLAGS.score_dates:
        scoring_set = get_scoring_data(model_features, [model_dim_feature], additional_str_grain_map, date)
        model = get_model(FLAGS.model_path)
        pred = predict(model, scoring_set)

        os.makedirs(FLAGS.pred_path, exist_ok=True)
        result_location = f"{FLAGS.pred_path}pred.csv.gz"
        pred.to_csv(result_location, header=True, index=False, mode='w', compression="gzip")

        #output file to S3
        s3_output_path = f"{FLAGS.s3_offline_path}/{FLAGS.env}/date={FLAGS.model_creation_date}/scored_date={date}"
        s3_copy(FLAGS.pred_path, s3_output_path)


if __name__ == '__main__':
    app.run(main)
