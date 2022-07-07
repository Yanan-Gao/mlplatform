import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.isotonic import IsotonicRegression
import numpy as np
import tensorflow as tf
import math
from scipy.special import expit
from absl import app, flags
from datetime import datetime
from scoring import get_model
from kongming.data import s3_copy, s3_move
from kongming.layers import VocabLookup
from multiprocessing import Pool
from itertools import repeat
import os
import kongming.models

FLAGS = flags.FLAGS

INPUT_PATH = "./input/"
OUTPUT_PATH = "./output/"
MODEL_PATH = f"{OUTPUT_PATH}model/"
MODEL_LOGS = "./logs/"
ASSET_PATH = "./conversion-python/assets_string/"
ASSET_ADGROUP_LOOKUP_SCORE_PATH = f'{ASSET_PATH}ag_score_mod.txt'

CALIBRATED_MODEL_PATH = "output/model_calibrated/"
CALIBRATION_LOG_PATH = "output/calibration_log/"

OFFLINE_ATTRIBUTION_PATH = INPUT_PATH + "offlineattribution/"
OFFLINE_ATTRIBUTION_CVR_PATH = INPUT_PATH + "offlineattribution_cvr/"


flags.DEFINE_string('offline_attribution_path', default=OFFLINE_ATTRIBUTION_PATH,
                    help=f'Folder of offline attribution impression score')
flags.DEFINE_string('offline_attribution_cvr_path', default=OFFLINE_ATTRIBUTION_CVR_PATH,
                    help=f'Folder of offline attribution adgroup cvr')

flags.DEFINE_string('date',
                    default=datetime.now().strftime("%Y%m%d"),
                    help='The date we are generating the calibration model, which can be different from model_creation_date')

flags.DEFINE_string('conversion_model_path', default=MODEL_PATH,
                    help=f'Location of saved model.')
flags.DEFINE_integer('score_grid_count', default=10000, help='Grid count of scores')
flags.DEFINE_integer('cpu_cores', default=6, help='Number of cpu cores. It is used for multithread computing.')
flags.DEFINE_string('asset_adgroup_lookup_score_path', default=ASSET_ADGROUP_LOOKUP_SCORE_PATH, help='asset path.')

flags.DEFINE_string('calibrated_conversion_models', default=CALIBRATED_MODEL_PATH, help='output model local location.')
flags.DEFINE_string('calibrated_conversion_models_log', default=CALIBRATION_LOG_PATH, help='output model log local location.')

flags.DEFINE_float('r2_threshold', default=0.25, help='isotonic regression r2 threshold.')

def logit(s):
    """
    It takes a probability and returns the log odds

    :param s: the probability of a success
    :return: The logit function is being returned.
    """
    return math.log(s/(1-s))


def new_score_fun(df, bias, ag_cvr):
    """
    For each row in the dataframe, we take the logit of the score, subtract the bias, add the logit of the CVR for that
    adgroup, and then take the expit of that value

    :param df: the dataframe with the scores and adgroupids
    :param bias: the bias term in the logistic regression
    :param ag_cvr: the cvr for each adgroup
    :return: A dataframe with a new column called 'calibrated_cvr'
    """
    df['calibrated_cvr'] = df.\
        apply(lambda row: expit(logit(row['score']) - bias + logit(ag_cvr)), axis=1)
    return df

def parallelize_dataframe(df, bias, ag_cvr, func, n_cores):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.starmap(func, zip(df_split, repeat(bias), repeat(ag_cvr))))
    pool.close()
    pool.join()
    return df

def load_data():
    # prepare impression level score and label for isotonic regression
    impr_label = pd.read_csv(f"{FLAGS.offline_attribution_path}{FLAGS.date}.csv")
    impr_label.columns = ['adgroupidstr', 'adgroupid', 'piece_cvr', 'score', 'label', 'imr_weight_for_model']

    # prepare bias tuning
    cvr = pd.read_csv(f"{FLAGS.offline_attribution_cvr_path}{FLAGS.date}.csv", index_col=None)
    cvr.columns = ['adgroupidstr', 'cvr', 'adgroupid']

    cvr[['adgroupidstr', 'adgroupid']].\
        to_csv(f"{FLAGS.calibrated_conversion_models_log}ag_int_str_mapping.csv", index=False)

    cvr_dict = cvr[['adgroupid', 'cvr']].set_index("adgroupid").to_dict()

    # default_cvr is now useless, it has been used to generated cvr dataframe
    default_cvr = cvr_dict['cvr'].pop(-1)
    del cvr

    # load trained model
    # model = tf.keras.models.load_model("output/model/basic_0.3", compile=False)
    model = get_model(FLAGS.conversion_model_path, to_compile=False)
    bias = model.layers[-1].get_weights()[1][0]

    return impr_label, bias, cvr_dict, model


def add_value_to_log(ag_log, adgroupid, calibration_type, calibration_metric_key, calibration_metric_value):
    # could make this to a class instead of a function
    ag_log['adgroupid'].append(adgroupid)
    ag_log['calibration_type'].append(calibration_type)
    ag_log['calibration_metric_key'].append(calibration_metric_key)
    ag_log['calibration_metric_value'].append(calibration_metric_value)
    return ag_log

def calculate_calibration_adjustments(impr_label, bias, cvr_dict):
    # initialize outputs
    ag_log = {'adgroupid': [], 'calibration_type': [], 'calibration_metric_key': [], 'calibration_metric_value':[]}
    score_range = np.array(range(1, FLAGS.score_grid_count))/FLAGS.score_grid_count
    df_calibrated_ag_cvr = pd.DataFrame(columns=['adgroupid', 'score', 'calibrated_cvr'])

    for ag in impr_label['adgroupid'].unique():
        traintest = impr_label.query(f'adgroupid == {ag}')

        # todo: sample_weight: if row is positive, then it is the sum of the normalized conversion of that impression.
        #  (also suffer from being low value if pixel weight is low.) If row is negative, weight is 1.
        iso_reg = IsotonicRegression(increasing=True, out_of_bounds='clip').fit(
            X=traintest['score'].values
            , y=traintest['label']
            , sample_weight=traintest['imr_weight_for_model']
        )

        piece_cvr = traintest['piece_cvr'].values.reshape(-1, 1)
        iso_cvr = iso_reg.predict(traintest['score'].values)

        lin_reg = LinearRegression().fit(
            X=piece_cvr
            , y=iso_cvr
            , sample_weight=traintest['imr_weight_for_model']
        )
        lin_reg_r2 = lin_reg.score(piece_cvr, iso_cvr)

        if ((lin_reg.coef_[0]) > 0) and (lin_reg_r2 > FLAGS.r2_threshold):
            ag_log = add_value_to_log(ag_log, ag, "isotonic_reg", 'r-square', lin_reg_r2)
            ag_df = pd.DataFrame([ag]*len(score_range), columns=['adgroupid'])
            ag_df['score'] = score_range
            ag_df['calibrated_cvr'] = iso_reg.predict(score_range)
            df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)
        else:
            ag_cvr = cvr_dict['cvr'][ag]
            ag_log = add_value_to_log(ag_log, ag, "bias_tuning", 'offline_attribution_cvr', ag_cvr)
            ag_df = pd.DataFrame([ag]*len(score_range), columns=['adgroupid'])
            ag_df['score'] = score_range
            ag_df = parallelize_dataframe(ag_df, bias, ag_cvr, new_score_fun, n_cores=FLAGS.cpu_cores)
            df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)
        cvr_dict['cvr'].pop(ag)

    for ag in cvr_dict['cvr']:
        ag_cvr = cvr_dict['cvr'][ag]
        ag_log = add_value_to_log(ag_log, ag, "bias_tuning", 'offline_attribution_cvr', ag_cvr)
        ag_df = pd.DataFrame([ag] * len(score_range), columns=['adgroupid'])
        ag_df['score'] = score_range
        ag_df = parallelize_dataframe(ag_df, bias, ag_cvr, new_score_fun, n_cores=FLAGS.cpu_cores)
        df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)
#
    del impr_label

    # generate the log dataframe
    ag_calibration_logs = pd.DataFrame(ag_log).sort_values('calibration_type')

    # generate the look up table
    fstring = "{"+":.{}f".format(int(math.log10(FLAGS.score_grid_count)))+"}"
    df_calibrated_ag_cvr['key'] = (df_calibrated_ag_cvr['adgroupid'] + df_calibrated_ag_cvr['score']).apply(lambda x: fstring.format(x))

    return ag_calibration_logs,  df_calibrated_ag_cvr[['key', 'calibrated_cvr']]

def add_calibration_layer(model):

    # add calibration layer to model
    # adgroupid
    dim_input = model.input[-1]
    # score
    score_input = model.output
    # maximum_score for edge case where score round up to 1
    maximum_score = tf.constant(1-1/FLAGS.score_grid_count, dtype=tf.float32)

    # a bit hacky - adgroupid to float then add score
    addlayer = tf.keras.layers.Add(dtype=tf.float32, trainable=False)([tf.cast(dim_input, tf.float32), tf.math.minimum(maximum_score, score_input)])
    # round to 4 digits if score_grid_count is 10000
    score_precision = int(math.log10(FLAGS.score_grid_count))
    stringlayer = tf.strings.as_string(addlayer, precision=score_precision)
    # load the pre-generated lookup table
    lookup_out = VocabLookup(vocab_path=FLAGS.asset_adgroup_lookup_score_path, value_dtype=tf.float32, name="Output")
    # look up the addgroupid+score in
    output = lookup_out(stringlayer)
    # redefine output of conversion model

    # todo: what if adgroup is not shown up in the offline attribution result? now the calibrated model will output 0.
    calibrationmodel = tf.keras.models.Model(inputs=model.input, outputs=output)
    calibrationmodel.compile()

    return calibrationmodel

def main(argv):

    # load data for isotonic regression and bias tuning
    impr_label, bias, cvr_dict, model = load_data()

    # 1. calculate calibration based on impression and label for each adgroup
    ag_calibration_logs,  ag_calibrated_cvr = calculate_calibration_adjustments(impr_label, bias, cvr_dict)

    # save log and asset to local
    ag_calibration_logs.to_csv(f"{FLAGS.calibrated_conversion_models_log}ag_calibration_type.csv", index=False)
    ag_calibrated_cvr.to_csv(FLAGS.asset_adgroup_lookup_score_path, index=False, header=False)

    # upload log to s3
    log_s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model_log/calibrate_date={FLAGS.date}"
    s3_copy(FLAGS.calibrated_conversion_models_log, log_s3_output_path)

    # 2. add calibration_layer to model
    calibrated_model = add_calibration_layer(model)

    # save calibrated model to local
    calibrated_model_path = f"{FLAGS.calibrated_conversion_models}grid_count={FLAGS.score_grid_count}"
    calibrated_model.save(calibrated_model_path)

    # upload calibrated model to s3
    s3_output_path_tmp = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model/1"
    s3_copy(calibrated_model_path, s3_output_path_tmp)
    # rename trick
    s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model/{FLAGS.date}"
    s3_move(s3_output_path_tmp, s3_output_path)

if __name__ == '__main__':
    # took 20m for 500 adgroups on 32Gb 8cores machine.
    # requires data and model in: input/offlineattribution, input/offlineattribution_cvr, output/model
    app.run(main)