import pandas as pd
import numpy as np
import tensorflow as tf
import math
from scipy.special import expit
from absl import app, flags
from datetime import datetime
from scoring import get_model
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from kongming.treebin import optimal_binning_boundary
from kongming.isotonic_regressor import isotonic_regressor
from kongming.utils import load_csv, s3_copy, modify_model_embeddings
from kongming.layers import VocabLookup
from sklearn import metrics
from multiprocessing import Pool
from itertools import repeat
import seaborn as sns
import matplotlib.pyplot as plt
import os
import kongming.models

FLAGS = flags.FLAGS

INPUT_PATH = "./input/"
OUTPUT_PATH = "./output/"
MODEL_PATH = f"{INPUT_PATH}model/"
MODEL_LOGS = "./logs/"
ASSET_ADGROUP_LOOKUP_SCORE_PATH = f'{OUTPUT_PATH}ag_score_mod.txt'

CALIBRATED_MODEL_PATH = "output/model_calibrated/"
CALIBRATION_LOG_PATH = "output/calibration_log/"

OFFLINE_ATTRIBUTION_PATH = INPUT_PATH + "offlineattribution/"
OFFLINE_ATTRIBUTION_CVR_PATH = INPUT_PATH + "offlineattribution_cvr/"
CLIENT_TEST_ADGROUPS_PATH = INPUT_PATH + "client_test_adgroups/"


flags.DEFINE_string('offline_attribution_path', default=OFFLINE_ATTRIBUTION_PATH,
                    help=f'Folder of offline attribution impression score')
flags.DEFINE_string('offline_attribution_cvr_path', default=OFFLINE_ATTRIBUTION_CVR_PATH,
                    help=f'Folder of offline attribution adgroup cvr')
flags.DEFINE_string('client_test_adgroups_path', default=CLIENT_TEST_ADGROUPS_PATH,
                    help=f'Folder of client test adgroups')

flags.DEFINE_string('date',
                    default=datetime.now().strftime("%Y%m%d"),
                    help='The date we are generating the calibration model, which can be different from model_creation_date')

flags.DEFINE_string('conversion_model_path', default=MODEL_PATH,
                    help=f'Location of saved model.')
flags.DEFINE_integer('score_grid_count', default=100000, help='Grid count of scores')

flags.DEFINE_integer('max_leaf_nodes', default=100, help='CVR binning max leaf nodes')
flags.DEFINE_float('min_samples_leaf', default=0.05, help='CVR binning min samples per leaf')
flags.DEFINE_float('r2_threshold', default=0.1, help='Isotonic regression r2 threshold.')

flags.DEFINE_integer('cpu_cores', default=6, help='Number of cpu cores. It is used for multithread computing.')
flags.DEFINE_string('asset_adgroup_lookup_score_path', default=ASSET_ADGROUP_LOOKUP_SCORE_PATH, help='asset adgroup look up score path')

flags.DEFINE_string('calibrated_conversion_models', default=CALIBRATED_MODEL_PATH, help='output model local location.')
flags.DEFINE_string('calibrated_conversion_models_log', default=CALIBRATION_LOG_PATH, help='output model log local location.')


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
    :return: A dataframe with a new column called 'predicted_cvr'
    """
    df['predicted_cvr'] = df.\
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
    impr_label_columns = ['adgroupidstr', 'adgroupid', 'score', 'label', 'imr_weight_for_model']
    impr_label = load_csv(FLAGS.offline_attribution_path, impr_label_columns)

    # prepare bias tuning
    cvr_columns = ['adgroupidstr', 'cvr', 'adgroupid']
    cvr = load_csv(FLAGS.offline_attribution_cvr_path, cvr_columns)

    cvr[['adgroupidstr', 'adgroupid']].\
        to_csv(f"{FLAGS.calibrated_conversion_models_log}ag_int_str_mapping.csv", index=False)

    cvr_dict = cvr[['adgroupid', 'cvr']].set_index("adgroupid").to_dict()

    # load test adgroups
    test_adgroup_columns = ['Advertiser', 'BaseAdGroupId', 'BaseAdGroupIdInt', 'AdGroupId', 'AdGroupIdInt']
    test_adgroups = load_csv(FLAGS.client_test_adgroups_path, test_adgroup_columns)

    # default_cvr is now useless, it has been used to generated cvr dataframe
    del cvr

    # load trained model
    # model = tf.keras.models.load_model("output/model/basic_0.3", compile=False)
    model = get_model(FLAGS.conversion_model_path, to_compile=False)
    bias = model.layers[-1].get_weights()[1][0]

    return impr_label, bias, cvr_dict, test_adgroups, model


def get_metrics(df: pd.DataFrame):
    label = df['label'].values.reshape((-1, 1))
    score = df['score'].values.reshape((-1, 1))
    sample_weight = df['imr_weight_for_model'].values.reshape((-1, 1))
    binned_cvr = df['cvr'].values.reshape((-1, 1))
    predicted_cvr = df['predicted_cvr'].values.reshape((-1, 1))

    # r2 is between 0 and 1. Larger r2 is better,
    lin_reg = LinearRegression().fit(
        X=binned_cvr
        , y=predicted_cvr
    )
    r2 = lin_reg.score(binned_cvr, predicted_cvr)
    slope = lin_reg.coef_[0][0]
    # oe, when it is higher than 1, means underestimate cvr. When is lower than 1, means overestimate cvr.
    # todo: we may use this oe to predicted adjust cvr
    oe = (np.inner(label.reshape(1,-1), sample_weight.reshape(1,-1))/np.inner(predicted_cvr.reshape(1,-1), sample_weight.reshape(1,-1)))[0][0]
    # mean-squared-error for positives and negative
    mse_pos = mean_squared_error(df.query('label==1')['cvr'].values, df.query('label==1')['predicted_cvr'].values, sample_weight = df.query('label==1')['imr_weight_for_model'].values)
    mse_neg = mean_squared_error(df.query('label==0')['cvr'].values, df.query('label==0')['predicted_cvr'].values, sample_weight = df.query('label==0')['imr_weight_for_model'].values)
    mse_all = mean_squared_error(binned_cvr, predicted_cvr, sample_weight=sample_weight)
    # auc
    fpr, tpr, _ = metrics.roc_curve(label, score, pos_label=1, sample_weight=sample_weight)
    auc = metrics.auc(fpr, tpr)

    metric_dict={ 'R2BinCvrPredCvr': r2, 'SlopeBinCvrPredCvr':slope, 'ObservationOverEstimation': oe, 'AUC': auc, 'MSE_Pos':mse_pos, 'MSE_Neg':mse_neg, 'MSE_All':mse_all}
    return metric_dict

def get_score_plot_with_bin_cvr(df: pd.DataFrame, ag_log_key, score_plot_path):
    # generate plot
    sns.scatterplot(data=df, x='score', y='cvr', label='score to cvr', s=1)
    sns.scatterplot(data=df, x='score', y='predicted_cvr', label='score to calibrated cvr', s=1)
    plt.legend()
    plt.title(ag_log_key)
    plt.savefig(f"{score_plot_path}{ag_log_key}.png")
    plt.clf()

def get_score_plot_without_bin_cvr(df: pd.DataFrame, ag_log_key, score_plot_path):
    # generate plot
    sns.scatterplot(data=df, x='score', y='predicted_cvr', label='score to calibrated cvr', s=1)
    plt.legend()
    plt.title(ag_log_key)
    plt.savefig(f"{score_plot_path}{ag_log_key}.png")
    plt.clf()



def recoverCVR(groupDF):
    # positive impressions are not sampled, imr_weight_for_model is the summed conversion weight
    conversions = groupDF.query('label==1')['imr_weight_for_model'].sum()
    # negative impressions are sampled, imr_weight_for_model is the inverse of sample rate
    impressions = groupDF.query('label==1')['imr_weight_for_model'].count() + groupDF.query('label==0')['imr_weight_for_model'].sum()
    return conversions / impressions

def calculate_calibration_adjustments(impr_label, bias, cvr_dict, test_adgroups):
    # initialize metrics log
    log_dict = {}
    score_plot_path = f"{FLAGS.calibrated_conversion_models_log}score_plot/"
    os.makedirs(score_plot_path, exist_ok=True)

    # define hyper parameters
    alphas = np.linspace(0.1, 1, 10)

    score_range = np.array(range(1, FLAGS.score_grid_count))/FLAGS.score_grid_count
    df_calibrated_ag_cvr = pd.DataFrame(columns=['adgroupid', 'score', 'predicted_cvr'])

    for ag in test_adgroups['BaseAdGroupIdInt'].unique():
        traintest = impr_label.query(f'adgroupid == {ag}')
        ag_cvr = cvr_dict['cvr'][ag]
        if (len(traintest)) > 0: #have samples and enough conversions, eligible for isotonic regression try

            # CVR binning
            boundary = optimal_binning_boundary(x=traintest['score'],
                                                y=traintest['label'],
                                                sample_weight = traintest['imr_weight_for_model'],
                                                criterion='entropy', max_leaf_nodes=FLAGS.max_leaf_nodes,
                                                min_samples_leaf=FLAGS.min_samples_leaf)

            bins = np.digitize(traintest['score'], boundary)
            traintest_binned = traintest
            traintest_binned['bin'] = bins

            traintest_binned = traintest_binned.merge(
                traintest_binned.groupby('bin').apply(recoverCVR).reset_index().rename(columns={0: 'cvr'}), on='bin',
                how="inner")

            # Smoothed isotonic
            myf_cvr_smooth = isotonic_regressor(n_gridpoints=100, isotonic=1, convexity=0)
            BICs = myf_cvr_smooth.fit_alphas(traintest_binned['score'].values, traintest_binned['cvr'], alphas, min_x=0,
                                             max_x=1)
            traintest_binned['predicted_cvr'] = myf_cvr_smooth.predict(traintest_binned['score'].values)
            traintest_binned['predicted_cvr'] = traintest_binned['predicted_cvr'].apply(lambda x: x if x > 0 else 0)

            # calculate metrics based on sampled impressions
            metrics_dict = get_metrics(traintest_binned)

            if ((metrics_dict['SlopeBinCvrPredCvr']) > 0) and (metrics_dict['R2BinCvrPredCvr'] > FLAGS.r2_threshold):
                for test_ag in test_adgroups.query(f"BaseAdGroupIdInt=={ag}")['AdGroupIdInt'].unique():
                    # log metrics
                    test_ag_id_str = test_adgroups.query(f"AdGroupIdInt=={test_ag}")['AdGroupId'].values[0]
                    metrics_dict.update({'Method': 'SmoothedIsotonic', 'Ag_CVR': ag_cvr})
                    log_dict[test_ag_id_str] = metrics_dict
                    get_score_plot_with_bin_cvr(traintest_binned, test_ag_id_str, score_plot_path)
                    # create score to cvr df
                    ag_df = pd.DataFrame([test_ag] * len(score_range), columns=['adgroupid'])
                    ag_df['score'] = score_range
                    ag_df['predicted_cvr'] = myf_cvr_smooth.predict(score_range)
                    df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)
            else:
                # todo: initial experiment shows cvr of ag optimize oe towards 1, while r2 is not necessarily optimized
                for test_ag in test_adgroups.query(f"BaseAdGroupIdInt=={ag}")['AdGroupIdInt'].unique():
                    # calculate metrics based on sampled impressions
                    traintest_binned['score'] = traintest_binned['score'].apply(lambda x: x-(1e-10) if x == 1 else x)
                    traintest_binned_bias_tuning = parallelize_dataframe(traintest_binned, bias, ag_cvr, new_score_fun, n_cores=FLAGS.cpu_cores)
                    # log metrics
                    test_ag_id_str = test_adgroups.query(f"AdGroupIdInt=={test_ag}")['AdGroupId'].values[0]
                    metrics_dict = get_metrics(traintest_binned_bias_tuning)
                    metrics_dict.update({'Method': 'BiasTuning', 'Ag_CVR': ag_cvr})
                    log_dict[test_ag_id_str] = metrics_dict
                    get_score_plot_with_bin_cvr(traintest_binned_bias_tuning, test_ag_id_str, score_plot_path)
                    # create score to cvr df
                    ag_df = pd.DataFrame([test_ag]*len(score_range), columns=['adgroupid'])
                    ag_df['score'] = score_range
                    ag_df = parallelize_dataframe(ag_df, bias, ag_cvr, new_score_fun, n_cores=FLAGS.cpu_cores)
                    df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)

                    del traintest_binned_bias_tuning
            del traintest_binned
            cvr_dict['cvr'].pop(ag)
        else: # no samples, go directly to bias tuning
            for test_ag in test_adgroups.query(f"BaseAdGroupIdInt=={ag}")['AdGroupIdInt'].unique():
                # log metrics
                test_ag_id_str = test_adgroups.query(f"AdGroupIdInt=={test_ag}")['AdGroupId'].values[0]
                log_dict[test_ag_id_str] = {'Method': 'BiasTuning', 'Ag_CVR': ag_cvr}
                # create score to cvr df
                ag_df = pd.DataFrame([test_ag] * len(score_range), columns=['adgroupid'])
                ag_df['score'] = score_range
                ag_df = parallelize_dataframe(ag_df, bias, ag_cvr, new_score_fun, n_cores=FLAGS.cpu_cores)
                get_score_plot_without_bin_cvr(ag_df, test_ag_id_str, score_plot_path)
                df_calibrated_ag_cvr = df_calibrated_ag_cvr.append(ag_df)

    del impr_label

    # upload calibration plot to s3
    s3_score_plot_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model_log/calibrate_date={FLAGS.date}/score_plot"
    s3_copy(score_plot_path, s3_score_plot_path)

    # generate the log dataframe
    ag_calibration_logs = pd.DataFrame.from_dict(log_dict, orient='index').reset_index().rename(columns = {'index':'AdGroupId'})

    # generate the look up table
    fstring = "{"+":.{}f".format(int(math.log10(FLAGS.score_grid_count)))+"}"
    df_calibrated_ag_cvr['key'] = (df_calibrated_ag_cvr['adgroupid'] + df_calibrated_ag_cvr['score']).apply(lambda x: fstring.format(x))

    return ag_calibration_logs,  df_calibrated_ag_cvr[['key', 'predicted_cvr']]

def add_calibration_layer(model):

    # add calibration layer to model
    # adgroupid
    dim_input = model.input[-1]
    # score
    score_input = model.output
    # maximum_score for edge case where score round up to 1
    maximum_score = tf.constant(1-1/FLAGS.score_grid_count, dtype=tf.float32)

    # a bit hacky - adgroupid to float then add score
    addlayer = tf.keras.layers.Add(dtype=tf.float64, trainable=False)([tf.cast(dim_input, tf.float64), tf.math.minimum(maximum_score, score_input)])
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

def print_debug(msg):
    print(datetime.now(), msg)

def main(argv):

    print_debug("load data for isotonic regression and bias tuning")
    impr_label, bias, cvr_dict, test_adgroups, model = load_data()

    print_debug("modify model embeddings for client test adgroups")
    # todo: this step is subject to change(may be removed in future) cuz mengxi's working on implementing the mapping in model
    model = modify_model_embeddings(model, test_adgroups)
    
    print_debug("1. calculate calibration based on impression and label for each adgroup")
    ag_calibration_logs,  ag_calibrated_cvr = calculate_calibration_adjustments(impr_label, bias, cvr_dict, test_adgroups)

    print_debug("save log and asset to local")
    ag_calibration_logs.to_csv(f"{FLAGS.calibrated_conversion_models_log}ag_calibration_type.csv", index=False)
    ag_calibrated_cvr.to_csv(FLAGS.asset_adgroup_lookup_score_path, index=False, header=False)

    print_debug("upload log to s3")
    log_s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model_log/calibrate_date={FLAGS.date}"
    s3_copy(FLAGS.calibrated_conversion_models_log, log_s3_output_path)

    print_debug("2. add calibration_layer to model")
    calibrated_model = add_calibration_layer(model)

    print_debug("save calibrated model to local")
    calibrated_model_path = f"{FLAGS.calibrated_conversion_models}grid_count={FLAGS.score_grid_count}"
    calibrated_model.save(calibrated_model_path)

    print_debug("upload calibrated model to s3")
    s3_output_path_tmp = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/temp_calibrated_conversion_model/1"
    s3_copy(calibrated_model_path, s3_output_path_tmp)
    # rename trick - don't copy it over for now because there's a manual step
    # s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/calibrated_conversion_model/{FLAGS.date}"
    # s3_move(s3_output_path_tmp, s3_output_path)

if __name__ == '__main__':
    # requires data and model in: input/offlineattribution, input/offlineattribution_cvr, input/client_test_adgroups, output/model,
    app.run(main)