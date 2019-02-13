"""Functions to make it easier to work with H2O"""
import h2o
import numpy as np
import pandas as pd
from python_scripts.util import timeit, log

@timeit
def process_df(df : pd.DataFrame() = None, cols : list = None):
    if cols is not None:
        df = df[cols]

    cols_clip_min = {'cl_avg_lifetime': 0,
                     'rc_avg_day': 0,
                     'rc' : 0}

    print('Clipping min values.')
    # print(cols_clip_min)
    for col in cols_clip_min.keys():
        if col in df.columns:
            # print(col)
            # print(df[col].min())
            df[df[col] < cols_clip_min[col], col] = cols_clip_min[col]
            # print(df[col].min())

    cols_clip_max = {'cl_avg_lifetime': 9000,
                     'rc': 5000,
                     'rc_avg_day': 100,
                     'sms_tot_cnt': 3000,
                     'sum_ses_mou': 200000,
                     'mou': 10000,
                     'avg_day_voice_cnt': 1000,
                     'avg_day_mou': 10000,
                     'avg_day_mbou': 5000,
                     'sum_ses_mbou': 50000,
                     'avg_ses_mbou': 100,
                     'cl_onnet_cnt': 100,
                     'cl_size': 300}

    print('Clipping max values.')
    for col in cols_clip_max.keys():
        if col in df.columns:
            # print(col)
            # print(df[col].max())
            df[df[col] >= cols_clip_max[col], col] = cols_clip_max[col]
            # print(df[col].max())

    to_log_cols = ['avg_day_data_cnt', 'rc']
    print('Applying log transformation')
    for col in to_log_cols:
        if col in df.columns:
            df[col] = df[col].log1p()

    print('Applying special transformations')
    if 'io_voice_traf_ratio' in df.columns:
        df['io_voice_traf_ratio'] = df['io_voice_traf_ratio'] ** 0.5

    if 'add_sim_cnt' in df.columns:
        df[df['add_sim_cnt'] >= 5, 'add_sim_cnt'] = 5
        df['add_sim_cnt'] = df['add_sim_cnt'].asfactor()

    return df

@timeit
def generate_interaction_features(df, cols_to_use, feature):
    for col1 in cols_to_use:
        if col1 != feature:
            df[col1 + '_mult_' + feature] = df[col1] * df[feature]
            df[col1 + '_div_' + feature] = df[col1] / df[feature]
            df[df[feature] == 0, col1 + '_div_' + feature] = 0

    return df

@timeit
def select_features_by_importance(model, threshold=0.0001):
    """Select features based on tree importance."""


    vi = model.varimp(True)
    features = list(vi.loc[vi.scaled_importance > threshold, 'variable'].values)
    print(f'Selected {len(features)} features from {vi.shape[0]}.')
    return features

@timeit
def train_model(model='GradientBoosting', features=[], params={}, label='', train=h2o.H2OFrame(), valid=h2o.H2OFrame(), calibrate_model=False,
                calibration_frame=h2o.H2OFrame(), print_model=False, print_test_stats=False, test=h2o.H2OFrame()):
    """Train model and show results."""
    model_id = f'features_{len(features)}_' + '__'.join([f'{k}_{v}' for k, v in params.items()])
    if model == 'GradientBoosting':
        model = h2o.estimators.gbm.H2OGradientBoostingEstimator(**params, calibrate_model=calibrate_model, calibration_frame=calibration_frame,
                                                                model_id='gbm_'+ model_id)

    model.train(x=list(features), y=label, training_frame=train, validation_frame=valid)

    if print_model:
        print(model.show())
    if print_test_stats:
        print_test_stat(test, model, model.type)

    return model

@timeit
def print_test_stat(test_data=h2o.H2OFrame(), model=h2o.model, type='classifier'):
    """Print model statistics for test data."""
    if type == 'classifier':
        threshold = model.find_threshold_by_max_metric('f1', valid=True)
        print('threshold', threshold)
        m = model.model_performance(test_data)
        print('F1: {0:.4f}.'.format(m.F1([threshold])[0][1]))
        print('ROCAUC: {0:.4f}.'.format(m.auc()))
        print('Precision: {0:.4f}.'.format(m.precision([threshold])[0][1]))
        print('Recall: {0:.4f}.'.format(m.recall([threshold])[0][1]))
        print('Accuracy: {0:.4f}.'.format(m.accuracy([threshold])[0][1]))
#! rewrite!!! or just rewrite the whole script into class

@timeit
def log_training(model, parameters=['ntrees', 'max_depth', 'min_rows', 'learn_rate', 'balance_classes',
                                                           'fold_assignment', 'nfolds', 'col_sample_rate', 'sample_rate',
                                                           'class_sampling_factors', 'seed'],
                 metric='classification_error', log_file_name='log.txt',
                 plot_metric=False, print_log=False):
    """
    Logging models data.
    
    Writes model parameters and metric into log file
    """
    print('Writing...')
    actual_parameters = {k: model.actual_params[k] for k in model.actual_params.keys() if k in parameters}
    
    train_metric_name = 'training_' + metric
    valid_metric_name = 'validation_' + metric

    train_metric = model.score_history()[train_metric_name].values[-1]
    valid_metric = model.score_history()[valid_metric_name].values[-1]
    
    features = ', '.join(model.varimp(True)['variable'].values)
    
    string_to_write = f"{gbm_classifier_test.model_id}\t{actual_parameters}\t{features}\t{train_metric:.4f}\t{valid_metric:.4f}\n"
    
    if plot_metric:
        plt.plot(model.score_history()[train_metric_name].values, label=train_metric_name);
        plt.plot(model.score_history()[valid_metric_name].values, label=valid_metric_name);
        plt.legend();
        plt.show();
        
    if not os.path.exists(log_file_name):
        with open(log_file_name, 'w') as f:
            f.write('Model_id' + '\t' + 'Model_parameters' + '\t' + 'Variables' + train_metric_name.capitalize() + '\t' + valid_metric_name.capitalize() + '\n')
            f.write(string_to_write)
    else:
        with open(log_file_name, 'a') as f:
            f.write(string_to_write)
    
    if print_log:
        print('Model_id:', gbm_classifier_test.model_id)
        print('Actual parameters:', actual_parameters)
        print('Features:', features)
        print(f'{train_metric_name}: {train_metric}')
        print(f'{valid_metric_name}: {valid_metric}')
    print('Done!')

@timeit
def prepare_data(data, n_sample, hidden_size):
    data['target'] = -1
    data[data['hashed'] != '\\N', 'target'] = 1

    data['target'] = data['target'].asfactor()
    df = data[data['target'] == '-1'][:n_sample, :].rbind(data[data['target'] == '1'])

    # orig_target = df['target']

    target_1_len = df[df['target'] == '1'].shape[0]
    rand_ind = np.random.randint(0, target_1_len, hidden_size)

    data_1 = data[data['target'] == '1']
    data_1[list(rand_ind), 'target'] = '-1'
    df1 = data[data['target'] == '-1'][:n_sample, :].rbind(data_1)

    features = df1.columns[3:-1]

    return data, df, df1, features

@timeit
def first_step(model, df1, features):

    model.train(x=list(features), y='target', training_frame=df1)

    pred = model.predict(df1)
    df1['target'] = df1['target'].asnumeric()
    df1['ys'] = df1['target'] * 2 - 1
    df1 = df1.cbind(pred['p1'])
    max_prob = df1[df1['ys'] > 0, 'p1'].max()
    min_prob = df1[df1['ys'] > 0, 'p1'].min()
    df1[(df1['ys'] < 0) & (df1['p1'] > max_prob), 'ys'] = 1
    df1[(df1['ys'] < 0) & (df1['p1'] < min_prob), 'ys'] = 0
    df1['ys'] = df1['ys'].asfactor()

    return df1

@timeit
def second_step(model, df1, features):
    df1['ys'] = df1['ys'].asfactor()
    model.train(x=list(features), y='ys', training_frame=df1)
    # df_show = orig_target.cbind(df1['target']).cbind(pred['p1'])

    pred = model.predict(df1)['p1']
    df1['p1'] = pred
    df1['ys'] = df1['ys'].asnumeric()
    # print('p1' in df1.columns)
    max_prob = df1[df1['ys'] > 0, 'p1'].max()
    min_prob = df1[df1['ys'] > 0, 'p1'].min()
    df1[(df1['ys'] < 0) & (df1['p1'] > max_prob), 'ys'] = 1
    df1[(df1['ys'] < 0) & (df1['p1'] < min_prob), 'ys'] = 0

    df1['ys'] = df1['ys'].asfactor()

    return df1

@timeit
def run_two_step_model(model, df1, features):

    df1 = first_step(model, df1, features)
    for _ in range(10):
        df2 = second_step(model, df1, features)
        if df2['ys'] == df1['ys']:
            print('Finished')
            break
        else:
            df1 = df2.copy()

    return df1

def cumulative_gain_curve(y_true, y_score, pos_label=None):
    """This function generates the points necessary to plot the Cumulative Gain
    Note: This implementation is restricted to the binary classification task.
    Args:
        y_true (array-like, shape (n_samples)): True labels of the data.
        y_score (array-like, shape (n_samples)): Target scores, can either be
            probability estimates of the positive class, confidence values, or
            non-thresholded measure of decisions (as returned by
            decision_function on some classifiers).
        pos_label (int or str, default=None): Label considered as positive and
            others are considered negative
    Returns:
        percentages (numpy.ndarray): An array containing the X-axis values for
            plotting the Cumulative Gains chart.
        gains (numpy.ndarray): An array containing the Y-axis values for one
            curve of the Cumulative Gains chart.
    Raises:
        ValueError: If `y_true` is not composed of 2 classes. The Cumulative
            Gain Chart is only relevant in binary classification.
    """
    y_true, y_score = np.asarray(y_true), np.asarray(y_score)

    # ensure binary classification if pos_label is not specified
    classes = np.unique(y_true)
    if (pos_label is None and
        not (np.array_equal(classes, [0, 1]) or
             np.array_equal(classes, [-1, 1]) or
             np.array_equal(classes, [0]) or
             np.array_equal(classes, [-1]) or
             np.array_equal(classes, [1]))):
        raise ValueError("Data is not binary and pos_label is not specified")
    elif pos_label is None:
        pos_label = 1.

    # make y_true a boolean vector
    y_true = (y_true == pos_label)

    sorted_indices = np.argsort(y_score)[::-1]
    y_true = y_true[sorted_indices]
    gains = np.cumsum(y_true)

    percentages = np.arange(start=1, stop=len(y_true) + 1)

    gains = gains / float(np.sum(y_true))
    percentages = percentages / float(len(y_true))

    gains = np.insert(gains, 0, [0])
    percentages = np.insert(percentages, 0, [0])

    return percentages, gains