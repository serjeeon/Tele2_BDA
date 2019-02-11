import h2o
import numpy as np
import configparser
import pandas as pd
from python_scripts.util import timeit, log
from python_scripts.h2o_functions import process_df, cumulative_gain_curve
import matplotlib.pyplot as plt

import time
import plotly.graph_objs as go
import plotly
from plotly.offline import plot
plotly.tools.set_credentials_file(username='Artgor', api_key='7EijYGtA6vckYmWU2IzB')


class LookALiker(object):
    """
    Look a like model.

    This class implements look alike model in H2O.
    At first h2o connection is established.
    Then data is loaded from Hive table.1
    Then data is sampled and prepared for training.
    Then selected model is run.
    After it final model is run and could be optimized.
    Predictions are made and saved on hdfs.
    """
    def __init__(self, ip: str = '', port: str = '', settings_file_name: str = 'settings.ini'):
        """Init."""
        self.config = configparser.ConfigParser()
        self.config.read(settings_file_name, encoding='utf-8')
        for key, value in self.config['MAIN'].items():
            setattr(self, key, value)
        h2o.connect(ip=ip, port=port, auth=(self.login, self.password), verbose=False)
        h2o.no_progress()

    @timeit
    def load_data_hive(self, table: str = '', col_names_df: pd.DataFrame() = None, col_names_list: list = []):
        """
        Load data from hive table into H2O dataframe.

       :param table: path to table on hive.
              example: hdfs://T2-HDFS-HA-PROD/user/hive/warehouse/developers.db/al_for_segments_short
       :param col_names_df: dataframe of column names
       :param col_names_list: list of column names
       :return: loaded H2O Frame
        """
        if col_names_df is not None:
            col_names = pd.read_csv(col_names_df)['col_name'].tolist()
        elif col_names_list != []:
            col_names = col_names_list
        else:
            raise ValueError('Column names are not defined!')

        self.data = h2o.import_file(path=table, destination_frame='df', col_names=col_names)

    @timeit
    def prepare_data(self, target: str = None, to_target: str = None, n_sample: int = 10, hidden_size: int = 5,
                     process: bool = True, rass=False):
        """
        Prepare data for model.

       :param target:
       :param to_target:
       :param n_sample:
       :param hidden_size:
       :param process:
       :param rass:
       :return:
        """
        self.hidden_size = hidden_size

        if not rass:
            if target is not None:
                if target in self.data.columns:
                    pass
                else:
                    raise ValueError(f'{target} column not found!')

            elif to_target is not None:
                self.data['target'] = 0
                self.data[self.data[to_target] != '\\N', 'target'] = 1

            else:
                raise ValueError('Target column not defined!')

            self.data['target'] = self.data['target'].asfactor()

            if hidden_size == 0:
                # sample data from negative class
                print('Model will be trained without validation.')
                df = self.data[self.data['target'] == '0'][:n_sample, :].rbind(self.data[self.data['target'] == '1'])

            else:
                # random sampling of positive, the rest is validation.
                df_ = self.data[self.data['target'] == '0'][:n_sample, :].rbind(self.data[self.data['target'] == '1'])
                self.orig_target = df_['target']
                print('Doing random sampling')
                target_1_len = self.data[self.data['target'] == '1'].shape[0]
                rand_ind = np.random.choice(range(target_1_len), hidden_size, replace=False)

                data_1 = self.data[self.data['target'] == '1']
                data_1[list(rand_ind), 'target'] = '0'
                df = self.data[self.data['target'] == '0'][:n_sample, :].rbind(data_1)

        else:
            self.data['target'] = self.data['target'].set_levels(['0', '1', '-1'])

            if hidden_size == 0:
                # sample data from negative class
                print('Model will be trained without validation.')
                df = self.data[self.data['target'] == '-1'][:n_sample, :].rbind(self.data[self.data['target'] == '0']).rbind(self.data[self.data['target'] == '1'])

            else:
                # random sampling of positive, the rest is validation.
                df_ = self.data[self.data['target'] == '-1'][:n_sample, :].rbind(self.data[self.data['target'] == '0']).rbind(self.data[self.data['target'] == '1'])
                self.orig_target = df_['target']
                print('Doing random sampling')
                target_1_len = self.data[self.data['target'] == '1'].shape[0]
                rand_ind = np.random.choice(range(target_1_len), hidden_size, replace=False)

                data_1 = self.data[self.data['target'] == '1']
                data_1[list(rand_ind), 'target'] = '-1'
                df = self.data[self.data['target'] == '-1'][:n_sample, :].rbind(data_1).rbind(self.data[self.data['target'] == '0'])

        self.features = df.columns[3:-1]

        if process:
            df = process_df(df)

        self.df = df
        self.orig_df = h2o.deep_copy(self.df, 'orig_df')

    @timeit
    def first_step(self):
        """


       :return:
        """
        df = h2o.deep_copy(self.df, 'df_')
        self.model.train(x=list(self.features), y='target', training_frame=df)

        pred = self.model.predict(df)
        df['target'] = df['target'].asnumeric()
        df['ys'] = df['target'] * 2 - 1
        df = df.cbind(pred['p1'])
        max_prob = df[df['ys'] > 0, 'p1'].max()
        min_prob = df[df['ys'] > 0, 'p1'].min()
        log('New positives: {0}.'.format(df[(df['ys'] < 0) & (df['p1'] > max_prob)].shape))
        df[(df['ys'] < 0) & (df['p1'] > max_prob), 'ys'] = 1
        log('New negatives: {0}.'.format(df[(df['ys'] < 0) & (df['p1'] < min_prob)].shape))
        df[(df['ys'] < 0) & (df['p1'] < min_prob), 'ys'] = 0
        df['ys'] = df['ys'].asfactor()
        self.df = df

    @timeit
    def second_step(self):
        # self.model = h2o.estimators.glm.H2OGeneralizedLinearEstimator(family='multinomial')
        df = h2o.deep_copy(self.df, 'df2')
        df['ys'] = df['ys'].asfactor()
        self.model.train(x=list(self.features), y='ys', training_frame=df)

        pred = self.model.predict(df)['p1']
        df['p1'] = pred
        df['ys'] = df['ys'].asnumeric()
        max_prob = df[df['ys'] > 0, 'p1'].max()
        min_prob = df[df['ys'] > 0, 'p1'].min()
        log('New positives: {0}.'.format(df[(df['ys'] < 0) & (df['p1'] > max_prob)].shape))
        df[(df['ys'] < 0) & (df['p1'] > max_prob), 'ys'] = 1
        log('New negatives: {0}.'.format(df[(df['ys'] < 0) & (df['p1'] < min_prob)].shape))
        df[(df['ys'] < 0) & (df['p1'] < min_prob), 'ys'] = 0

        df['ys'] = df['ys'].asfactor()

        return df

    @timeit
    def run_two_step_model(self, model=None, rass=False):
        if model is None:
            model = h2o.estimators.random_forest.H2ORandomForestEstimator(col_sample_rate_per_tree=0.9,
                                                                          ntrees=100,
                                                                          model_id='two_step_model')
            # model = h2o.estimators.glm.H2OGeneralizedLinearEstimator(family='binomial')
        self.model = model

        if not rass:
            self.first_step()
        else:
            self.df['ys'] = self.df['target']

        for _ in range(10):
            log(f'Step {_}. {time.ctime()}')
            df2 = self.second_step()
            if df2['ys'] == self.df['ys']:
                log('Finished')
                break
            else:
                self.df = h2o.deep_copy(df2, 'df')

        # self.df = df2[df2['ys'] != '-1']
        self.df = df2
        # #self.df['ys'] = self.df['ys'].ascharacter()
        # self.df['ys'] = self.df['ys'].asnumeric()
        # self.df['ys'] = self.df['ys'].ascharacter()
        # self.df = self.df[self.df['ys'] != '-1']
        # self.df['ys'] = self.df['ys'].asfactor()
        self.change_target()

    @timeit
    def change_target(self):
        # self.df['ys'] = self.df['ys'].ascharacter()
        self.df['ys'] = self.df['ys'].asnumeric()
        self.df['ys'] = self.df['ys'].ascharacter()
        self.df = self.df[self.df['ys'] != '-1']
        self.df['ys'] = self.df['ys'].asfactor()

    @timeit
    def train_full_model(self):
        self.df = self.df[self.df['ys'] != '-1']
        self.df['ys'] = self.df['ys'].asfactor()

        model2 = h2o.estimators.random_forest.H2ORandomForestEstimator(col_sample_rate_per_tree=0.9,
                                                                       ntrees=100,
                                                                       max_depth=20,
                                                                       model_id='rfe_' + '4')
        # model2 = h2o.estimators.glm.H2OGeneralizedLinearEstimator(family='multinomial')
        model2.train(x=list(self.features), y='ys', training_frame=self.df)
        # select features
        # gridsearch
        self.model2 = model2

    @timeit
    def predict(self):

        pr_big = self.model2.predict(self.data)
        pr_big = pr_big.sort('p1', ascending=False)
        df_short_ = pr_big[:1000000, :]
        df_short_ = df_short_['msisdn']
        h2o.export_file(df_short_, 'hdfs://T2-HDFS-HA-PROD/user/andrey.lukyanenko/exported.csv')

    @timeit
    def plot_hidded_validation(self):
        pred = self.model2.predict(self.orig_df)
        df_show = self.orig_target.cbind(self.orig_df['target']).cbind(pred['p1'])
        ts = range(100, self.hidden_size, 100)
        y_std = []
        y_ = []
        df_ = df_show[df_show['target0'] == '0'].sort('p1', ascending=False)
        goods = df_[df_['target'] == '1'].shape[0]
        for t in ts:
            df_small = df_.head(t)
            rate = df_small[df_small['target'] == '1'].shape[0] / df_small.shape[0]
            y_std.append(rate)
            y_.append(df_small[df_small['target'] == '1'].shape[0] / goods)

        # Performance graphing
        fig, ax1 = plt.subplots(figsize=(16, 12))
        plt.rcParams['font.size'] = 16
        plt.rcParams['figure.figsize'] = 15, 8
        ax1.plot(ts, y_std, lw=3)
        vals = plt.gca().get_yticks()
        plt.yticks(vals, ['%.0f%%' % (v * 100) for v in vals])
        plt.xlabel('Number of unlabeled data points chosen from the top rated')
        plt.ylabel('Percent of chosen that are secretly positive')
        # plt.legend('Standard classifier'])
        ylim = plt.gca().get_ylim()
        plt.title('Performance of model')
        plt.grid()
        ax2 = ax1.twinx()
        ax2.plot(
            ts, y_, color='r'
        )
        plt.show()

    @timeit
    def plot_lift(self):
        lift = self.model2.gains_lift().as_data_frame()

        pred = self.model2.predict(self.orig_df)
        df_show = self.orig_target.cbind(self.orig_df['target']).cbind(pred['p1'])

        pd_df = df_show[['p1', 'target']].as_data_frame()
        percentages, gains2 = cumulative_gain_curve(pd_df['target'].values, pd_df['p1'].values, 1)

        goods = df_show[(df_show['target'] == '1')].shape[0]
        df_show = df_show.sort('p1', ascending=False)
        y_s = []
        y_s1 = []
        tf1 = [i * df_show.shape[0] for i in lift.cumulative_data_fraction.values]
        tf1 = [int(np.round(i)) for i in tf1]
        for t in tf1:
            df__ = df_show[:t, :]
            df_small = df__[df__['target'] == '1']
            rate = df_small.shape[0] / goods
            y_s.append(rate)
            rate1 = df_small.shape[0] / t
            y_s1.append(rate1)
            del df_small
            del rate

        data_p = []
        data_p.append(go.Scatter(
            x=lift.cumulative_data_fraction.values,
            y=lift.cumulative_lift,
            name='Куммулятивный лифт',
            # line = dict(color = '#17BECF'),
            opacity=0.8))
        data_p.append(go.Scatter(
            x=lift.cumulative_data_fraction.values,
            y=y_s,
            name='Доля отобранных из заданного сегмента',
            # line = dict(color = '#17BECF'),
            opacity=0.8,
            yaxis='y2'))

        # data_p = [trace_high,trace_low]

        layout = dict(
            title="Data",
            # hovermode='closest',
            #     xaxis=dict(
            #         type='log'),
            yaxis=dict(
                title='Куммулятивный лифт'
            ),
            yaxis2=dict(
                title='Доля отобранных из заданного сегмента', overlaying='y',
                side='right'
            ),
        )
        # column_as_df = json_normalize(df[column])
        fig = dict(data=data_p, layout=layout)
        plot(fig)

    @timeit
    def make_bagging_model(self):
        raise NotImplementedError
