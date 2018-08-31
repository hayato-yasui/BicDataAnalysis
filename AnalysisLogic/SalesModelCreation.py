import os
import re
import pandas as pd
import numpy as np
import datetime
from sklearn import linear_model
import matplotlib.pyplot as plt
from sklearn import model_selection
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasRegressor

from keras.models import Sequential
from keras.layers import Dense, Activation
from keras import optimizers
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split

from Common.Logic.ChartClient import ChartClient
from Common.Logic.Preprocess import *
from Common.Logic.Postprocess import *
from Common.Setting.SalesModelCreationSetting import *
from Common.Setting.Common.PreprocessSetting import *
from Common.util import Util

# 重回帰分析クラス
class SalesModelCreation:
    clf = linear_model.LinearRegression()

    def __init__(self):
        self.preproc_s = PreprocessSetting()
        self.smc_s = SalesModelCreationSetting()
        self.preproc = Preprocess()
        self.postproc = Postprocess()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.chart_cli = ChartClient()
        self.gu = GroupingUnit()
        self.util = Util()

    def execute(self):
        self.df_preproc, preproc_csv_file_name = self._preprocess()
        # preproc_csv_file_name = ''
        # self.df_preproc = self.preproc.fetch_csv_and_create_src_df(self.preproc_s.PROCESSED_DATA_DIR
        #                                                            , [preproc_csv_file_name])
        self._create_prediction_model()
        # self._postprocess()

    def _preprocess(self):
        df_src = self.preproc.common_proc(self.preproc_s)
        df_item_pivot = self.preproc.tanspose_cols_and_rows(df_src, self.gu.DAY_BILL,
                                                            self.preproc_s.TGT_TRANPOSE_C_AND_R_COL,
                                                            self.preproc_s.TRANPOSE_C_AND_R_COUNT_COL)

        self.preproc.dt_min_round(df_src, '滞在時間', 20)
        df_src['客構成'] = self.preproc.create_cstm_strctr(df_src)
        df_src = self.mmt.merge_store(df_src, self.mmt_s.F_PATH_STORE)
        df_src = self.mmt.merge_weather(df_src, self.mmt_s.DIR_WEATHER, self.preproc_s.TGT_PERIOD_FLOOR,
                                        self.preproc_s.TGT_PERIOD_TOP)

        df_src = self.preproc.calc_entering_and_exiting_time(df_src)
        df_src = self.preproc.create_stay_presense(df_src, df_src.loc[0, '営業開始時間'], df_src.loc[0, '営業締め時間'])
        self.preproc.dt_min_round(df_src, '注文時間', 10)
        self.preproc.dt_min_round(df_src, '滞在時間', 20)
        df_grouped_by_bill = df_src.groupby(self.gu.BILL).max().reset_index()
        df_grouped_by_bill = pd.merge(df_grouped_by_bill, df_item_pivot)
        # df_src = self.preproc.change_label_name(df_src)
        # preproc_csv_file_name = self.preproc.create_proc_data_csv(df_src, self.preproc_s.PROCESSED_DATA_DIR,
        #                                                           self.preproc_s.TGT_STORE,
        #                                                           self.preproc_s.TGT_PERIOD_FLOOR,
        #                                                           self.preproc_s.TGT_PERIOD_TOP,
        #                                                           '_' + self.preproc_s.GROUPING_FILE_MEMO)
        preproc_csv_file_name = None
        return df_grouped_by_bill, preproc_csv_file_name

    def _get_preproc_data(self, csv_file_name):
        return pd.read_csv(self.preproc_s.PROCESSED_DATA_DIR + csv_file_name, encoding='cp932')

    def _create_prediction_model(self):
        self.df_preproc.drop(columns=['H.集計対象営業年月日', 'H.伝票番号','H.伝票発行日','H.伝票処理日','滞在時間','D.価格','注文時間','D.オーダー日時'], inplace=True)
        self.preproc.replace_missing_value(self.df_preproc)
        # self.df_preproc['滞在時間'] = (self.df_preproc['滞在時間'] / np.timedelta64(1, 'M')).astype(int)
        X, y = self.util.create_prd_and_obj_df_or_values(self.df_preproc, 'H.伝票金額','values',does_replace_dummy=True)
        X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=.2, random_state=0)
        self._standardization(X_train)
        print(X_train)
        # self._create_model(X_train, X_test, y_train, y_test)
        n_hidden = 80  # 出力次元
        epochs = 100  # エポック数
        batch_size = 10  # ミニバッチサイズ

        # モデル定義
        prediction = Prediction(maxlen, n_hidden, n_in, n_out)
        # 学習
        model = prediction.train(x_train, t_train, batch_size, epochs)
        # テスト
        score = model.evaluate(x_test, t_test, batch_size=batch_size, verbose=1)
        print("score:", score)

        # 正答率、準正答率（騰落）集計
        preds = model.predict(x_test)
        correct = 0
        semi_correct = 0
        for i in range(len(preds)):
            pred = np.argmax(preds[i, :])
            tar = np.argmax(t_test[i, :])
            if pred == tar:
                correct += 1
            else:
                if pred + tar == 1 or pred + tar == 5:
                    semi_correct += 1

        print("正答率:", 1.0 * correct / len(preds))
        print("準正答率（騰落）:", 1.0 * (correct + semi_correct) / len(preds))


    def _standardization(self, X_train):
        scaler = StandardScaler()
        scaler.fit(X_train)

    #
    # def _create_model(self, X_train, X_test, y_train, y_test):
    #     pass


    # def _create_model(self, X_train, X_test, y_train, y_test):
    #     # モデルの取得
    #     model = Sequential()
    #     model.add(Dense(10, input_dim=X_train.shape[1], activation='relu'))
    #     model.add(Dense(16, activation='relu'))
    #     model.add(Dense(1,activation='sigmoid'))
    #     model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])
    #     model.compile(loss='binary_crossentropy',
    #                   optimizer=optimizers.SGD(lr=0.01),
    #                   metrics=['accuracy', ])
    #     model.summary()
    #
    #     # Training
    #     model.fit(X_train, y_train, batch_size=200, epochs=200)
    #
    #     # スコア（参考値）
    #     model.evaluate(X_test, y_test)
    #     # model.score(X_test, y_test)
    #
    #     # 予測値の取得
    #     X_positive_new = np.random.normal(loc=1.0, scale=1.0, size=(5, X_train.shape[1]))
    #     X_negative_new = np.random.normal(loc=-1.0, scale=1.0, size=(5, X_train.shape[1]))
    #     print(model.predict(X_positive_new))
    #     print(model.predict(X_negative_new))
    #     # y_pred = model.predict(X_test)

class Prediction :
  def __init__(self, maxlen, n_hidden, n_in, n_out):
    self.maxlen = maxlen
    self.n_hidden = n_hidden
    self.n_in = n_in
    self.n_out = n_out

  def create_model(self):
    model = Sequential()
    model.add(LSTM(self.n_hidden, batch_input_shape = (None, self.maxlen, self.n_in),
             kernel_initializer = glorot_uniform(seed=20170719),
             recurrent_initializer = orthogonal(gain=1.0, seed=20170719),
             dropout = 0.5,
             recurrent_dropout = 0.5))
    model.add(Dropout(0.5))
    model.add(Dense(self.n_out,
            kernel_initializer = glorot_uniform(seed=20170719)))
    model.add(Activation("softmax"))
    model.compile(loss="categorical_crossentropy", optimizer = "RMSprop", metrics = ['categorical_accuracy'])
    return model

  # 学習
  def train(self, x_train, t_train, batch_size, epochs) :
    early_stopping = EarlyStopping(patience=0, verbose=1)
    model = self.create_model()
    model.fit(x_train, t_train, batch_size = batch_size, epochs = epochs, verbose = 1,
          shuffle = True, callbacks = [early_stopping], validation_split = 0.1)
    return model



if __name__ == '__main__':
    smr = SalesModelCreation()
    smr.execute()
