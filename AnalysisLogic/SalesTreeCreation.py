import os
import re
import pandas as pd
import numpy as np
import datetime
from sklearn import linear_model
import matplotlib.pyplot as plt
from sklearn import model_selection
from sklearn.preprocessing import StandardScale
from sklearn.metrics import mean_squared_error
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasRegressor

from keras.models import Sequential
from keras.layers import Dense, Activation
from keras import optimizers
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split


from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

from sklearn.metrics import (roc_curve, auc, accuracy_score)
import pydotplus as pdp
from sklearn import tree
# from IPython.display import Image
from graphviz import Digraph
from sklearn.externals.six import StringIO


from Common.Logic.ChartClient import ChartClient
from Common.Logic.Preprocess import *
from Common.Logic.Postprocess import *
from Common.Setting.SalesTreeCreationSetting import *
from Common.Setting.Common.PreprocessSetting import *
from Common.util import Util


# 重回帰分析クラス
class SalesTreeCreation:
    # clf = DecisionTreeClassifier(random_state=0)
    clf = RandomForestClassifier(n_estimators=150)

    def __init__(self):
        self.preproc_s = PreprocessSetting()
        self.smc_s = SalesTreeCreationSetting()
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
        self._create_decision_tree()
        # self._postprocess()

    def _preprocess(self):
        df_src = self.preproc.common_proc(self.preproc_s)
        df_item_pivot = self.preproc.tanspose_cols_and_rows(df_src, self.gu.DAY_BILL,
                                                            self.preproc_s.TGT_TRANPOSE_C_AND_R_COL,
                                                            self.preproc_s.TRANPOSE_C_AND_R_COUNT_COL)

        self.preproc.dt_min_round(df_src, '滞在時間', 20)
        df_src['客構成'] = self.preproc.create_cstm_strctr(df_src)
        df_grouped_by_bill = self.preproc.grouping(df_src, self.gu.DAY_BILL, self.preproc_s.GROUPING_WAY_BY_BILL)
        df_grouped_by_bill = pd.merge(df_grouped_by_bill, df_item_pivot)
        # df_src = self.preproc.change_label_name(df_src)
        preproc_csv_file_name = self.preproc.create_proc_data_csv(df_grouped_by_bill, self.preproc_s.PROCESSED_DATA_DIR,
                                                                  self.preproc_s.TGT_STORE,
                                                                  self.preproc_s.TGT_PERIOD_FLOOR,
                                                                  self.preproc_s.TGT_PERIOD_TOP,
                                                                  '_' + self.preproc_s.GROUPING_FILE_MEMO)

        return df_grouped_by_bill, preproc_csv_file_name

    def _get_preproc_data(self, csv_file_name):
        return pd.read_csv(self.preproc_s.PROCESSED_DATA_DIR + csv_file_name, encoding='cp932')

    def _create_decision_tree(self):
        self.df_preproc.drop(columns=['H.集計対象営業年月日', 'H.伝票番号','H.伝票発行日'], inplace=True)
        self.df_preproc['滞在時間'] = (self.df_preproc['滞在時間'] / np.timedelta64(1, 'M')).astype(int)
        self.preproc.replace_missing_value(self.df_preproc)
        X, y = self._create_prd_and_obj_valiables(self.df_preproc, 'H.伝票金額')
        X = pd.get_dummies(X)
        df_train, df_test, label_train, label_test = train_test_split(X, y)

        self.clf.fit(df_train, label_train)
        print("========================================================")
        print("予測の精度")
        print(self.clf.score(df_test, label_test))

        estimators = self.clf.estimators_
        file_name = "./data/sample.pdf"
        dot_data = tree.export_graphviz(estimators[0],  # 決定木オブジェクトを一つ指定する
                                        out_file=None,  # ファイルは介さずにGraphvizにdot言語データを渡すのでNone
                                        filled=True,  # Trueにすると、分岐の際にどちらのノードに多く分類されたのか色で示してくれる
                                        rounded=True,  # Trueにすると、ノードの角を丸く描画する。
                                        feature_names=X.columns,  # これを指定しないとチャート上で特徴量の名前が表示されない
                                        # class_names=iris.target_names,  # これを指定しないとチャート上で分類名が表示されない
                                        special_characters=True  # 特殊文字を扱えるようにする
                                        )
        graph = pdp.graph_from_dot_data(dot_data)
        graph.write_pdf(file_name)

    def _standardization(self, X_train):
        scaler = StandardScaler()
        scaler.fit(X_train)

    def _create_prd_and_obj_valiables(self, df, Y_col):
        # X = df.drop(Y_col, axis=1).values
        # y = df[Y_col].values
        X = df.drop(Y_col, axis=1)
        y = df[Y_col]
        return X, y

    def _create_model(self, X_train, X_test, y_train, y_test):
        # モデルの取得
        model = Sequential()
        model.add(Dense(10, input_dim=X_train.shape[1], activation='relu'))
        model.add(Dense(16, activation='relu'))
        model.add(Dense(1,activation='sigmoid'))
        model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])
        model.compile(loss='binary_crossentropy',
                      optimizer=optimizers.SGD(lr=0.01),
                      metrics=['accuracy', ])
        model.summary()

        # Training
        model.fit(X_train, y_train, batch_size=200, epochs=200)

        # スコア（参考値）
        model.evaluate(X_test, y_test)
        # model.score(X_test, y_test)

        # 予測値の取得
        X_positive_new = np.random.normal(loc=1.0, scale=1.0, size=(5, X_train.shape[1]))
        X_negative_new = np.random.normal(loc=-1.0, scale=1.0, size=(5, X_train.shape[1]))
        print(model.predict(X_positive_new))
        print(model.predict(X_negative_new))
        # y_pred = model.predict(X_test)

        # # 二乗平方根で誤差を算出
        # mse = mean_squared_error(y_test, y_pred)
        # print("KERAS REG RMSE : %.2f" % (mse ** 0.5))
        #
        # # 可視化
        # pd.DataFrame({"pred": y_pred, "act": y_test})[:100].reset_index(drop=True).plot(figsize=(15, 4))
        #
        # # self.chart_cli.savefig(self.mra_s.OUTPUT_DIR, 'サンプル.png')
        # # self.chart_cli.plotfig()


if __name__ == '__main__':
    stc = SalesTreeCreation()
    stc.execute()
