import os
import re
import pandas as pd
import numpy as np
import datetime
from enum import Enum, IntEnum
from sklearn import linear_model
import matplotlib.pyplot as plt

from Common.Logic.ChartClient import ChartClient
from Common.Logic.Preprocess import *
from Common.Logic.Postprocess import *
from Common.Setting.MultipleRegressionAnalysis import *
from Common.Setting.Common.PreprocessSetting import *
from Common.util import Util

# class PLAYER_ANSWER(Enum):
#     MIN = 'MIN'
#     MAX = 'MAX'
#     MID = 'MID'
#     PASS = 'PASS'


# 重回帰分析クラス
class MultipleRegressionAnalysis:
    clf = linear_model.LinearRegression()

    def __init__(self):
        self.preproc_s = PreprocessSetting()
        self.mra_s = MultipleRegressionAnalysisSetting()
        self.preproc = Preprocess()
        self.postproc = Postprocess()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.chart_cli = ChartClient()
        self.gu = GroupingUnit()
        self.util = Util()

    def execute(self):
        self.df_preproc,preproc_csv_file_name = self._preprocess()
        # preproc_csv_file_name = ''
        # self.df_preproc = self.preproc.fetch_csv_and_create_src_df(self.preproc_s.PROCESSED_DATA_DIR
        #                                                            , [preproc_csv_file_name])
        corr = self._calc_correlation(self.df_preproc)
        # corr.replace(np.nan, 0, inplace=True)
        # corr.replace([np.nan,np.inf, -np.inf], 0, inplace=True)
        print(corr)
        self._create_prediction_model()
        # self._postprocess()

    def _preprocess(self):
        df_src = self.preproc.common_proc(self.preproc_s)
        preproc_csv_file_name = self.preproc.create_proc_data_csv(df_src, self.preproc_s.PROCESSED_DATA_DIR,
                                                                  self.preproc_s.TGT_STORE,
                                                                  self.preproc_s.TGT_PERIOD_FLOOR,
                                                                  self.preproc_s.TGT_PERIOD_TOP,
                                                                  memo='_before_grouping')

        df_src = self.mmt.merge_store(df_src, self.mmt_s.F_PATH_STORE)
        df_src = self.mmt.merge_weather(df_src, self.mmt_s.DIR_WEATHER, self.preproc_s.TGT_PERIOD_FLOOR,
                                        self.preproc_s.TGT_PERIOD_TOP)

        # Do grouping though, no change df name due to being able to skip those process
        # df_src = self.preproc.grouping(df_src, self.gu.DOW, self.preproc_s.GROUPING_WAY)
        preproc_csv_file_name = self.preproc.create_proc_data_csv(df_src, self.preproc_s.PROCESSED_DATA_DIR,
                                                                  self.preproc_s.TGT_STORE,
                                                                  self.preproc_s.TGT_PERIOD_FLOOR,
                                                                  self.preproc_s.TGT_PERIOD_TOP,
                                                                  '_' + self.preproc_s.GROUPING_FILE_MEMO)

        return df_src,preproc_csv_file_name

    def _get_preproc_data(self, csv_file_name):
        return pd.read_csv(self.preproc_s.PROCESSED_DATA_DIR + csv_file_name, encoding='cp932')

    # for debug
    def _calc_correlation(self, df_preproc):
        return df_preproc.corr(method='pearson').replace([np.nan,np.inf, -np.inf], 0, inplace=True)

    def _create_prediction_model(self):
        self.df_sales_high_corr = self._del_lower_corr_cols()
        self._normalization()
        X, Y = self._create_prd_and_obj_valiables()
        self._create_model(X, Y)

    def _del_lower_corr_cols(self):
        # get columns that have high corr
        high_corr_cols_li = self.df_preproc.corr()[(self.df_preproc.corr()['D.価格'] >= self.mra_s.CORR_LIMIT) |
                                                   (self.df_preproc.corr()[
                                                        'D.価格'] <= -self.mra_s.CORR_LIMIT)].index.values
        return self.df_preproc[high_corr_cols_li]

    def _normalization(self):
        # データフレームの各列を正規化
        self.df_sales_high_corr = self.df_sales_high_corr.apply(lambda x: (x - np.mean(x)) / (np.max(x) - np.min(x)))
        self.df_sales_high_corr.head()

    def _create_prd_and_obj_valiables(self):
        # X = Predictor variable , Y = Objective variable
        # X = self.df_sales_high_corr.drop('売上', axis=1).as_matrix()
        # Y = self.df_sales_high_corr['売上'].as_matrix()
        X = self.df_sales_high_corr.drop('D.価格', axis=1).values
        Y = self.df_sales_high_corr['D.価格'].values
        return X, Y

    def _create_model(self, X, Y):
        self.clf.fit(X, Y)
        # 偏回帰係数
        # print(pd.DataFrame({"Name": self.df_sales_high_corr.drop('売上', axis=1).columns,
        #                     "Coefficients": self.clf.coef_}).sort_values(by='Coefficients'))
        print(pd.DataFrame({"Name": self.df_sales_high_corr.drop('価格', axis=1).columns,
                            "Coefficients": self.clf.coef_}).sort_values(by='Coefficients'))
        # 切片 (誤差)
        print(self.clf.intercept_)

        # 散布図
        plt.scatter(X, Y)

        # 回帰直線
        plt.plot(X, self.clf.predict(X))
        self.chart_cli.savefig(self.mra_s.OUTPUT_DIR,'サンプル.png')
        self.chart_cli.plotfig()



if __name__ == '__main__':
    mra = MultipleRegressionAnalysis()
    mra.execute()
