import pandas as pd
import numpy as np
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

from Common.Logic.Preprocess import Preprocess
from Common.Logic.Postprocess import Postprocess
from Common.Logic.ChartClient import ChartClient
from Common.Setting.StoreCurrAnalysisSetting import *
from Common.Setting.Common.PreprocessSetting import *
from Common.util import Util
from Common.Setting.BasketAnalysisSetting import *



# 店舗の現状を把握する為のクラス
class BasketAnalysis:

    def __init__(self):
        self.preproc_s = PreprocessSetting()
        self.ba_s = BasketAnalysisSetting()
        self.preproc = Preprocess()
        self.postproc = Postprocess()
        self.gu = GroupingUnit()
        self.util = Util()
        self.chart_cli = ChartClient()

    def execute(self):
        preproc_csv_file_name = self._preprocess()
        # preproc_csv_file_name = ''
        self.df_preproc = self.preproc.fetch_csv_and_create_src_df(self.preproc_s.PROCESSED_DATA_DIR
                                                                   , [preproc_csv_file_name])
        self._basket_analysis()


    def _preprocess(self):
        df_src = self.preproc.common_proc(self.preproc_s)
        # df_src = self.preproc.grouping(df_src, self.preproc_s.GROUPING_KEY_DOW, self.preproc_s.GROUPING_WAY)
        df_src = self.preproc.tanspose_cols_and_rows(df_src, self.gu.DAY_BILL_ORDER,
                                                     self.preproc_s.TGT_TRANPOSE_C_AND_R_COL,
                                                     self.preproc_s.TRANPOSE_C_AND_R_COUNT_COL)

        # df_src = self.preproc.change_label_name(df_src)
        preproc_csv_file_name = self.preproc.create_proc_data_csv(df_src, self.preproc_s.PROCESSED_DATA_DIR,
                                                                  self.preproc_s.TGT_STORE,
                                                                  self.preproc_s.TGT_PERIOD_FLOOR,
                                                                  self.preproc_s.TGT_PERIOD_TOP,
                                                                  '_' + self.preproc_s.GROUPING_FILE_MEMO)

        return preproc_csv_file_name

    def _get_preproc_data(self, csv_file_name):
        return pd.read_csv(self.preproc_s.PROCESSED_DATA_DIR + csv_file_name, encoding='cp932')

    def _basket_analysis(self):
        basket = self.df_preproc.drop(columns=self.gu.DAY_BILL_ORDER)
        basket_sets = basket.applymap(self.encode_units)
        frequent_itemsets = apriori(basket_sets, min_support=0.05, use_colnames=True)
        rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)
        self.util.df_to_csv(rules,self.ba_s.OUTPUT_DIR,"商品のアソシエーション分析.csv")

    @staticmethod
    def encode_units(x):
        if x <= 0:
            return 0
        if x >= 1:
            return 1


if __name__ == '__main__':
    ba = BasketAnalysis()
    ba.execute()