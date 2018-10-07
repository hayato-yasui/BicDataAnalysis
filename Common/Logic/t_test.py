import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from Common.DB.SQLServer_Client import SQLServerClient
from AnalysisLogic.Resource.SQL.Rakuten_sql import *
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.Rakuten_PointUPAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class Ttest:
    def __init__(self, df_src, index_li, diff_tgt_col, diff_condition, does_output_csv=True, dir=None,file_name=None):
        # self.sql_cli = SQLServerClient()
        # self.sql_cli = None
        self.util = Util()
        # self.rpa_s = RakutenPointUPAnalysisSetting()
        # self.mmt = MergeMasterTable()
        # self.mmt_s = MergeMasterTableSetting()
        # self.df_shortage = self.df_master = self.df_tgt_item = None
        self.preproc = Preprocess
        self.df_src = df_src
        self.index_li = index_li
        self.diff_tgt_col = diff_tgt_col
        self.diff_condition = diff_condition
        self.does_output_csv = does_output_csv
        self.dir = dir
        self.file_name = file_name

    def execute(self):
        df_t_test_rslt = self.t_test()
        if self.does_output_csv:
            self.util.df_to_csv(df_t_test_rslt,self.dir,self.file_name)
        return

    def t_test(self):
        df_t_test_rslt = pd.DataFrame(
            columns=['item_cd', 'normal_count', 'normal_avg', 'special_count', 'special_avg', 't', 'p'])
        self.df_src.set_index(self.index_li, inplace=True)
        # for c in self.rpa_s.CALC_TGT_COLS:
        df_normal = self.df_src[self.df_src[self.diff_tgt_col] != self.diff_condition][self.diff_tgt_col]
        df_special = self.df_src[self.df_src[self.diff_tgt_col] != self.diff_condition][self.diff_tgt_col]

        for item in self.df_src.index.unique().tolist():
            # welch's t-test
            df_normal_by_item = df_normal[df_normal.index == item]
            df_special_by_item = df_special[df_special.index == item]
            t, p = stats.ttest_ind(df_normal_by_item, df_special_by_item, equal_var=False)
            df_t_test_rslt = df_t_test_rslt.append(pd.Series([item, df_normal_by_item.count(), df_normal_by_item.mean(),
                                                              df_special_by_item.count(), df_special_by_item.mean(), t, p],
                                                             index=df_t_test_rslt.columns),
                                                   ignore_index=True).sort_values('p')
        return df_t_test_rslt
