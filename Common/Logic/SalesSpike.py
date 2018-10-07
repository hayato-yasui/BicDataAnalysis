import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.BicEC_ShortageAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class SalesSpike:
    def __init__(self, df_src, index_li, amount_col='販売数', a=2, does_output_csv=True, dir=None,file_name=None,does_calc_weight=None):
        # self.sql_cli = SQLServerClient()
        self.util = Util()
        self.bsa_s = BicECShortageAnalysisSetting()
        # self.mmt = MergeMasterTable()
        # self.mmt_s = MergeMasterTableSetting()
        self.df_src = df_src
        self.index_li = index_li
        self.amount_col = amount_col
        self.a = a
        self.does_output_csv = does_output_csv
        self.dir = dir
        self.file_name = file_name
        self.does_calc_weight = does_calc_weight


    def execute(self):
        df_sales_spike_by_item = self._extract_sales_spike()
        if self.does_output_csv:
            self.util.df_to_csv(df_sales_spike_by_item,self.dir,self.file_name)
        return df_sales_spike_by_item

    def _preprocess(self):
        pass

    def _extract_sales_spike(self):
        df_sales_spike_by_item = self._calc_spike_day(self.df_src)
        return df_sales_spike_by_item

    def _calc_spike_day(self, df):
        df_m = df.groupby(self.index_li)

        # 平均と標準偏差
        df_m_avg = df_m[[self.amount_col]].mean().reset_index()
        df_m_std = df_m[[self.amount_col]].std().reset_index()
        df_m_avg_std = pd.merge(df_m_avg.rename(columns={self.amount_col: 'μ'}),
                                df_m_std.rename(columns={self.amount_col: 'σ'}))

        df_m_avg_std['μ+' + str(self.a) + "σ"] = df_m_avg_std['μ'] + self.a * df_m_avg_std['σ']
        df = pd.merge(df, df_m_avg_std)
        if df.empty:
            return
        df['スパイク'] = df.apply(
            lambda x: 0 if x[self.amount_col] < x["μ+" + str(self.a) + "σ"] or x[self.amount_col] == 0 else 1, axis=1)
        if self.does_calc_weight:
            df['スパイク度合'] = df['スパイク'] * df['μ']
        return df
