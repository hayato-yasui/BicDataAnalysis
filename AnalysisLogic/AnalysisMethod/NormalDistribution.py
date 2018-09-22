import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# from Common.DB.SQLServer_Client import SQLServerClient
# from Common.DB.sql import *
from Common.util import Util
from AnalysisLogic.AnalysisMethod.Setting.NormalDistributionSetting import *
from Common.Setting.SalesPredictionSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class NormalDistribution:
    def __init__(self, df_training_data, df_actual_sales, df_inv):
        # self.sql_cli = SQLServerClient()
        # self.util = Util()
        self.nd_s = NormalDistributionSetting()
        self.sp_s = SalesPredictionSetting()
        self.df_training_data = df_training_data
        self.df_actual_sales = df_actual_sales
        self.df_inv = df_inv

    def execute(self):
        df_grouped = self._grouped()
        self._calc_avg_std(self.df_training_data)
        # self._create_model
        self._exec_sim()
        print(1)

    def _grouped(self, df):
        index_li = df.columns.values.tolist()
        index_li.remove('販売数')
        return df.groupby(index_li)

    def _calc_avg_std(self, df_grouped, upper_date=None, floor_date=None):
        # 平均と標準偏差
        df_avg = df_grouped[['販売数']].mean().reset_index()
        df_std = df_grouped[['販売数']].std().reset_index()
        df_avg_std = pd.merge(df_avg.rename(columns={'販売数': 'μ'}), df_std.rename(columns={'販売数': 'σ'}))

        df_avg_std['μ+' + str(self.nd_s.a) + "σ"] = df_avg_std['μ'] + self.nd_s.a * df_avg_std['σ']
        df_avg_std['μ-' + str(self.nd_s.a) + "σ"] = df_avg_std['μ'] - self.nd_s.a * df_avg_std['σ']
        df = pd.merge(df_grouped, df_avg_std)

        return df

    def _exec_sim(self):
        pass
