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
        self._calc_avg_std(self.df_training_data)
        # self._create_model
        self._exec_sim()
        print(1)

    def _calc_avg_std(self, df):
        # 平均と標準偏差
        df_m_avg = df[['販売数']].mean().reset_index()
        df_m_std = df[['販売数']].std().reset_index()
        df_m_avg_std = pd.merge(df_m_avg.rename(columns={'販売数': 'μ'}), df_m_std.rename(columns={'販売数': 'σ'}))

        df_m_avg_std['μ+' + str(self.nd_s.a) + "σ"] = df_m_avg_std['μ'] + self.nd_s.a * df_m_avg_std['σ']
        df = pd.merge(df, df_m_avg_std)

        return df

    def _exec_sim(self):
        pass
