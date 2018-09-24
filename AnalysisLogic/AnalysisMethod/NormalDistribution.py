import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats

# from Common.DB.SQLServer_Client import SQLServerClient
# from Common.DB.sql import *
from Common.util import Util
from AnalysisLogic.AnalysisMethod.Setting.NormalDistributionSetting import *
from Common.Setting.SalesPredictionSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class NormalDistribution:
    def __init__(self, df_training_data, df_actual_data, df_inv, amount_col):
        # self.sql_cli = SQLServerClient()
        self.util = Util()
        self.nd_s = NormalDistributionSetting()
        self.sp_s = SalesPredictionSetting()
        self.df_training_data = df_training_data
        self.df_actual_sales = df_actual_data
        self.df_inv = df_inv
        self.amout_col = amount_col

    def execute(self):
        df_training_sales_tested, df_training_avg_std = self._preprocess()
        # self._create_model
        df_sim_rslt = self._exec_sim(df_training_sales_tested, df_training_avg_std)
        self.util.df_to_csv(df_sim_rslt,self.sp_s.OUTPUT_DIR,'sim結果_単純な正規分布.csv')

    def _preprocess(self):
        self.index_li = [c for c in self.df_training_data.columns.values.tolist() if
                         c not in ['日付', '販売数', '季節休日係数割戻販売数', '季節休日係数']]

        df_training_avg_std = self._calc_avg_std(self._grouped(self.df_training_data))
        self.df_training_data = pd.merge(self.df_training_data, df_training_avg_std)

        return self._test_normality(self._grouped(self.df_training_data)), df_training_avg_std

    def _grouped(self, df):
        return df.groupby(self.index_li)

    def _calc_avg_std(self, df_grouped, upper_date=None, floor_date=None):
        # 平均と標準偏差
        df_avg = df_grouped[['販売数']].mean().reset_index()
        df_std = df_grouped[['販売数']].std().reset_index()
        df_avg_std = pd.merge(df_avg.rename(columns={'販売数': 'μ'}), df_std.rename(columns={'販売数': 'σ'}))

        df_avg_std['μ+' + str(self.nd_s.a) + "σ"] = df_avg_std['μ'] + self.nd_s.a * df_avg_std['σ']
        df_avg_std['μ-' + str(self.nd_s.a) + "σ"] = df_avg_std['μ'] - self.nd_s.a * df_avg_std['σ']
        return df_avg_std

    def _test_normality(self, df_grouped, amount_col='販売数'):
        list_of_dfs = []
        for key, df in df_grouped:
            values_li = df[amount_col].tolist()
            W, p = stats.shapiro(values_li)
            df['W'] = W
            df['p'] = p
            df['正規分布'] = df.apply(lambda x: 1 if x['p'] <= self.nd_s.p_lower_limit else 0, axis=1)
            list_of_dfs.append(df)
        return pd.concat(list_of_dfs)

    def _exec_sim(self, df_sales_calced_avg_std, df_training_avg_std, amount_col='販売数'):
        df_sim_rslt = pd.merge(self.df_actual_sales, df_training_avg_std, on=self.index_li)
        # suffixes=['_actual', '_training'])

        df_sim_rslt['予測的中'] = df_sim_rslt.apply(lambda x: 1 if x['μ-' + str(self.nd_s.a) + "σ"] <= x[amount_col] <= x
            ['μ+' + str(self.nd_s.a) + "σ"] else 0, axis=1)
        return df_sim_rslt
