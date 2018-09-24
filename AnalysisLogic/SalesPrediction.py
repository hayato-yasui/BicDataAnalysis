import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.SalesPredictionSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *
from AnalysisLogic.AnalysisMethod.NormalDistribution import *


class SalesPrediction:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.sp_s = SalesPredictionSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.preprc = Preprocess

    def execute(self):
        df_training_data, df_actual_sales, df_inv = self._preprocess()
        df_training_data = pd.read_csv(self.sp_s.OUTPUT_DIR + '学習データ.csv', encoding='cp932', engine='python')
        df_actual_sales = pd.read_csv(self.sp_s.OUTPUT_DIR + '実販売数データ.csv', encoding='cp932', engine='python')
        df_inv = pd.read_csv(self.sp_s.OUTPUT_DIR + '在庫データ(全期間).csv', encoding='cp932', engine='python')

        # self._create_model
        self._exec_sim(df_training_data, df_actual_sales, df_inv)

    def _exec_sim(self, df_training_data, df_actual_sales, df_inv):
        if self.sp_s.ANALYSIS_METHOD == 'normal_distribution':
            self.nd = NormalDistribution(df_training_data, df_actual_sales, df_inv, "販売数")
            self.nd.execute()
        else:
            pass
        return

    def _preprocess(self):
        self.df_tgt_item = pd.read_csv('./data/Input/tgt_itm_cd/sim/24item.csv', encoding='cp932', engine='python')
        self.df_tgt_item = self.preprc.adjust_0_filled(self.df_tgt_item)
        df_tgt_item_info = self.util.extract_tgt_itm_info(self.sql_cli, self.df_tgt_item['item_cd'].tolist(),
                                                          self.sp_s.TGT_UPPER_DATE)
        self.df_tgt_item = pd.merge(self.df_tgt_item, df_tgt_item_info)

        df_training_sales_by_chanel = self._fetch_sales_by_item(self.df_tgt_item['item_cd'].tolist(),
                                                                self.sp_s.TRAINING_FLOOR_DATE,
                                                                self.sp_s.TRAINING_UPPTER_DATE)
        df_training_sales_by_chanel = self.mmt.merge_chanel(df_training_sales_by_chanel)
        df_actual_sales_by_chanel = self._fetch_sales_by_item(self.df_tgt_item['item_cd'].tolist(),
                                                              self.sp_s.TGT_FLOOR_DATE,
                                                              self.sp_s.TGT_UPPER_DATE)
        df_actual_sales_by_chanel = self.mmt.merge_chanel(df_actual_sales_by_chanel)

        # Grouping by Chanel Group
        df_training_sales = self.preprc.merge_sales_by_chanel_group(df_training_sales_by_chanel, '販売数')
        df_actual_sales = self.preprc.merge_sales_by_chanel_group(df_actual_sales_by_chanel, '販売数')

        # 学習期間 + シミュレーション期間の在庫データを取得
        df_inv = self.util.select_ec_inv_by_item(self.sql_cli, self.df_tgt_item, self.sp_s.TRAINING_FLOOR_DATE,
                                                 self.sp_s.TGT_UPPER_DATE)

        # 算出済の季節休日係数を取得し、販売数を割り戻す
        df_f_weekend_season = self.util.select_calced_week_season_factor(
            self.df_tgt_item, self.sql_cli, self.sp_s.TRAINING_FLOOR_DATE, self.sp_s.TGT_UPPER_DATE)

        # get dept cd to merge factor
        df_training_sales = pd.merge(df_training_sales, df_tgt_item_info, on='item_cd')
        df_actual_sales = pd.merge(df_actual_sales, df_tgt_item_info, on='item_cd')

        df_training_sales = pd.merge(df_training_sales, df_f_weekend_season, how='left')
        df_actual_sales = pd.merge(df_actual_sales, df_f_weekend_season, how='left')
        df_training_sales['季節休日係数割戻販売数'] = (df_training_sales['販売数'] // df_training_sales['季節休日係数']).round()
        df_actual_sales['季節休日係数割戻販売数'] = (df_actual_sales['販売数'] // df_actual_sales['季節休日係数']).round()

        self.util.df_to_csv(df_training_sales, self.sp_s.OUTPUT_DIR, '学習データ.csv')
        self.util.df_to_csv(df_actual_sales, self.sp_s.OUTPUT_DIR, '実販売数データ.csv')
        self.util.df_to_csv(df_inv, self.sp_s.OUTPUT_DIR, '在庫データ(全期間).csv')
        return df_training_sales, df_actual_sales, df_inv

    def _fetch_sales_by_item(self, item_cd_li, floor_date, upper_date):
        return self.util.select_ec_sales(self.sql_cli, item_cd_li, floor_date, upper_date)


if __name__ == '__main__':
    sp = SalesPrediction()
    sp.execute()
    print("END")
