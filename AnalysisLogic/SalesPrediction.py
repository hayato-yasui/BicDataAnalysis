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
        # self._create_model
        self._exec_sim(df_training_data, df_actual_sales, df_inv)
        print(1)

    def _exec_sim(self, df_training_data, df_actual_sales, df_inv):
        if self.sp_s.ANALYSIS_METHOD == 'normal_distribution':
            self.nd = NormalDistribution(df_training_data, df_actual_sales, df_inv)
            self.nd.execute()
        else:
            pass
        return

    def _preprocess(self):
        df_tgt_item = pd.read_csv('./data/Input/tgt_itm_cd/sim/25item.csv', encoding='cp932', engine='python')
        df_tgt_item = self.preprc.adjust_0_filled(df_tgt_item)
        df_training_sales_by_chanel = self._fetch_sales_by_item(df_tgt_item['item_cd'].tolist(),
                                                                self.sp_s.TRAINING_FLOOR_DATE,
                                                                self.sp_s.TRAINING_UPPTER_DATE)
        df_training_sales_by_chanel = self.mmt.merge_chanel(df_training_sales_by_chanel)
        df_actual_sales_by_chanel = self._fetch_sales_by_item(df_tgt_item['item_cd'].tolist(), self.sp_s.TGT_FLOOR_DATE,
                                                              self.sp_s.TGT_UPPER_DATE)
        df_actual_sales_by_chanel = self.mmt.merge_chanel(df_actual_sales_by_chanel)
        df_training_sales_by_chanel_group = self.preprc.merge_sales_by_chanel_group(df_training_sales_by_chanel, '販売数')
        df_actual_sales_by_chanel_group = self.preprc.merge_sales_by_chanel_group(df_actual_sales_by_chanel, '販売数')

        # 学習期間 + シミュレーション期間の在庫データを取得
        df_inv = self.util.select_ec_inv_by_item(self.sql_cli, df_tgt_item, self.sp_s.TRAINING_FLOOR_DATE,
                                                 self.sp_s.TGT_UPPER_DATE)

        self.util.df_to_csv(df_training_sales_by_chanel_group, self.sp_s.OUTPUT_DIR, '学習データ.csv')
        self.util.df_to_csv(df_actual_sales_by_chanel_group, self.sp_s.OUTPUT_DIR, '実販売数データ.csv')
        self.util.df_to_csv(df_inv, self.sp_s.OUTPUT_DIR, '在庫データ(全期間).csv')
        return df_training_sales_by_chanel_group, df_actual_sales_by_chanel_group, df_inv

    def _fetch_sales_by_item(self, item_cd_li, floor_date, upper_date):
        return self.util.select_ec_sales(self.sql_cli, item_cd_li, floor_date, upper_date)


if __name__ == '__main__':
    sp = SalesPrediction()
    sp.execute()
    print("END")
