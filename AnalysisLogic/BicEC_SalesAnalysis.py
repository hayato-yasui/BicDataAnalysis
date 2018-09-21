import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.BicEC_SalesAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class BicECSalesAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.bsa_s = BicECSalesAnalysisSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()

    def execute(self):
        self._preprocess()
        df_sales_pivot_by_chanel = self.extract_sales_qty_by_chanel()

    def _preprocess(self):
        # df_itm = self._fetch_itm_master()
        # df_itm = pd.read_csv('./data/Input/tgt_itm_cd/8JAN.csv', encoding='cp932', engine='python')
        self.df_chanel = pd.read_csv(self.mmt_s.F_PATH_CHANEL, encoding='cp932', engine='python')
        self.chanel_li = self.df_chanel["店舗名称"].tolist()

    def extract_sales_qty_by_chanel(self):
        df_sales_by_chanel = self.util.select_ec_total_sales_by_chanel(self.sql_cli, does_output=True,
                                                                       dir=self.bsa_s.OUTPUT_DIR,
                                                                       file_name='商品別チャネル別部門別売上金額_縦持.csv')
        df_sales_by_chanel = self.mmt.merge_chanel(df_sales_by_chanel, self.mmt_s.F_PATH_CHANEL)

        # 0.49以下をNullとする
        for c in ['売上金額', '販売数']:
            df_sales_by_chanel[c] = df_sales_by_chanel.apply(lambda x: None if x[c] <= 0.49 else x[c], axis=1)

        # # チャネルGP別にも算出
        # chanel_gp_li = ['自社EC', 'Amazon', '楽天', 'Yahoo!', 'Wowma!', 'デジタルコレクション']
        # for chanel_gp in chanel_gp_li:
        #     for c in ['売上金額', '販売数']:
        #         df_sales_by_chanel[]

        # df_sales_by_dept = df_sales_by_chanel.groupby('部門コード')['売上金額', '販売数'].sum().reset_index()
        df_sales_by_dept = df_sales_by_chanel.groupby('JANコード')['売上金額', '販売数'].sum().reset_index()
        df_sales_by_dept.rename(columns={'売上金額': '売上金額_sum', '販売数': '販売数_sum'}, inplace=True)

        # index = ['発注GP', '部門コード', '部門名']
        index = ['発注GP', '部門コード', '部門名','JANコード','商品名']
        df_sales_pivot_by_chanel = df_sales_by_chanel.pivot_table(index=index, columns='店舗名称', values='売上金額',
                                                                  aggfunc=sum).reset_index()
        df_sales_qty_pivot_by_chanel = df_sales_by_chanel.pivot_table(index=index, columns='店舗名称', values='販売数',
                                                                      aggfunc=sum).reset_index()

        # df_sales_pivot_by_chanel = pd.merge(df_sales_pivot_by_chanel, df_sales_by_dept, on='部門コード')
        # df_sales_qty_pivot_by_chanel = pd.merge(df_sales_qty_pivot_by_chanel, df_sales_by_dept, on='部門コード')
        df_sales_pivot_by_chanel = pd.merge(df_sales_pivot_by_chanel, df_sales_by_dept, on='JANコード')
        df_sales_qty_pivot_by_chanel = pd.merge(df_sales_qty_pivot_by_chanel, df_sales_by_dept, on='JANコード')

        for c in self.chanel_li:
            df_sales_pivot_by_chanel[c + '_売上比率'] = df_sales_pivot_by_chanel.apply(
                lambda x: (x[c]) / x['売上金額_sum'] if x[c] is not None and x['売上金額_sum'] != 0 else 0, axis=1)
            df_sales_pivot_by_chanel[c] = df_sales_pivot_by_chanel.apply(lambda x: None if x[c] <= 0 else x[c], axis=1)
            df_sales_pivot_by_chanel[c + '_売上比率'] = df_sales_pivot_by_chanel.apply(
                lambda x: None if x[c + '_売上比率'] <= 0 else x[c + '_売上比率'], axis=1)

            df_sales_qty_pivot_by_chanel[c + '_販売比率'] = df_sales_qty_pivot_by_chanel.apply(
                lambda x: x[c] / x['販売数_sum'] if x[c] is not None and x['販売数_sum'] != 0 else 0, axis=1)
            df_sales_qty_pivot_by_chanel[c] = df_sales_qty_pivot_by_chanel.apply(lambda x: None if x[c] <= 0 else x[c],
                                                                                 axis=1)
            df_sales_qty_pivot_by_chanel[c + '_販売比率'] = df_sales_qty_pivot_by_chanel.apply(
                lambda x: None if x[c + '_販売比率'] <= 0 else x[c + '_販売比率'], axis=1)

        # 部門別JAN数取得
        df_jan_num_by_dept = self.util.select_jan_num_by_dept(self.sql_cli)
        df_sales_pivot_by_chanel = pd.merge(df_sales_pivot_by_chanel,df_jan_num_by_dept)
        df_sales_qty_pivot_by_chanel = pd.merge(df_sales_qty_pivot_by_chanel,df_jan_num_by_dept)

        self.util.df_to_csv(df_sales_pivot_by_chanel, dir=self.bsa_s.OUTPUT_DIR,
                            file_name='商品別チャネル別部門別売上金額_横持.csv')
        self.util.df_to_csv(df_sales_qty_pivot_by_chanel, dir=self.bsa_s.OUTPUT_DIR,
                            file_name='商品別チャネル別部門別販売量_横持.csv')

        # df_sales_qty_pivot_by_chanel = df_sales_qty_pivot_by_chanel.set_index('vcItemCategory1Name')
        # sns.heatmap(df_sales_qty_pivot_by_chanel['BicEC_販売比率'])
        # plt.savefig('./sample.png')
        return df_sales_pivot_by_chanel


if __name__ == '__main__':
    bsa = BicECSalesAnalysis()
    bsa.execute()
    print("END")
