import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.EC_SalesSpikeAnalysisSetting import *
from Common.Logic.SalesSpike import *


class ECSalesSpikeAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        # self.sql_cli = None
        self.util = Util()
        self.ec_ssa_s = ECSalesSpikeAnalysisSetting()
        self.preprc_s = PreprocessSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.df_shortage = self.df_master = None
        self.file_name = '【201806-08】'

    def execute(self):
        # df_daily_sales_qty_by_chanel = self._preprocess()
        df_daily_sales_qty_by_chanel = self._fetch_daily_sales_qty_by_chanel()
        self._calc_spike_by_dept_mall(
            df_daily_sales_qty_by_chanel, ['発注GP', 'dept_cd', '部門名'], ['item_cd', '商品名', '日付'])
        self._calc_spike_by_dept_mall(
            df_daily_sales_qty_by_chanel, ['発注GP', 'dept_cd', '部門名', '日付'], ['item_cd', '商品名'])
        self._calc_spike_by_dept_mall(
            df_daily_sales_qty_by_chanel, ['日付'], ['item_cd', '商品名', '発注GP', 'dept_cd', '部門名'])

    def _fetch_daily_sales_qty_by_chanel(self):
        return pd.read_sql(SQL_DICT['select_daily_sales_qty_by_chanel'], self.sql_cli.conn)

    def _calc_spike_by_dept_mall(self, df_daily_sales_qty_by_chanel, index_li, unnessary_cols_li=None):
        mall_li = df_daily_sales_qty_by_chanel.columns.values.tolist()
        [mall_li.remove(c) for c in mall_li if c in index_li + unnessary_cols_li]
        df_spike = pd.DataFrame
        for m in mall_li:
            df_tgt_spike = SalesSpike(df_daily_sales_qty_by_chanel, index_li, m, False, True).execute()
            if df_spike.empty:
                df_spike = df_tgt_spike
            else:
                df_spike = pd.merge(df_spike, df_tgt_spike, how='outer', on=index_li, suffixes=['', '_' + m])
                df_spike_by_index = df_spike.groupby(index_li).sum().reset_index()
                file_name = '_'.join(index_li) + 'スパイク数.csv'
                self.util.df_to_csv(df_spike_by_index, self.ec_ssa_s.OUTPUT_DIR, self.file_name + file_name)

    def _preprocess(self):
        index_gp = ['発注GP', 'dept_cd', '部門名', 'item_cd', '商品名', '日付']
        # df_sales_by_chanel = self.util.select_ec_total_sales_by_chanel(self.sql_cli, self.ec_ssa_s.TGT_FLOOR_DATE,
        #                                                                self.ec_ssa_s.TGT_UPPER_DATE, True,
        #                                                                self.ec_ssa_s.OUTPUT_DIR, '全販売数(201801-08).csv')
        # df_sales_by_chanel = self.util.csv_to_df(self.ec_ssa_s.OUTPUT_DIR + '全販売数(201801-08).csv', True, True)
        # # df_sales_by_chanel = self.util.csv_to_df(self.ec_ssa_s.OUTPUT_DIR + 'サンプル販売数.csv', True, True)
        # df_sales_by_chanel = self.mmt.merge_chanel(df_sales_by_chanel)
        #
        # df_all_sales = df_sales_by_chanel.groupby(['item_cd', '日付'])['販売数'].sum().reset_index()
        # df_all_sales.rename(columns={'販売数': '全販売数'}, inplace=True)
        # df_sales_qty_by_chanel = df_sales_by_chanel.pivot_table(index=index_gp, columns='店舗名称', values='販売数',
        #                                                         aggfunc=sum).reset_index()
        #
        # df_daily_sales_qty_by_chanel = pd.merge(df_sales_qty_by_chanel, df_all_sales, on=['item_cd', '日付']).fillna(0.0)
        # self.util.df_to_csv(df_daily_sales_qty_by_chanel, self.ec_ssa_s.OUTPUT_DIR, '日別JAN別モール別ローデータ.csv')
        #
        # # その他GP作成のため、necessary_gpを作成
        # necessary_gp = index_gp + ['全販売数']
        # for mall_gp, mall_li in self.ec_ssa_s.MALL_GP.items():
        #     necessary_gp.append(mall_gp)
        #     df_daily_sales_qty_by_chanel[mall_gp] = 0
        #     for mall in mall_li:
        #         df_daily_sales_qty_by_chanel[mall_gp] = df_daily_sales_qty_by_chanel[mall_gp].astype(int) + \
        #                                                 df_daily_sales_qty_by_chanel[mall].astype(int)
        #         necessary_gp.append(mall)
        # unnecessary_gp = [c for c in df_daily_sales_qty_by_chanel.columns if c not in necessary_gp]
        # df_daily_sales_qty_by_chanel['その他'] = 0
        # for c in unnecessary_gp:
        #     df_daily_sales_qty_by_chanel['その他'] = df_daily_sales_qty_by_chanel['その他'] + df_daily_sales_qty_by_chanel[c]
        # df_daily_sales_qty_by_chanel.drop(unnecessary_gp, axis=1, inplace=True)
        # self.util.df_to_csv(df_daily_sales_qty_by_chanel, self.ec_ssa_s.OUTPUT_DIR, '日別JAN別モール別加工データ.csv')

        df_daily_sales_qty_by_chanel = pd.read_csv(
            self.ec_ssa_s.OUTPUT_DIR + '日別JAN別モール別加工データ.csv', encoding='cp932', engine='python',
            dtype=self.ec_ssa_s.AMOUNT_DTYPE)
        df_daily_sales_qty_by_chanel['日付'] = pd.to_datetime(df_daily_sales_qty_by_chanel['日付'])
        df_daily_sales_qty_by_chanel = df_daily_sales_qty_by_chanel[
            df_daily_sales_qty_by_chanel['日付'].between(datetime.date(2018, 6, 1), datetime.date(2018, 8, 31))]

        all_mall_li = df_daily_sales_qty_by_chanel.columns.values.tolist()
        [all_mall_li.remove(c) for c in index_gp]
        df_adjuted = self.mmt.merge_calender(df_daily_sales_qty_by_chanel[index_gp + ['全販売数']],
                                             floor_date=datetime.date(2018, 6, 1),
                                             upper_date=datetime.date(2018, 8, 31),
                                             adjust_0=True, amout_col='全販売数')
        df_all_sales = pd.merge(df_adjuted, df_daily_sales_qty_by_chanel.drop('全販売数',axis=1), how='left').fillna(0)

        self.util.df_to_csv(df_all_sales, dir=self.ec_ssa_s.OUTPUT_DIR,
                            file_name=self.file_name + '日別JAN別モール別販売数.csv')

        return df_daily_sales_qty_by_chanel


if __name__ == '__main__':
    essa = ECSalesSpikeAnalysis()
    essa.execute()
    print("END")
