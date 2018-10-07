import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.BicEC_PriceGapAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *
from Common.Logic.ChartClient import *


class BicECPriceGapAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.bpg_s = BicECPriceGapAnalysisSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.preproc=Preprocess
        self.chart_cli = ChartClient()
        self.df_tgt_item = None

    def execute(self):
        # df_price, df_sales, df_inv = self._preprocess()

        df_price = pd.read_csv(self.bpg_s.OUTPUT_DIR+'自社EC_売価(201806-201808).csv',encoding='cp932', engine='python')
        df_sales = pd.read_csv(self.bpg_s.OUTPUT_DIR+'自社EC_販売数(201806-201808).csv',encoding='cp932', engine='python')
        df_inv = pd.read_csv(self.bpg_s.OUTPUT_DIR+'自社EC_在庫(201806-201808).csv',encoding='cp932', engine='python')

        df_price = self.util.shape_values(df_price)
        df_sales = self.util.shape_values(df_sales)

        df_price_gap_item = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_PriceGapAnalysis_売価変動あり.csv', encoding='cp932', engine='python')
        df_price_gap_item['item_cd'] = df_price_gap_item['item_cd'].astype(str).str.zfill(13)

        df_price.set_index("日付",inplace=True)
        df_sales.set_index("日付",inplace=True)

        for item_cd in df_price_gap_item['item_cd'].tolist():
            df_p = df_price[df_price['item_cd'] == item_cd]
            df_s = df_sales[df_sales['item_cd'] == item_cd]
            if df_s.empty or df_p.empty:
                continue
            self.chart_cli.plot_2axis(df_p,df_s,True,self.bpg_s.OUTPUT_DIR+'販売数_売価グラフ/'+item_cd +'.png')

    def _preprocess(self):
        self.df_tgt_item = self._fetch_tgt_item()
        df_price = self.util.select_price_by_item(self.sql_cli,'861', self.df_tgt_item['item_cd'].tolist(),
                                                  self.bpg_s.TGT_FLOOR_DATE,self.bpg_s.TGT_UPPER_DATE)
        self.util.df_to_csv(df_price,self.bpg_s.OUTPUT_DIR,'自社EC_売価(201806-201808).csv')
        for item_cd in self.df_tgt_item['item_cd'].tolist():
            df = df_price[df_price['item_cd']==item_cd].set_index("日付")
            self.chart_cli.time_series_graph(df,'売価',True,self.bpg_s.OUTPUT_DIR+'売価グラフ/'+item_cd +'_売価グラフ.png')
        df_sales =  self.util.select_ec_sales(
            self.sql_cli, self.df_tgt_item['item_cd'].tolist(),self.bpg_s.TGT_FLOOR_DATE,self.bpg_s.TGT_UPPER_DATE,tgt_mall=['094','842'])
        self.util.df_to_csv(df_sales, self.bpg_s.OUTPUT_DIR, '自社EC_販売数(201806-201808).csv')
        df_inv = self.util.select_ec_inv_by_item(self.sql_cli, self.df_tgt_item, self.bpg_s.TGT_FLOOR_DATE, self.bpg_s.TGT_UPPER_DATE)
        self.util.df_to_csv(df_inv, self.bpg_s.OUTPUT_DIR, '自社EC_在庫(201806-201808).csv')

        df_price_sales = pd.merge(df_price, df_sales,how='left', on=['日付', 'store_cd', 'item_cd'])
        df_price_sales_inv = pd.merge(df_price_sales, df_inv,how='left', on=['日付', 'store_cd', 'item_cd'])

        self.util.df_to_csv(df_price_sales_inv, self.bpg_s.OUTPUT_DIR, '自社EC_売価_販売数_在庫(201806-201808).csv')

        return df_price,df_sales,df_inv

    def _fetch_tgt_item(self):
        df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_PriceGapAnalysis_dept.csv', encoding='cp932', engine='python')
        df_tgt_dept = self.preproc.adjust_0_filled(df_tgt_dept)
        dept_li = df_tgt_dept["dept_cd"].tolist()
        return self.util.select_all_item_using_dept(self.sql_cli, '861', dept_li,tgt_date=self.bpg_s.TGT_UPPER_DATE)

    def extract_sales_qty_by_chanel(self):
        df_sales_by_chanel = self.util.select_ec_total_sales_by_chanel(self.sql_cli, does_output=True,
                                                                       dir=self.bpg_s.OUTPUT_DIR,
                                                                       file_name='商品別チャネル別部門別売上金額_縦持.csv')
        df_sales_by_chanel = self.mmt.merge_chanel(df_sales_by_chanel)

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

        self.util.df_to_csv(df_sales_pivot_by_chanel, dir=self.bpg_s.OUTPUT_DIR,
                            file_name='商品別チャネル別部門別売上金額_横持.csv')
        self.util.df_to_csv(df_sales_qty_pivot_by_chanel, dir=self.bpg_s.OUTPUT_DIR,
                            file_name='商品別チャネル別部門別販売量_横持.csv')

        # df_sales_qty_pivot_by_chanel = df_sales_qty_pivot_by_chanel.set_index('vcItemCategory1Name')
        # sns.heatmap(df_sales_qty_pivot_by_chanel['BicEC_販売比率'])
        # plt.savefig('./sample.png')
        return df_sales_pivot_by_chanel


if __name__ == '__main__':
    bpg = BicECPriceGapAnalysis()
    bpg.execute()
    print("END")
