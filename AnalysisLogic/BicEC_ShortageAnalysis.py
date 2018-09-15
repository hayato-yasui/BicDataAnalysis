import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.BicEC_ShortageAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class BicECShortageAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.bsa_s = BicECShortageAnalysisSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.df_shortage = self.df_master = None

    def execute(self):
        # self._preprocess()
        # self.df_shortage = self.extract_shortage_of_tgt_dept()
        # self.df_master = self._fetch_master()
        # self.df_tran = self._fetch_tran()
        # df_output = pd.merge(self.df_master, self.df_shortage, on=['item_cd'])
        # df_output = pd.merge(df_output, self.df_tran)
        # self.util.df_to_csv(df_output, './data/Output/sample/', "欠品マス目.csv")

        # self._create_daily_sales_inv_amount_by_item()
        self._extract_sales_spike()

    def _preprocess(self):
        pass

    def _extract_sales_spike(self):
        a = 2
        df_tgt_item = self._fetch_tgt_item()
        df_sales_spike = pd.DataFrame()
        for row in df_tgt_item.iterrows():
            df_sales_by_item = self.util.select_sales_amount_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                                     item_cd=row[1]['item_cd'])

            df_sales_spike_by_item = self._calc_sales_spike(df_sales_by_item, a)
            df_daily_inv = self.util.select_inv_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                        item_cd=row[1]['item_cd'])
            df_sales_spike_by_item = pd.merge(df_sales_spike_by_item, df_daily_inv, how='outer')
            df_sales_spike = df_sales_spike.append(df_sales_spike_by_item, ignore_index=True)
        df_sales_spike = pd.merge(df_sales_spike, df_tgt_item)
        self.util.df_to_csv(df_sales_spike, './data/Output/sample/', "販売スパイク_" + str(a) + ".csv")
        return df_sales_spike

    def _calc_sales_spike(self, df, a=2):
        # 平均と標準偏差
        average = np.mean(df["販売数"])
        sd = np.std(df["販売数"])

        # 外れ値の基準点
        # outlier_min = average - (sd) * a
        outlier_max = average + (sd) * a
        df["μ"] = average
        df["μ+" + str(a) + "σ"] = outlier_max
        if df.empty:
            return
        df['スパイク日'] = df.apply(lambda x: 1 if x['販売数'] >= outlier_max else 0, axis=1)
        return df

    def _fetch_tgt_item(self):
        df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_5dept.csv', encoding='cp932', engine='python')
        dept_li = df_tgt_dept["dept_cd"].tolist()
        return self.util.select_all_item_using_dept(self.sql_cli, '861', dept_li)

    def _create_daily_sales_inv_amount_by_item(self):
        df_tgt_item = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_5item.csv', encoding='cp932', engine='python')
        df_sales_inv = pd.DataFrame()
        for row in df_tgt_item.iterrows():
            df_inv_by_item = self.util.select_inv_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                          item_cd=row[1]['item_cd'])
            df_sales_by_item = self.util.select_sales_amount_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                                     item_cd=row[1]['item_cd'])
            df_sales_pivot_by_chanel = df_sales_by_item.pivot_table(
                index=['日付', 'item_cd'], columns='chanel_cd', values='販売数', aggfunc=sum).reset_index()
            df_sales_inv_by_item = pd.merge(df_inv_by_item, df_sales_pivot_by_chanel, how="outer")
            df_sales_inv = df_sales_inv.append(df_sales_inv_by_item, ignore_index=True)
        self.util.df_to_csv(df_sales_inv, './data/Output/sample/', "日別販売・在庫量.csv")
        return df_sales_inv

    def extract_shortage_of_tgt_dept(self):
        df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_5dept.csv', encoding='cp932', engine='python')
        df_shortage = pd.DataFrame()
        for row in df_tgt_dept.iterrows():
            df_shortage_by_item = self.util.select_shortage_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                                    dept_cd=row[1]['dept_cd'])
            df_shortage = df_shortage.append(df_shortage_by_item, ignore_index=True)
        self.util.df_to_csv(df_shortage, './data/Output/sample/', "欠品マス目.csv")
        return df_shortage

    def extract_shortage_by_tgt_item(self):
        pass

    def _fetch_master(self):
        item_cd_li = self.df_shortage[~ self.df_shortage.duplicated()]['item_cd'].tolist()
        df_master = self.util.extract_tgt_itm_info(self.sql_cli, item_cd_li)
        return pd.merge(df_master, self.util.select_auto_ord_start_date(self.sql_cli, '861', item_cd_li))

    def _fetch_tran(self):
        item_cd_li = self.df_shortage['item_cd'].tolist()
        df_sales_amount = self.util.select_ec_sales_amount(self.sql_cli, item_cd_li)
        df_sales_pivot_by_chanel = df_sales_amount.pivot_table(
            index='item_cd', columns='chanel_cd', values='販売量', aggfunc=sum).reset_index()
        return df_sales_pivot_by_chanel


if __name__ == '__main__':
    bsa = BicECShortageAnalysis()
    bsa.execute()
    print("END")
