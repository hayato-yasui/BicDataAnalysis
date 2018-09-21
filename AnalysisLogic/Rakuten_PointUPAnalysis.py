import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from Common.DB.SQLServer_Client import SQLServerClient
from AnalysisLogic.Resource.SQL.Rakuten_sql import *
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.Rakuten_PointUPAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class RakutenPointUPAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.rpa_s = RakutenPointUPAnalysisSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.df_shortage = self.df_master = None

    def execute(self):
        df_sales_by_item = self._fetch_sales_by_item()
        self.util.df_to_csv(df_sales_by_item, self.rpa_s.OUTPUT_DIR, '楽天_JAN別日別販売数.csv')
        df_sales_spike = self._extract_sales_spike(df_sales_by_item, a=2)
        self.util.df_to_csv(df_sales_spike, self.rpa_s.OUTPUT_DIR, '楽天_JAN別日別販売数(スパイクフラグ付き).csv')
        # self._create_daily_sales_inv_amount_by_item()
        # self._extract_sales_spike()

    def _preprocess(self):
        pass

    def _fetch_sales_by_item(self):
        df_all_sales = pd.DataFrame
        tgt_date_li = ['\'' + str(self.rpa_s.TGT_FLOOR_DATE + datetime.timedelta(i)) + '\'' for i in
                       range((self.rpa_s.TGT_UPPER_DATE - self.rpa_s.TGT_FLOOR_DATE).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        chanel_cd_li = ['\'' + c + '\'' for c in ['846', '253', '736', '089']]
        chanel_cd = ','.join(chanel_cd_li)
        sql = RAKUTEN_SQL_DICT['select__sales_by_chanel_and_item'].format(
            tgt_date=tgt_date, upper_date=self.rpa_s.TGT_UPPER_DATE, chanel_cd=chanel_cd)
        df = pd.read_sql(sql, self.sql_cli.conn)

        for jan in df['item_cd'].drop_duplicates().tolist():
            df_sales = self.util.adjust_0_sales(df[df["item_cd"] == str(jan)], self.rpa_s.TGT_FLOOR_DATE,
                                                self.rpa_s.TGT_UPPER_DATE)
            if df_all_sales.empty:
                df_all_sales = df_sales
            else:
                df_all_sales = df_all_sales.append(df_sales)
        return df_all_sales

    def _extract_sales_spike(self, df_sales_by_item, a):
        df_sales_spike_by_item = self._calc_sales_spike(df_sales_by_item, a)
        self.util.df_to_csv(df_sales_spike_by_item, './data/Output/sample/', "販売スパイク_" + str(a) + ".csv")
        return df_sales_spike_by_item

    def _calc_sales_spike(self, df, a=2):
        df['年月'] = df['日付'].dt.year.astype(str) + df['日付'].dt.month.astype(str)
        df_m = df
        df_m.drop('日付', axis=1, inplace=True)
        df_m = df_m.groupby(['年月', '発注GP', 'store_cd', '部門コード', '部門名', '商品名', 'item_cd'])

        # 平均と標準偏差
        df_m_avg = df_m[['販売数']].mean().reset_index()
        df_m_std = df_m[['販売数']].std().reset_index()
        df_m_avg_std = pd.merge(df_m_avg.rename(columns={'販売数': 'μ'}), df_m_std.rename(columns={'販売数': 'σ'}))

        df_m_avg_std['μ+' + str(a) + "σ"] = df_m_avg_std['μ'] + a * df_m_avg_std['σ']
        df = pd.merge(df, df_m_avg_std)
        if df.empty:
            return
        df['スパイク日'] = df.apply(lambda x: 1 if x['販売数'] >= x["μ+" + str(a) + "σ"] else 0, axis=1)
        return df

    def _fetch_tgt_item(self):
        df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_5dept.csv', encoding='cp932', engine='python')
        dept_li = df_tgt_dept["dept_cd"].tolist()
        return self.util.select_all_item_using_dept(self.sql_cli, '861', dept_li)

    def _create_daily_sales_inv_amount_by_item(self):
        df_tgt_item = pd.read_csv('./data/Input/tgt_itm_cd/BicEC_spike3item.csv', encoding='cp932', engine='python')
        df_sales_inv = pd.DataFrame()
        for row in df_tgt_item.iterrows():
            df_inv_by_item = self.util.select_inv_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                          item_cd=row[1]['item_cd'])
            df_sales_by_item = self.util.select_sales_amount_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                                     item_cd=row[1]['item_cd'])
            df_price_by_item = self.util.select_price_by_item(self.sql_cli, store_cd=row[1]['store_cd'],
                                                              item_cd=row[1]['item_cd'])
            df_sales_pivot_by_chanel = df_sales_by_item.pivot_table(
                index=['日付', 'store_cd', 'item_cd'], columns='chanel_cd', values='販売数', aggfunc=sum).reset_index()
            df_sales_inv_by_item = pd.merge(df_inv_by_item, df_sales_pivot_by_chanel, how="outer",
                                            on=['item_cd', 'store_cd', '日付'])
            df_sales_inv_by_item = pd.merge(df_sales_inv_by_item, df_price_by_item, how="outer",
                                            on=['item_cd', 'store_cd', '日付'])
            for i in range((datetime.date(2018, 7, 31) - datetime.date(2017, 8, 1)).days + 1):
                date = datetime.datetime.strptime(str(datetime.date(2017, 8, 1) + datetime.timedelta(i)), '%Y-%m-%d')
                if len(df_sales_inv_by_item[df_sales_inv_by_item['日付'] == date]) == 0:
                    df_sales_inv_by_item = pd.concat \
                        ([df_sales_inv_by_item, pd.DataFrame([[date, row[1]['store_cd'], row[1]['item_cd']]],
                                                             columns=["日付", 'store_cd', 'item_cd'])])
            df_sales_inv_by_item.sort_values('日付', inplace=True)
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
    rpa = RakutenPointUPAnalysis()
    rpa.execute()
    print("END")
