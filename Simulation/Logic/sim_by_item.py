import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import codecs as cd

from Common.DB.SQLServer_Client import SQLServerClient
from Simulation.DB.sql import *
from Common.util import Util
from Simulation.Setting.sim_by_item import *
from Simulation.Logic.Util.sim_util import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *


class SimByItem:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()
        self.sim_util = SimUtil()
        self.bsa_s = SimByItemSetting()
        self.preproc = Preprocess()
        self.preprc_s = PreprocessSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.df_tgt_item = self.df_prdct_priod = None
        # self.store_cd = self.distro_cd = self.item_cd = None

    def execute(self):
        self.df_tgt_item = self._fetch_tgt_item()
        self._preprocess()
        self._exec_sim()
        self._postprocess()

    def _exec_sim(self):
        """
        Recommend ord numl = XXXXX
        """
        pass

    def _fetch_tgt_item(self):
        with cd.open(self.preprc_s.TGT_ITEM_FILE_DIR , "r", "cp932","ignore") as csv_file:
            df = pd.read_csv(csv_file,engine='python')
        return self.preproc.adjust_0_filled(df)

    def _preprocess(self):
        df_master, df_event = self._fetch_master_data()
        self._fetch_tran_data()
        self._calc_factor()

    def _fetch_tran_data(self):
        self.df_inv = self._fetch_inv_qty(self.df_tgt_item, self.sql_cli, self.bsa_s.PRDCT_DATE)
        self.df_deliveries = self._fetch_deliveries()

    def _fetch_master_data(self):
        df_prdct_priod = self._fetch_daily_prdct_priod()
        df_event = self._fetch_event(from_csv=True)
        return df_prdct_priod, df_event

    def _calc_factor(self):

        self._calc_season_weekend(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE,True)
        self._calc_sales_avg(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE,True)
        self._calc_sales_std(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE,True)
        self._calc_safty_inv_fctr(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE,True)

    def _calc_season_weekend(self, sql_cli, store_cd,item_cd, floor_date, upper_date,use_calced_fctr=True):
        if use_calced_fctr:
            sql = SQL_DICT['select_ord_delivery_timing_and_lt'].format(item_cd=item_cd, store_cd=store_cd,
                                                                       floor_date=floor_date, upper_date=upper_date)
            df_season_weekend = pd.read_sql(sql, sql_cli.conn)
            return df_season_weekend
        return

    def _calc_sales_avg(self, sql_cli, store_cd,item_cd, floor_date, upper_date,use_calced_fctr=True,):
        if use_calced_fctr:
            sql = SQL_DICT['select_ord_delivery_timing_and_lt'].format(item_cd=item_cd, store_cd=store_cd,
                                                                       floor_date=floor_date, upper_date=upper_date)
            df_season_weekend = pd.read_sql(sql, sql_cli.conn)
            return df_season_weekend
        return

    def _calc_sales_std(self, sql_cli, store_cd,item_cd, floor_date, upper_date,use_calced_fctr=True,):
        if use_calced_fctr:
            sql = SQL_DICT['select_ord_delivery_timing_and_lt'].format(item_cd=item_cd, store_cd=store_cd,
                                                                       floor_date=floor_date, upper_date=upper_date)
            df_season_weekend = pd.read_sql(sql, sql_cli.conn)
            return df_season_weekend
        return

    def _calc_safty_inv_fctr(self, sql_cli, store_cd, item_cd, floor_date, upper_date, use_calced_fctr=True):
        if use_calced_fctr:
            sql = SQL_DICT['select_ord_delivery_timing_and_lt'].format(item_cd=item_cd, store_cd=store_cd,
                                                                       floor_date=floor_date, upper_date=upper_date)
            df_season_weekend = pd.read_sql(sql, sql_cli.conn)
            return df_season_weekend
        return

    def _fetch_min_max_inv(self, df, sql_cli, floor_date, upper_date, use_calced_fctr=True, ):
        if use_calced_fctr:
            sql_li = [SQL_DICT['select_min_max_inv'].format(tore_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'],
                                                            tgt_date=upper_date) for row in df.iterrows()]
            sql = 'union all '.join(sql_li)
            df_min_max_inv = pd.read_sql(sql, sql_cli.conn)
        return df_min_max_inv

    def _fetch_daily_prdct_priod(self):
        """
         - 日別で発注可能か納品可能かを算出
         - ロット数を取得(まとめ発注)
         - 最低、最高在庫数を設定(デフォルト or 計算)
         - 次回発注商品の到着日を算出
         - γを算出:
        """
        df_ord_dst_info = self._fetch_ord_dst_info()
        df_ord_lot_num = self._fetch_ord_lot_num(self.df_tgt_item, self.sql_cli, self.bsa_s.TGT_UPPER_DATE)
        df_ord_info = pd.merge(df_ord_dst_info, df_ord_lot_num)
        df_min_max_inv = self._fetch_min_max_inv(self.df_tgt_item, self.sql_cli, self.bsa_s.TGT_FLOOR_DATE,
                                                 self.bsa_s.TGT_UPPER_DATE, True)
        df_ord_info = pd.merge(df_ord_info, df_min_max_inv)
        df_ord_info["日付"] = pd.to_datetime(df_ord_info["日付"])
        df_ord_info["次回発注商品販売日"] = None
        for row in self.df_tgt_item.itrrows():
            df_ord_by_item = df_ord_info[
                (df_ord_info['store_cd'] == row[1]['store_cd']) & (df_ord_info['item_cd'] == row[1]['item_cd'])]
            df_prdct_priod = self.df_prdct_priod.append(self._calc_prdct_priod(df_ord_by_item))
        return df_prdct_priod

    def _calc_prdct_priod(self, df_ord_info):
        for row in df_ord_info.iterrows():
            if row[1]['発注可能'] == 0:
                continue
            next_ord = row[1]['日付'] + datetime.timedelta(1)
            for i in range((self.bsa_s.TGT_UPPER_DATE - next_ord).days + 1):
                if len(df_ord_info[(df_ord_info["日付"] == next_ord + datetime.timedelta(i))
                                   & (df_ord_info["発注可能"] == 1)]) == 1:
                    next_ord = next_ord + datetime.timedelta(i)
            ship_date = None
            possible_ship_date = next_ord + datetime.timedelta(row[1]["出荷LT"])
            for i in range((self.bsa_s.TGT_UPPER_DATE - next_ord).days + 1):
                if len(df_ord_info[(df_ord_info["日付"] == possible_ship_date + datetime.timedelta(i)) & (
                        df_ord_info["発注可能"] == 1)]) == 1:
                    ship_date = possible_ship_date + datetime.timedelta(i)
                    break
            if ship_date is None:
                continue

            possible_del_date = ship_date + datetime.timedelta(row[1]["納品LT"])
            for i in range((self.bsa_s.TGT_UPPER_DATE - possible_del_date).days + 1):
                if len(df_ord_info[(df_ord_info["日付"] == possible_del_date + datetime.timedelta(i)) & (
                        df_ord_info["納品可能"] == 1)]) == 1:
                    del_date = possible_del_date + datetime.timedelta(i)
                    df_ord_info.loc[df_ord_info['日付'] == row[1]["日付"], '次回発注商品販売日'] = del_date
                    break
        return df_ord_info

    def _fetch_ord_dst_info(self):
        df_ord_div = self._fetch_ord_div(self.df_tgt_item, self.sql_cli, self.bsa_s.TGT_FLOOR_DATE,
                                         self.bsa_s.TGT_UPPER_DATE)
        # 仕入れ先区分を"1"と指定しておき、df_ord_divと結合
        df_supprier_cd = self._fetch_supprier_cd(self.df_tgt_item, self.sql_cli, self.bsa_s.TGT_FLOOR_DATE,
                                                 self.bsa_s.TGT_UPPER_DATE)
        df_ord_dst_info = pd.merge(df_ord_div, df_supprier_cd, how="left", on={"日付", 'item_cd', 'store_cd', "仕入れ先区分"})
        df_supprier_special_hol = self._fetch_supplier_special_holiday(
            df_ord_dst_info, self.sql_cli, self.bsa_s.TGT_FLOOR_DATE, self.bsa_s.TGT_UPPER_DATE)
        df_ord_dst_info = pd.merge(df_ord_dst_info, df_supprier_special_hol, how="left")

        df_ord_delivery_timing_and_lt = self._fetch_ord_delivery_timing_and_lt(
            df_ord_dst_info, self.sql_cli, self.bsa_s.TGT_FLOOR_DATE, self.bsa_s.TGT_UPPER_DATE)
        df_ord_dst_info = pd.merge(df_ord_dst_info, df_ord_delivery_timing_and_lt, how="left",
                                   on={'item_cd', 'store_cd', 'supplier_cd'})

        df_ord_dst_info['発注可能'] = df_ord_dst_info.apply(lambda x: 0 if x['発注不能'] == 1 else x['発注可能'])
        df_ord_dst_info['納品可能'] = df_ord_dst_info.apply(lambda x: 0 if x['納品不能'] == 1 else x['納品可能'])
        return df_ord_dst_info.drop(['発注不能', '納品不能'])

    def _fetch_supprier_cd(self, df, sql_cli, floor_date, upper_date):
        sql_li = []
        for row in df.iterrows():
            sql_li = [SIM_SQL_DICT['select_supplier_cd'].format(tore_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'],
                                                                tgt_date=floor_date + datetime.timedelta(i)) for i
                      in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    def _fetch_ord_div(self, df, sql_cli, floor_date, upper_date):
        sql_li = []
        for row in df.iterrows():
            sql_li = [SIM_SQL_DICT['select_ord_div'].format(store_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'],
                                                            tgt_date=floor_date + datetime.timedelta(i)) for i
                      in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    def _fetch_event(self, from_db=False, from_csv=False) -> pd.DataFrame:
        if from_db:
            pass
        if from_csv:
            df_event = None
        return df_event

    def _fetch_ord_lot_num(self, df, sql_cli, tgt_date):
        sql_li = [SIM_SQL_DICT['select_ord_lot_num'].format(store_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'],
                                                            tgt_date=tgt_date) for row in df.iterrows()]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    def _fetch_ord_delivery_timing_and_lt(self, df, sql_cli, floor_date, upper_date):
        sql_li = []
        df_tgt_date = df[df['日付'] == upper_date]
        for row in df_tgt_date.iterrows():
            sql_li = [SIM_SQL_DICT['select_ord_delivery_timing_and_lt'].format(supplier_cd=row[1]['supplier_cd'],
                                                                               store_cd=row[1]['store_cd'],
                                                                               item_cd=row[1]['item_cd'],
                                                                               tgt_date=upper_date,
                                                                               dummy_date=floor_date + datetime.timedelta(
                                                                                   i))
                      for i in range((upper_date - floor_date).days + 1) if row[1]['仕入れ先区分'] == 1]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    def _fetch_supplier_special_holiday(self, df, sql_cli, floor_date, upper_date):
        sql_li = []
        for row in df.iterrows():
            sql_li = [SIM_SQL_DICT['select_supplier_special_holiday'].format(
                supplier_cd=row[1]['supplier_cd'], store_cd=row[1]['store_cd'],
                item_cd=row[1]['item_cd'], tgt_date=floor_date + datetime.timedelta(i))
                for i in range((upper_date - floor_date).days + 1) if row["仕入れ先区分"] == '1']
        sql = 'union all '.join(sql_li)
        df_hol = pd.read_sql(sql, sql_cli.conn).drop_duplicates()
        df_hol_ord = pd.DataFrame(columns=["supplier_cd", "store_cd", "item_cd", '日付', "発注不能"])
        df_hol_del = pd.DataFrame(columns=["supplier_cd", "store_cd", "item_cd", '日付', "納品不能"])

        for row in df_hol.iterrows():
            if row[1]["発注不能日数"] is None:
                continue
            for dateadd in row[1]["発注不能日数"]:
                date = datetime.datetime.strptime((row[1]['発注不能開始日']), '%Y%m%d') + datetime.timedelta(dateadd)
                df_new_rec = pd.DataFrame(
                    [row[1]['supplier_cd'], row[1]['store_cd'], row[1]['item_cd'], date, 1], columns=df_hol_ord.columns)
                df_hol_ord = df_hol_ord.append(df_new_rec)
        for row in df_hol_del.iterrows():
            if row[1]["納品不能日数"] is None:
                continue
            for dateadd in range(1, row[1]["納品不能日数"]):
                date = datetime.datetime.strptime((row[1]['納品不能開始日']), '%Y%m%d') + datetime.timedelta(dateadd)
                df_new_rec = pd.DataFrame(
                    [row[1]['supplier_cd'], row[1]['store_cd'], row[1]['item_cd'], date, 1], columns=df_hol_del.columns)
                df_hol_ord = df_hol_ord.append(df_new_rec)
        return pd.merge(df_hol_ord, df_hol_ord, how="outer")

    def _fetch_inv_qty(self, df, sql_cli, floor_date=datetime.date(2018, 5, 1), upper_date=datetime.date(2018, 7, 31)):
        sql_li = []
        for row in df.iterrows():
            sql_li = [
                SIM_SQL_DICT['select_inv_qty_by_item'].format(store_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'],
                                                              tgt_date=floor_date + datetime.timedelta(i)) for i
                in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    def _fetch_deliveries(self):
        """
        シミュレーション開始日を基準に、初回発注の商品の到着日前日までは納品予定TBLから在庫量を補充する。
        """
        return None

    def _postprocess(self):
        # self._fetch_actual_sales()
        # self._fetch_actual_inv()
        # self._calc_kpi()
        # self._output()
        pass


if __name__ == '__main__':
    sbi = SimByItem()
    sbi.execute()
    print("END")
