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
        self.store_cd = self.distro_cd = self.item_cd = None

    def execute(self):
        df_tgt_item = self._fetch_tgt_item()
        for row in df_tgt_item.iterrows():
            self.store_cd = row[1]['store_cd']
            self.distro_cd = row[1]['distro_cd']
            self.item_cd = row[1]['item_cd']
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
        df_master = self._fetch_master_data()
        self._fetch_tran_data()
        self._calc_factor()

    def _fetch_tran_data(self):
        self.df_inv_prdct_date = self._fetch_inv_qty(self.sql_cli, self.store_cd, self.item_cd, self.bsa_s.PRDCT_DATE)
        print(self.df_inv_prdct_date)

    def _fetch_master_data(self):
        df_ord_info = self._fetch_ord_info()
        return df_ord_info

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

    def _fetch_min_max_inv(self, sql_cli, store_cd, item_cd, floor_date, upper_date, use_calced_fctr=True, ):
        if use_calced_fctr:
            sql_li = [SQL_DICT['select_min_max_inv'].format(store_cd=store_cd, item_cd=item_cd,
                                                                tgt_date=floor_date + datetime.timedelta(i)) for i
                      in range((upper_date - floor_date).days + 1)]
            sql = 'union all '.join(sql_li)
            df_min_max_inv = pd.read_sql(sql, sql_cli.conn)
        return df_min_max_inv

    def _fetch_ord_info(self):
        df_supprier_cd = self.preproc.fetch_item_info(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE)

        df_ord_lot_num = self._fetch_ord_lot_num(self.sql_cli, self.store_cd,self.item_cd, self.bsa_s.TGT_FLOOR_DATE,
                                             self.bsa_s.TGT_UPPER_DATE)

        df_ord_info = self._fetch_ord_delivery_timing_and_lt(self.sql_cli,df_supprier_cd, self.bsa_s.TGT_FLOOR_DATE,
                                               self.bsa_s.TGT_UPPER_DATE)
        df_ord_info = pd.merge(df_ord_info,df_ord_lot_num)
        df_min_max_inv = self._fetch_min_max_inv(self.sql_cli,self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,self.bsa_s.TGT_UPPER_DATE,True)

        return pd.merge(df_ord_info,df_min_max_inv)

    def _fetch_ord_lot_num(self, sql_cli, store_cd,item_cd, floor_date, upper_date):
        sql_li = [SIM_SQL_DICT['select_ord_lot_num'].format(store_cd=store_cd,item_cd=item_cd, tgt_date=floor_date + datetime.timedelta(i)) for i
                  in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        df_ord_num = pd.read_sql(sql, sql_cli.conn)
        return df_ord_num

    def _fetch_ord_delivery_timing_and_lt(self, sql_cli,df,  floor_date, upper_date):
        sql_li = [SIM_SQL_DICT['select_ord_delivery_timing_and_lt'].format(supplier_cd=row[1]['supplier_cd'], store_cd=row[1]['store_cd'],item_cd= row[1]['item_cd'],tgt_date=row[1]['日付'])
                  for row in df.iterrows()]
        sql = 'union all '.join(sql_li)
        df_ord_delivery_timing_and_lt = pd.read_sql(sql, sql_cli.conn)
        return df_ord_delivery_timing_and_lt

    def _fetch_inv_qty(self, sql_cli, store_cd, item_cd, prdct_date):
        sql = SIM_SQL_DICT['select_prdct_date_inv_qty_by_item'].format(store_cd=store_cd, item_cd=item_cd,
                                                                   prdct_date=prdct_date)
        df_inv_prdct_date = pd.read_sql(sql, sql_cli.conn)
        return df_inv_prdct_date


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
