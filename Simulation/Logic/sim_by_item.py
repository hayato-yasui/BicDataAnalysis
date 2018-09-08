import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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
        self.preprc_s = PreprocessSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.store_cd = self.distro_cd = self.item_cd = None

    def execute(self):
        for list in self.bsa_s.TGT_STORE_DISTRO_ITEM_CD:
            self.store_cd = list[0]
            self.distro_cd = list[1]
            self.item_cd = list[2]
            self._preprocess()
            self._exec_sim()
            self._postprocess()

    def _exec_sim(self):
        """
        Recommend ord numl = XXXXX
        """
        pass

    def _preprocess(self):
        self._fetch_tran_data()
        self._fetch_master_data()
        self._calc_factor()

    def _fetch_tran_data(self):
        self.df_inv_prdct_date = self._fetch_inv_qty(self.sql_cli, self.store_cd, self.item_cd, self.bsa_s.PRDCT_DATE)
        print(self.df_inv_prdct_date)

    def _fetch_master_data(self):
        self._fetch_ord_info()

    def _calc_factor(self):
        self._calc_season_weekend()
        self._calc_sales_std()
        self._calc_safty_inv()
        self._fetch_min_inv()

        pass

    def _fetch_ord_info(self):
        df_ord_num = self._fetch_ord_lot_num(self.sql_cli, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,
                                             self.bsa_s.TGT_UPPER_DATE)
        self._fetch_ord_delivery_timing_and_lt(self.sql_cli, self.store_cd, self.item_cd, self.bsa_s.TGT_FLOOR_DATE,
                                               self.bsa_s.TGT_UPPER_DATE)

    def _fetch_ord_lot_num(self, sql_cli, item_cd, floor_date, upper_date):
        sql_li = [SQL_DICT['select_lot_num'].format(item_cd=item_cd, tgt_date=floor_date + datetime.timedelta(i)) for i
                  in
                  range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        df_ord_num = pd.read_sql(sql, sql_cli.conn)
        return df_ord_num

    def _fetch_ord_delivery_timing_and_lt(self, sql_cli, store_cd, item_cd, floor_date, upper_date):
        sql = SQL_DICT['select_ord_delivery_timing_and_lt'].format(item_cd=item_cd, store_cd=store_cd,
                                                                   floor_date=floor_date, upper_date=upper_date)
        df_ord = pd.read_sql(sql, sql_cli.conn)
        return df_ord

    def _fetch_inv_qty(self, sql_cli, store_cd, item_cd, prdct_date):
        sql = SQL_DICT['select_prdct_date_inv_qty_by_item'].format(store_cd=store_cd, item_cd=item_cd,
                                                                   prdct_date=prdct_date)
        df_inv_prdct_date = pd.read_sql(sql, sql_cli.conn)
        return df_inv_prdct_date


    def _postprocess(self):
        self._fetch_actual_sales()
        self._fetch_actual_inv()
        self._output()
        pass


if __name__ == '__main__':
    sbi = SimByItem()
    sbi.execute()
    print("END")
