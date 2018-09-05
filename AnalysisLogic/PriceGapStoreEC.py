import pandas as pd
from Common.DB.SQLServer_Client import SQLServerClient
from Common.DB.sql import *
from Common.util import Util


class PriceGapStoreEC:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        self.util = Util()

    def execute(self):
        self.df_preproc = self._preprocess()
        print(self.df_preproc)

    def _preprocess(self):
        # df_itm = self._fetch_itm_master()
        df_itm = pd.read_csv('./data/Input/tgt_itm_cd/8JAN.csv', encoding='cp932', engine='python')
        self._fetch_trun_ec_data(df_itm)
        self._fetch_trun_store_data(df_itm)

    def _fetch_itm_master(self):
        item_cd_li = self.util.csv_to_list('./data/Input/tgt_itm_cd/V-Link_予測不足_JAN一覧_180716時点.xlsx')
        df = self.util.extract_tgt_itm_info(self.sql_cli, item_cd_li, does_output=True,
                                            dir='./data/Input/', file_name='85JAN.csv')
        return df

    def _fetch_trun_ec_data(self, df):
        logistics_cd_li = ['094']
        df = self.util.extract_tgt_ec_data(self.sql_cli, df['vcItemCd'].values.tolist(), logistics_cd_li, does_output=True,
                                           dir='./data/Input/', file_name='EC_trun_data.csv')
        return df

    def _fetch_trun_store_data(self, df):
        pass


if __name__ == '__main__':
    s = PriceGapStoreEC()
    s.execute()
    print("END")
