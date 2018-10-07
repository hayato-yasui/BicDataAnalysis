import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from Common.DB.SQLServer_Client import SQLServerClient
from AnalysisLogic.Resource.SQL.Rakuten_sql import *
from Common.DB.sql import *
from Common.util import Util
from Common.Setting.Rakuten_PointUPAnalysisSetting import *
from Common.Logic.Preprocess import *
from Common.Setting.Common.PreprocessSetting import *
from Common.Logic.SalesSpike import *
from Common.Logic.t_test import *


class RakutenPointUPAnalysis:
    def __init__(self):
        self.sql_cli = SQLServerClient()
        # self.sql_cli = None
        self.util = Util()
        self.rpa_s = RakutenPointUPAnalysisSetting()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()
        self.df_shortage = self.df_master = self.df_tgt_item = None
        self.preproc = Preprocess()

    def execute(self):
        # self.df_tgt_item = self._fetch_tgt_item()
        # df_sales_by_item = self._fetch_sales_by_item()
        # df_sales_by_item = self.mmt.merge_calender(df_sales_by_item, adjust_0=True)
        # self.util.df_to_csv(df_sales_by_item, self.rpa_s.OUTPUT_DIR, '楽天_JAN別日別販売数.csv')

        df_sales_by_item = pd.read_csv(self.rpa_s.OUTPUT_DIR + '楽天_JAN別日別販売数.csv', encoding='cp932',
                                       engine='python')
        df_sales_by_item = self.preproc.adjust_0_filled(df_sales_by_item)

        ss_sales_index_li = ['年月', '発注GP', 'store_cd', '部門コード', '部門名', '商品名', 'item_cd']
        df_tgt_sales_spike = SalesSpike(
            df_sales_by_item, ss_sales_index_li, dir=self.rpa_s.OUTPUT_DIR, file_name='楽天_JAN別日別販売数(スパイクフラグ付き).csv').execute()

        df_sales_spike = pd.read_csv(self.rpa_s.OUTPUT_DIR + '楽天_JAN別日別販売数(スパイクフラグ付き).csv'
                                     , encoding='cp932', engine='python')
        df_sales_spike['日付'] = pd.to_datetime(df_sales_spike['日付'], errors='coerce')

        Ttest(df_tgt_sales_spike, self.rpa_s.T_TEST_TGT_COL, self.rpa_s.T_TEST_DIFF_COL,
                    self.rpa_s.T_TEST_DIFF_CONDITION, True,dir=self.rpa_s.OUTPUT_DIR, file_name='t検定_販売数').execute()

    def _fetch_tgt_item(self):
        df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/Rakuten_PointUPAnalysis_dept.csv', encoding='cp932',
                                  engine='python')
        # df_tgt_dept = pd.read_csv('./data/Input/tgt_itm_cd/Rakuten_PointUPAnalysis_1dept.csv', encoding='cp932',
        #                           engine='python')
        df_tgt_dept = self.preproc.adjust_0_filled(df_tgt_dept)
        dept_li = df_tgt_dept["dept_cd"].tolist()
        return self.util.select_all_item_using_dept(self.sql_cli, '861', dept_li, tgt_date=self.rpa_s.TGT_UPPER_DATE)

    def _preprocess(self):
        pass

    # def t_test(self, df, index_col, diff_tgt_col, diff_condition, does_output_csv=False):
    #     df_t_test_rslt = pd.DataFrame(columns=['item', '普通の日_件数', '普通の日_avg', '特日_件数', '特日_平均販売数', 't', 'p'])
    #     df.set_index(index_col, inplace=True)
    #     for c in self.rpa_s.CALC_TGT_COLS:
    #         df_src = df[df[diff_tgt_col] != diff_condition][c]
    #         df_tgt = df[df[diff_tgt_col] == diff_condition][c]
    #
    #         for item in df.index.unique().tolist():
    #             # welch's t-test
    #             df_src_by_item = df_src[df_src.index == item]
    #             df_tgt_by_item = df_tgt[df_tgt.index == item]
    #             t, p = stats.ttest_ind(df_src_by_item, df_tgt_by_item, equal_var=False)
    #             df_t_test_rslt = df_t_test_rslt.append(pd.Series([item, df_src_by_item.count(), df_src_by_item.mean(),
    #                                                               df_tgt_by_item.count(), df_tgt_by_item.mean(), t, p],
    #                                                              index=df_t_test_rslt.columns),
    #                                                    ignore_index=True).sort_values('p')
    #         if does_output_csv:
    #             self.util.df_to_csv(df_t_test_rslt, self.rpa_s.OUTPUT_DIR, c + '_t検定.csv')

    def _fetch_sales_by_item(self, use_csv_data=False):
        if use_csv_data:
            df = pd.read_csv(self.rpa_s.OUTPUT_DIR + '楽天系モールの販売実績(201801-201808).csv', encoding='cp932',
                             engine='python')
            df = self.preproc.adjust_0_filled(df)
            df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
        else:
            tgt_date_li = ['\'' + str(self.rpa_s.TGT_FLOOR_DATE + datetime.timedelta(i)) + '\'' for i in
                           range((self.rpa_s.TGT_UPPER_DATE - self.rpa_s.TGT_FLOOR_DATE).days + 1)]
            tgt_date = ','.join(tgt_date_li)
            chanel_cd_li = ['\'' + c + '\'' for c in ['846', '253', '736', '089']]
            chanel_cd = ','.join(chanel_cd_li)
            sql_li = [RAKUTEN_SQL_DICT['select_sales_by_chanel_and_item'].format(
                tgt_date=tgt_date, upper_date=self.rpa_s.TGT_UPPER_DATE, chanel_cd=chanel_cd, item_cd=row[1]['item_cd'])
                for row in self.df_tgt_item.iterrows()]
            sql = 'union all'.join(sql_li)
            df = pd.read_sql(sql, self.sql_cli.conn)
        return df
        # df_grouped = df.groupby('item_cd')
        # list_of_dfs = []
        # for key, df_item in df_grouped:
        #     list_of_dfs.append(self.util.adjust_0_sales(df_item, self.rpa_s.TGT_UPPER_DATE))
        # return pd.concat(list_of_dfs)

    # def _extract_sales_spike(self, df_sales_by_item, a):
    #     df_sales_spike_by_item = self._calc_sales_spike(df_sales_by_item, a)
    #     return df_sales_spike_by_item
    #
    # def _calc_sales_spike(self, df, a=2):
    #     df_m = df.groupby(['年月', '発注GP', 'store_cd', '部門コード', '部門名', '商品名', 'item_cd'])
    #
    #     # 平均と標準偏差
    #     df_m_avg = df_m[['販売数']].mean().reset_index()
    #     df_m_std = df_m[['販売数']].std().reset_index()
    #     df_m_avg_std = pd.merge(df_m_avg.rename(columns={'販売数': 'μ'}), df_m_std.rename(columns={'販売数': 'σ'}))
    #
    #     df_m_avg_std['μ+' + str(a) + "σ"] = df_m_avg_std['μ'] + a * df_m_avg_std['σ']
    #     df = pd.merge(df, df_m_avg_std)
    #     if df.empty:
    #         return
    #     df['スパイク日'] = df.apply(lambda x: 0 if x['販売数'] < x["μ+" + str(a) + "σ"] or x['販売数'] == 0 else 1, axis=1)
    #     return df


if __name__ == '__main__':
    rpa = RakutenPointUPAnalysis()
    rpa.execute()
    print("END")
