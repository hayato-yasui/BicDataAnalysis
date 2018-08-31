import os
import pandas as pd
import numpy as np
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from multiprocessing import Process
from openpyxl import load_workbook
from scipy import stats
from Common.Logic.Preprocess import *
from Common.Logic.Postprocess import Postprocess
from Common.Logic.ChartClient import ChartClient
from Common.Setting.CausalAnalysisSetting import *
from Common.Setting.Common.PreprocessSetting import *
from Common.util import Util


# 店舗の現状を把握する為に加工したデータをExcelや図に出力するクラス
class CausalAnalysis:
    def __init__(self):
        self.chart_cli = ChartClient()
        self.preproc_s = PreprocessSetting()
        self.ca_s = CausalAnalysisSetting()
        self.preproc = Preprocess()
        self.sc = SrcConversion()
        self.gu = GroupingUnit()
        self.util = Util()
        self.mmt = MergeMasterTable()
        self.mmt_s = MergeMasterTableSetting()

    # def execute(self,tgt_store):
    def execute(self):
        tgt_store = ['大和乃山賊', '定楽屋', 'うおにく', 'かこい屋', 'くつろぎ屋', 'ご馳走屋名駅店', 'ご馳走屋金山店',
                     '九州乃山賊小倉総本店', '和古屋', '楽屋', '鳥Bouno!', 'ぐるめ屋']
        # tgt_store = ['sample', ]
        # tgt_store = ['大和乃山賊']
        for s in tgt_store:
            self.ca_s.TGT_STORE = self.preproc_s.TGT_STORE = s
            self.ca_s.OUTPUT_DIR = './data/OUTPUT/' + self.ca_s.TGT_STORE + '/'
            self.preproc_s.DATA_FILES_TO_FETCH = ['売上データ詳細_' + self.preproc_s.TGT_STORE + '_20180401-0630.csv', ]
            self.preproc_s.PROCESSED_DATA_DIR = './data/Input/processed_data/' + self.preproc_s.TGT_STORE + '/'

            self.df_preproc, preproc_csv_file_name = self._preprocess()

            # preproc_csv_file_name = ''
            # self.df_preproc = self.preproc.fetch_csv_and_create_src_df(self.preproc_s.PROCESSED_DATA_DIR
            #                                                            , [preproc_csv_file_name])
            # df_grouped_src = self.df_preproc.groupby(self.cols).mean().reset_index()
            # df_daily = df_grouped_src[self.cols]
            # self.util.df_to_csv(df_daily, self.ca_s.OUTPUT_DIR, '大和乃山賊＿サンプル.csv')

            # self._leveling_by_day_sales_up()
            df_leveled = self._leveling_sales(self.df_preproc)
            self.t_test(df_leveled, self.ca_s.T_TEST_TGT_COL, self.ca_s.T_TEST_DIFF_COL, self.ca_s.T_TEST_DIFF_CONDITION,True)

            print(s + " is finish")

    def _preprocess(self):
        df_src = self.preproc.common_proc(self.preproc_s)
        df_src = self.mmt.merge_store(df_src, self.mmt_s.F_PATH_STORE)
        df_src = self.mmt.merge_weather(df_src, self.mmt_s.DIR_WEATHER, self.preproc_s.TGT_PERIOD_FLOOR,
                                        self.preproc_s.TGT_PERIOD_TOP)
        df_src = self.mmt.merge_calender(df_src, self.mmt_s.F_PATH_CALENDER)
        df_src = self.preproc.calc_entering_and_exiting_time(df_src)
        # df_src = self.preproc.create_stay_presense(df_src, df_src.loc[0, '営業開始時間'], df_src.loc[0, '営業締め時間'])

        # self.preproc.dt_min_round(df_src, '注文時間', 10)
        # self.preproc.dt_min_round(df_src, '滞在時間', 20)
        preproc_csv_file_name = self.preproc.create_proc_data_csv(df_src, self.preproc_s.PROCESSED_DATA_DIR,
                                                                  self.preproc_s.TGT_STORE,
                                                                  self.preproc_s.TGT_PERIOD_FLOOR,
                                                                  self.preproc_s.TGT_PERIOD_TOP,
                                                                  memo=self.preproc_s.FILE_MEMO)

        return df_src, preproc_csv_file_name

    def t_test(self, df, index_col, diff_tgt_col, diff_condition,does_output_csv=False):
        df_t_test_rslt = pd.DataFrame(columns=['item', 'src_count','src_avg','tgt_count','tgt_avg','t', 'p'])
        df.set_index(index_col, inplace=True)
        for c in self.ca_s.CALC_TGT_COLS:
            df_src = df[df[diff_tgt_col] != diff_condition][c + '_平準化']
            df_tgt = df[df[diff_tgt_col] == diff_condition][c + '_平準化']

            for item in df.index.unique().tolist():
                # welch's t-test
                df_src_by_item = df_src[df_src.index == item]
                df_tgt_by_item = df_tgt[df_tgt.index == item]
                t, p = stats.ttest_ind(df_src_by_item, df_tgt_by_item, equal_var=False)
                df_t_test_rslt = df_t_test_rslt.append(pd.Series([item, df_src_by_item.count(),df_src_by_item.mean(),
                                                                  df_tgt_by_item.count(),df_tgt_by_item.mean(),t, p],
                                                                 index=df_t_test_rslt.columns),ignore_index=True).sort_values('p')
            if does_output_csv:
                self.util.df_to_csv(df_t_test_rslt, self.ca_s.OUTPUT_DIR, c + '_t検定.csv')

    def _leveling_sales(self, df_src):
        df_calc_src, calc_tgt_dict = self._calc_tgt_sales(self.ca_s.SUB_GROUP_COLS, self.ca_s.MAIN_GROUP_COLS,
                                                          self.ca_s.CALC_TGT_COLS, self.ca_s.DIFF_TGT_COL,
                                                          self.ca_s.DIFF_CONDITION)
        df_leveling_ratio = self._calc_sales_diff(df_calc_src, self.ca_s.CALC_TGT_COLS, does_output_csv=True)
        df_merged_ratio = pd.merge(df_src, df_leveling_ratio, on=self.ca_s.MAIN_GROUP_COLS)
        for c in calc_tgt_dict.keys():
            df_merged_ratio[c + '_平準化'] = df_merged_ratio.apply(
                lambda x: x[c] // x[c + '_増加率'] if x[self.ca_s.DIFF_TGT_COL] == self.ca_s.DIFF_CONDITION else x[c],
                axis=1)
        return df_merged_ratio

    def _calc_tgt_sales(self, sub_group_cols: list, main_group_cols: list, calc_tgt_cols: list, diff_tgt_col: str,
                        diff_condition):
        calc_tgt_dict = dict()
        for c in calc_tgt_cols:
            calc_tgt_dict.update({c: [c + '_sum', c + '_count']})
        df_grouped = self.df_preproc[sub_group_cols + [diff_tgt_col] + calc_tgt_cols]
        drop_cols = [c for c in sub_group_cols if c not in main_group_cols] + [diff_tgt_col]

        df_normal = df_grouped[df_grouped[diff_tgt_col] != diff_condition].groupby(
            sub_group_cols + [diff_tgt_col]).sum().reset_index().drop(drop_cols, axis=1)
        df_special = df_grouped[df_grouped[diff_tgt_col] == diff_condition].groupby(
            sub_group_cols + [diff_tgt_col]).sum().reset_index().drop(drop_cols, axis=1)
        dfs = [df_normal, df_special]
        for idx, df in enumerate(dfs):
            df = df.groupby(main_group_cols).agg(['sum', 'count']).reset_index()
            df.columns = ['_'.join(c) if c[1] != '' else c[0] for c in df.columns]
            for k, v_list in calc_tgt_dict.items():
                df[k + '_売上/日数'] = df[v_list[0]] / df[v_list[1]]
            dfs[idx] = df
        df_calc_src = pd.merge(dfs[0], dfs[1], on=main_group_cols, how='outer',
                               suffixes=('_normal', '_special')).set_index(main_group_cols)
        return df_calc_src, calc_tgt_dict

    def _calc_sales_diff(self, df_calc_src, calc_tgt_cols, does_output_csv=False):
        return_cols = []
        for c in calc_tgt_cols:
            # nan -> 1
            df_calc_src[c + '_増加率'] = (df_calc_src[c + '_売上/日数_special'] / df_calc_src[c + '_売上/日数_normal']).replace(
                np.nan, 1)
            return_cols.append(c + '_増加率')
        if does_output_csv:
            index_names = '_'.join(df_calc_src.index.names)
            [self.util.df_to_csv(df_calc_src[c], self.ca_s.OUTPUT_DIR, index_names + '_' + c + '.csv', True) for c in
             return_cols]
        return df_calc_src[return_cols].reset_index()


if __name__ == '__main__':
    ca = CausalAnalysis()
    ca.execute()
    print("END")
