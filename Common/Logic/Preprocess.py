import os
import re
import pandas as pd
import numpy as np
import datetime

from Common.Setting.Common.PreprocessSetting import *
from Common.DB.sql import *

class Preprocess:
    def __init__(self):
        self.sc = SrcConversion()

    def common_proc(self, setting):
        df = self.fetch_csv_and_create_src_df(setting.RAW_DATA_DIR, setting.DATA_FILES_TO_FETCH)
        # df = self.extract_data(df)

        # Data cleansing and convert data-type
        df = self.del_rec(df, self.sc.DEL_REC_DICT)
        self.del_unnecessary_cols(df, self.sc.UNNECESSARY_COLS_FOR_ALL_ANALYSIS)
        self.specific_data_correction(df)
        df = self.convert_dtype(df, self.sc.CONVERT_DTYPE)
        df = self.divide_col(df, self.sc.DIVIDE_NECESSARY_COLS)
        df = self.replace_values(df, self.sc.REPLACE_UNEXPECTED_VAL_TO_ALT_VAL,
                                 self.sc.REPLACE_NAN_TO_ALT_VAL)

        # df = self.deal_missing_values(df)
        # df = self.change_label_name(df)

        # Create new cols
        df = self.create_col_from_src_2cols(df, 'D.オーダー日時', 'H.伝票発行日', '注文時間')
        df = self.create_col_from_src_2cols(df, 'H.伝票処理日', 'H.伝票発行日', '滞在時間')
        df['客構成'] = self.create_cstm_strctr(df)
        df['男性比率'] = self.create_cstm_ratio(df)

        return df

    @staticmethod
    def fetch_csv_and_create_src_df(data_dir, file_names_li):
        for idx, f in enumerate(file_names_li):
            if idx == 0:
                df_src = pd.read_csv(data_dir + f, encoding='cp932', engine='python')
            else:
                df_src = pd.concat([df_src, pd.read_csv(data_dir + f, encoding='cp932', engine='python')])
        return df_src

    @staticmethod
    def del_unnecessary_cols(df, unnecessary_cols):
        return df.drop(columns=unnecessary_cols, axis=1, inplace=True)

    @staticmethod
    def del_rec(df, del_rec_dict):
        for k, v_list in del_rec_dict.items():
            if v_list is None:
                df = df[df[k].isnull() == True]
            else:
                df = df[df[k].isin(v_list) == False]
        return df

    @staticmethod
    def divide_col(df, divide_necessary_cols):
        # divide cols that has ID and name like 店舗ID:店舗名 -> 店舗ID,店舗名
        for c in divide_necessary_cols:
            df = pd.concat([df, df[c].str.split(':', expand=True)], axis=1).drop(c, axis=1)
            df.rename(columns={0: c + 'ID', 1: c + '名'}, inplace=True)
        return df

    @staticmethod
    def change_label_name(df):
        df.rename(columns=lambda s: s[2:] if s.count(".") else s, inplace=True)
        return df

    @staticmethod
    # ToDo:統合
    def deal_missing_values(df, method='interpolate'):
        if method == 'interpolate':
            df.interpolate(inplace=True)
        return df

    @staticmethod
    # ToDo:統合
    def replace_missing_value(df):
        df.fillna(0, inplace=True)
        df.replace([np.inf, -np.inf], 0, inplace=True)

    @staticmethod
    def extract_data(df, tgt_store, tgt_period_floor, tgt_period_top):
        # df = df[df['店舗名']==TGT_STORE]
        # df = df[tgt_period_floor <= df_['伝票処理日'] <= tgt_period_top]
        return df

    @staticmethod
    def create_proc_data_csv(df, proc_data_dir, tgt_store, tgt_period_floor, tgt_period_top, memo='', index=False):
        output_csv_file_name = tgt_store + str(tgt_period_floor) + '-' + str(tgt_period_top) + memo + '.csv'
        if not os.path.exists(proc_data_dir):
            os.mkdir(proc_data_dir)
        df.to_csv(proc_data_dir + output_csv_file_name, index=index, encoding='cp932')
        return output_csv_file_name

    @staticmethod
    def replace_values(df, unexpected_val_dict, nan_val_dict):
        for k, v_list in unexpected_val_dict.items():
            [df[k].replace(v[0], v[1], inplace=True) for v in v_list]
        [df[k].fillna(v, inplace=True) for k, v in nan_val_dict.items()]
        return df

    @staticmethod
    def convert_dtype(df, dict):
        for k, v in dict.items():
            if v == 'numeric':
                df[k] = pd.to_numeric(df[k], errors='coerce')
            elif v == 'datetime':
                df[k] = pd.to_datetime(df[k], errors='coerce')
            else:
                df[k] = df[k].astype(v)
        return df

    @staticmethod
    def grouping(df, key_li, grouping_item_and_way_dict, index_col=None):
        selected_cols = key_li + [k for k, v in grouping_item_and_way_dict.items()]
        df_selected = df[selected_cols]
        df_grouped_src = df_selected.groupby(key_li)
        df_grouped = df_grouped_src.agg(grouping_item_and_way_dict).reset_index()
        if index_col is not None:
            df_grouped = df_grouped.set_index(index_col)
        return df_grouped

    @staticmethod
    def tanspose_cols_and_rows(df, keys_li, tgt_cols_li, count_col):
        df_selected = df[keys_li + tgt_cols_li + [count_col]]
        df_pivot = df_selected.pivot_table(index=keys_li, columns=tgt_cols_li, values='D.数量', aggfunc=sum). \
            fillna(0).astype("int").reset_index()
        return df_pivot

    @staticmethod
    def outlier_2s(df):
        for i in range(len(df.columns)):
            # 列を抽出する
            col = df.iloc[:, i]

            # 平均と標準偏差
            average = np.mean(col)
            sd = np.std(col)

            # 外れ値の基準点
            outlier_min = average - (sd) * 2
            outlier_max = average + (sd) * 2

            # 範囲から外れている値を除く
            col[col < outlier_min] = None
            col[col > outlier_max] = None

        df.dropna(how='any', axis=0, inplace=True)
        return df

    @staticmethod
    def outlier_iqr(df):

        for i in range(len(df.columns)):
            # 列を抽出する
            col = df.iloc[:, i]

            # 四分位数
            q1 = col.describe()['25%']
            q3 = col.describe()['75%']
            iqr = q3 - q1  # 四分位範囲

            # 外れ値の基準点
            outlier_min = q1 - (iqr) * 1.5
            outlier_max = q3 + (iqr) * 1.5

            # 範囲から外れている値を除く
            col[col < outlier_min] = None
            col[col > outlier_max] = None

        df.dropna(how='any', axis=0, inplace=True)
        return df

    # sort_ways_li : ascending  -> True
    #                descending -> False
    @staticmethod
    def sort_df(df, sort_cols_li, sort_ways_li):
        return df.sort_values(sort_cols_li, ascending=sort_ways_li)

    @staticmethod
    def create_col_from_src_2cols(df, col1, col2, new_col, method='minus'):
        # method is selected in ('minus', 'plus', 'divide', 'times')
        df_tgt_cols = df[[col1, col2]]
        # df_tgt_cols.dropna(how='any', inplace=True)
        df_tgt_cols = df_tgt_cols.dropna(how='any')
        if method == 'minus':
            df[new_col] = df_tgt_cols[col1] - df_tgt_cols[col2]
        elif method == 'plus':
            df[new_col] = df_tgt_cols[col1] + df_tgt_cols[col2]
        elif method == 'divide':
            df[new_col] = df_tgt_cols[col1] / df_tgt_cols[col2]
        elif method == 'plus':
            df[new_col] = df_tgt_cols[col1] * df_tgt_cols[col2]
        else:
            raise ValueError
        return df

    @staticmethod
    def create_sec_col_from_src_2cols(df, col1, col2):
        return (df[col1] - df[col2]).dt.total_seconds()

    @staticmethod
    def convert_dtype_to_datetime(df, cols_li):
        [df[c].convert_objects().astype(np.datetime64) for c in cols_li]

    @staticmethod
    def convert_dtype_to_numeric(df, cols_li):
        [df[c].convert_objects(convert_numeric=True).astype(np.numeric) for c in cols_li]

    @staticmethod
    def dt_min_round(df, col, round_min):
        df[col] = df[col].dt.round(str(round_min) + 'min')

    @staticmethod
    def create_cstm_strctr(df):
        return "男 : " + df['H.客数（男）'].astype(str) + '人, 女 : ' + df['H.客数（女）'].astype(str) + '人'

    @staticmethod
    def create_cstm_ratio(df):
        return (df['H.客数（男）'] / df['H.客数（合計）']).round(2)

    @staticmethod
    def specific_data_correction(df):
        df['D.サブメニュー'] = df.apply(lambda x: x['D.商品'] if x['D.帳票集計対象商品'] not in ['Yes', 'No'] \
            else x['D.サブメニュー'], axis=1)

        df['D.商品'] = df.apply(lambda x: x['D.帳票集計対象商品'] if x['D.帳票集計対象商品'] not in ['Yes', 'No'] \
            else x['D.商品'], axis=1)

    @staticmethod
    def calc_entering_and_exiting_time(df):
        df['入店時間'] = df['H.伝票発行日'].apply(
            lambda x: int(x.strftime('%H%M')) + 2400 if int(x.strftime('%H%M')) < 1200 else int(x.strftime('%H%M')))
        df['退店時間'] = df['H.伝票処理日'].apply(
            lambda x: int(x.strftime('%H%M')) + 2400 if int(x.strftime('%H%M')) < 1200 else int(x.strftime('%H%M')))
        return df

    @staticmethod
    def create_stay_presense(df, start_time, end_time):
        curr_time = start_time
        while curr_time < end_time:
            if curr_time % 100 == 0:
                curr_time_plus30 = curr_time + 30
            else:
                curr_time_plus30 = curr_time + 70
            df[str(curr_time) + '-' + str(curr_time_plus30)] = 0
            df.loc[(df['入店時間'] <= curr_time_plus30) & (curr_time_plus30 <= df['退店時間']), str(curr_time) + '-' + str(
                curr_time_plus30)] = df['H.客数（合計）']
            curr_time = curr_time_plus30
        return df

    @staticmethod
    def adjust_0_filled(df):
        for c in df.columns.values:
            if c == 'item_cd':
                df[c] = df[c].astype(str).str.zfill(13)
            elif c == 'store_cd':
                df[c] = df[c].astype(str).str.zfill(3)
            elif c == 'distro_cd':
                df[c] = df[c].astype(str).str.zfill(3)
            elif c == 'supplier_cd':
                df[c] = df[c].astype(str).str.zfill(5)
        return df

    @staticmethod
    def fetch_item_info(sql_cli,store_cd, item_cd, floor_date,upper_date):
        sql_li = [SQL_DICT['select_supplier_cd'].format(store_cd=store_cd,item_cd=item_cd, tgt_date=floor_date + datetime.timedelta(i)) for i
                  in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def fetch_supplier_special_holiday(sql_cli,store_cd, item_cd, floor_date,upper_date):
        sql_li = [SQL_DICT['select_supplier_cd'].format(store_cd=store_cd,item_cd=item_cd, tgt_date=floor_date + datetime.timedelta(i)) for i
                  in range((upper_date - floor_date).days + 1)]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def merge_sales_by_chanel_group(df,amount_cols_li):
        df.drop(['chanel_cd',"店舗名称",'店舗属性','法人コード'],axis=1,inplace=True)
        index_li = df.columns.values.tolist()
        index_li.remove(amount_cols_li)
        return df.groupby(index_li).sum().reset_index()

class MergeMasterTable:
    folder_path = './data/Input/master/'
    def __init__(self):
        self.mmt_s = MergeMasterTableSetting()

    def merge_store(self, df_src, file_path):
        df_store = pd.read_csv(file_path, encoding='cp932', engine='python')
        df_store = df_store[self.mmt_s.NECESSARY_COLS]
        df_store['営業開始時間'] = df_store['営業開始時間'].str.replace(':', '').astype(int)
        df_store['営業締め時間'] = df_store['営業締め時間'].str.replace(':', '').astype(int)
        return pd.merge(df_src, df_store, left_on='H.店舗名', right_on='店舗名')

    def merge_weather(self, df_src, dir, floor_date, top_date, prefecture='all'):
        file_name = 'weather_' + str(floor_date).replace("-", "") + '-' + str(top_date).replace("-", "") + '.csv'
        df_weather = pd.read_csv(dir + file_name, encoding='cp932', engine='python')
        if prefecture != 'all':
            df_weather = df_weather[df_weather['都道府県'] == prefecture]
        df_weather['年月日'] = pd.to_datetime(df_weather['年月日'], errors='coerce')

        # Define rain flg
        df_weather['雨フラグ'] = df_weather.apply(lambda x: 1 if x['降水量の合計(mm)'] >= 5 else 0, axis=1)

        return pd.merge(df_src, df_weather, left_on=['都道府県', 'H.集計対象営業年月日'], right_on=['都道府県', '年月日']) \
            .drop('年月日', axis=1)

    def merge_calender(self, df_src, file_path, floor_date=None, top_date=None):
        df_calender = pd.read_csv(file_path, encoding='cp932', engine='python')
        if floor_date is not None and top_date is not None:
            df_calender = df_calender[df_calender['日付'].between(floor_date, top_date)]
        df_calender['日付'] = pd.to_datetime(df_calender['日付'], errors='coerce')
        df_calender['翌日が休日'] = df_calender.apply(lambda x: 1 if x['翌日が休日'] > 0 else 0, axis=1)
        return pd.merge(df_src, df_calender, left_on='H.集計対象営業年月日', right_on='日付').drop('日付', axis=1)

    def merge_chanel(self, df_src):
        df_chanel = pd.read_csv(self.folder_path + 'chanel.csv', encoding='cp932', engine='python')
        df_chanel['chanel_cd'] = df_chanel['chanel_cd'].astype(str).str.zfill(3)
        return pd.merge(df_src, df_chanel, how='inner', on='chanel_cd')
