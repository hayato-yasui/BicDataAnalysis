# -*- coding: utf-8; -*-
import os.path
import openpyxl as px
import pandas as pd
import csv
import datetime
import matplotlib.pyplot as plt
from sklearn.preprocessing import Imputer
from typing import List, Dict, Tuple

from Common.DB.sql import *
from Common.Logic.Preprocess import *


class Util:
    preproc = Preprocess()
    imr = Imputer(missing_values='NaN', strategy='most_frequent', axis=0)

    @staticmethod
    def create_dir(path):
        os.path.exists(os.mkdir(path))

    @staticmethod
    def check_existing_and_create_excel_file(file_path):
        if not os.path.exists(file_path):
            wb = px.Workbook()
            wb.save(file_path)

    @staticmethod
    def df_to_csv(df, dir, file_name, index=False):
        if not os.path.exists(dir):
            os.mkdir(dir)
        df.to_csv(dir + '/' + file_name, encoding='cp932', index=index)

    @staticmethod
    def datetime_to_date(df, column_li):
        for c in column_li:
            df[c] = df[c].dt.date
        return df

    @staticmethod
    def moving_average(df, col_name, period):
        df['avg_' + col_name] = df[col_name].rolling(window=period).mean()
        return df

    @staticmethod
    def create_prd_and_obj_df_or_values(df, Y_col, df_or_values='df', does_replace_dummy=False):
        # X = Predictor variable , y = Objective variable
        X = df.drop(Y_col, axis=1)
        y = df[Y_col]
        if does_replace_dummy:
            X = pd.get_dummies(X, prefix='', prefix_sep='')
        if df_or_values == 'values':
            X = X.values
            y = y.values
        return X, y

    def extract_tgt_itm_info(self, sql_cli, item_cd_li, tgt_date='2018/7/31', does_output=False, dir=None,
                             file_name=None) -> pd.DataFrame:
        item_cd = ','.join(["\'" + str(i) + "\'" for i in item_cd_li])
        sql = SQL_DICT['select_item_info'].format(item_cd=item_cd, tgt_date=tgt_date)
        df = pd.read_sql(sql, sql_cli.conn
                         # , index_col='HIRE_DATE'
                         # , parse_dates='HIRE_DATE'
                         )
        for c in df.columns:
            df[c] = df[c].astype(str)
        if does_output:
            self.df_to_csv(df, dir, file_name)
        return df

    def extract_tgt_ec_data(self, sql_cli, item_cd_li, logistics_cd_li, floor_date='2018/6/1', upper_date='2018/8/31',
                            does_output=False, dir=None, file_name=None) -> pd.DataFrame:
        logistics_cd = ','.join(["\'" + str(i) + "\'" for i in logistics_cd_li])
        item_cd = ','.join(["\'" + str(i) + "\'" for i in item_cd_li])
        sql = SQL_DICT['select_ec_trun_data'].format(item_cd=item_cd, logistics_cd=logistics_cd,
                                                     floor_date=floor_date, upper_date=upper_date)
        df = pd.read_sql(sql, sql_cli.conn
                         # , index_col='HIRE_DATE'
                         # , parse_dates='HIRE_DATE'
                         )
        if does_output:
            self.df_to_csv(df, dir, file_name)
        return df

    @staticmethod
    def csv_to_list(file_path):
        data = []
        with open(file_path, "r", encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                data.append(row)

        return data

    def select_ec_total_sales_by_chanel(self, sql_cli, floor_date=datetime.date(2018, 6, 1),
                                        upper_date=datetime.date(2018, 8, 31),
                                        does_output=False, dir=None, file_name=None) -> pd.DataFrame:
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        # sql = SQL_DICT['select_ec_total_sales_by_chanel_and_item'].format(tgt_date=tgt_date, upper_date=upper_date)
        sql = SQL_DICT['select_ec_total_sales_qty_by_chanel_and_item'].format(tgt_date=tgt_date, upper_date=upper_date)
        df = pd.read_sql(sql, sql_cli.conn)
        if does_output:
            self.df_to_csv(df, dir, file_name)
        return df

    def select_ec_sales_amount(self, sql_cli, item_cd_li, floor_date='2018/7/1', upper_date='2018/7/31',
                               does_output=False, dir=None, file_name=None,need_by_chanel=False) -> pd.DataFrame:
        item_cd = ','.join(["\'" + str(i) + "\'" for i in item_cd_li])
        sql = SQL_DICT['select_ec_sales_amount'].format(item_cd=item_cd, floor_date=floor_date, upper_date=upper_date)
        df = pd.read_sql(sql, sql_cli.conn)
        if need_by_chanel:
            sql = SQL_DICT['select_ec_sales_amount_by_chanel'].format(item_cd=item_cd, floor_date=floor_date,
                                                            upper_date=upper_date)
            df_by_chanel = pd.read_sql(sql, sql_cli.conn)
            df = pd.merge(df,df_by_chanel)
        if does_output:
            self.df_to_csv(df, dir, file_name)
        return df

    @staticmethod
    def select_jan_num_by_dept(sql_cli, floor_date='2018-7-1', upper_date='2018-7-31') -> pd.DataFrame:
        sql = SQL_DICT['select_jan_num_by_dept'].format(floor_date=floor_date, upper_date=upper_date)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def select_shortage_by_item(sql_cli, store_cd, dept_cd, floor_date=datetime.date(2018, 7, 1),
                                upper_date=datetime.date(2018, 7, 31)) -> pd.DataFrame:
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        sql = SQL_DICT['select_shortage_day_count'].format(store_cd=store_cd, dept_cd=dept_cd, tgt_date=tgt_date,
                                                           upper_date=upper_date)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def select_auto_ord_start_date(sql_cli, store_cd, item_cd_li, tgt_date='2018-7-31') -> pd.DataFrame:
        item_cd = ','.join(["\'" + str(i) + "\'" for i in item_cd_li])
        sql = SQL_DICT['select_auto_order_start_end_date'].format(store_cd=store_cd, item_cd=item_cd, tgt_date=tgt_date)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def select_sales_amount_by_item(sql_cli, store_cd, item_cd, floor_date=datetime.date(2017, 8, 1),
                                    upper_date=datetime.date(2018, 7, 31)) -> pd.DataFrame:
        sql_li = [SQL_DICT['select_sales_amount_by_item'].format(store_cd=store_cd, item_cd=item_cd,
                                                                 tgt_date=floor_date + datetime.timedelta(i)) for i in
                  range((upper_date - floor_date).days + 1)]
        sql = 'union all'.join(sql_li)
        df_sales = pd.read_sql(sql, sql_cli.conn)

        for i in range((upper_date - floor_date).days + 1):
            if len(df_sales[df_sales["日付"].astype(str) == str(floor_date + datetime.timedelta(i))]) == 0:
                df_no_sales = pd.DataFrame([(floor_date + datetime.timedelta(i), store_cd, item_cd, 0)],
                                           columns=['日付', 'store_cd', 'item_cd', '販売数'])
                df_sales = pd.concat([df_sales, df_no_sales])
            df_sales['日付'] = pd.to_datetime(df_sales.日付)
        return df_sales.sort_values("日付")

    @staticmethod
    def select_inv_by_item(sql_cli, store_cd, item_cd, floor_date=datetime.date(2017, 8, 1),
                           upper_date=datetime.date(2018, 7, 31)) -> pd.DataFrame:
        # tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
        #                range((upper_date - floor_date).days + 1)]
        # tgt_date = ','.join(tgt_date_li)
        # sql = SQL_DICT['select_inv_by_item'].format(store_cd=store_cd, item_cd=item_cd, tgt_date=tgt_date)

        sql_li = [SQL_DICT['select_inv_by_item'].format(store_cd=store_cd, item_cd=item_cd,
                                                        tgt_date=floor_date + datetime.timedelta(i)) for i in
                  range((upper_date - floor_date).days + 1)]
        sql = "union all".join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def select_ec_inv_by_item(sql_cli,df,floor_date,upper_date):
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        sql_li = [SQL_DICT['select_ec_inv_by_item'].format(
            store_cd=row[1]['store_cd'], item_cd=row[1]['item_cd'], tgt_date=tgt_date) for row in df.iterrows()]
        sql = 'union all '.join(sql_li)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def select_all_item_using_dept(sql_cli, store_cd, dept_cd_li, tgt_date='2018-7-31') -> pd.DataFrame:
        dept_cd = ','.join(["\'" + str(d) + "\'" for d in dept_cd_li])
        sql = SQL_DICT['select_all_auto_item_using_dept'].format(store_cd=store_cd, dept_cd=dept_cd, tgt_date=tgt_date)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def adjust_0_sales(df, upper_date, amount_col="販売数"):
        df.reset_index(inplace=True, drop=True)
        floor_date = df[:1]['日付'][0]
        upper_date = datetime.datetime.strptime(str(upper_date), '%Y-%m-%d')
        date_li = [floor_date + datetime.timedelta(i) for i in range((upper_date - floor_date).days + 1)]
        df_date = pd.DataFrame(date_li, columns=['日付'])
        df_merged = pd.merge(df_date, df, how='left', on='日付')
        for c in df.columns:
            if c in ["日付", amount_col]:
                continue
            df_merged[c] = df[:1][c][0]
        return df_merged.fillna(0)

    def select_ec_sales(self, sql_cli, item_cd_li, floor_date, upper_date, tgt_mall='all') -> pd.DataFrame:
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        if tgt_mall == 'all':
            sql_li = [SQL_DICT['select_ec_sales'].format(item_cd=item_cd, tgt_date=tgt_date) for item_cd in item_cd_li]
        else:
            chanel_cd_li = ['\'' + m + '\'' for m in tgt_mall]
            chanel_cd = ','.join(chanel_cd_li)
            sql_li = [SQL_DICT['select_tgt_mall_sales'].format(item_cd=item_cd, tgt_date=tgt_date, chanel_cd=chanel_cd)
                      for item_cd in item_cd_li]
        sql = "union all".join(sql_li)
        df_sales = pd.read_sql(sql, sql_cli.conn)
        if tgt_mall == 'all':
            df_grouped = df_sales.groupby(['item_cd', 'chanel_cd'])
        else:
            df_grouped = df_sales.groupby('item_cd')
        list_of_dfs = []
        for key, df_item in df_grouped:
            list_of_dfs.append(self.adjust_0_sales(df_item, upper_date))
        return pd.concat(list_of_dfs)

    def select_price_by_item(self, sql_cli, store_cd, item_cd_li, floor_date, upper_date) -> pd.DataFrame:
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        sql_li = [SQL_DICT['select_price_by_item'].format(store_cd=store_cd, item_cd=item_cd, tgt_date=tgt_date) for
                  item_cd in item_cd_li]
        sql = 'union all'.join(sql_li)
        df_price = pd.read_sql(sql, sql_cli.conn)
        df_grouped = df_price.groupby('item_cd')
        list_of_dfs = []
        for key, df_item in df_grouped:
            list_of_dfs.append(self.adjust_0_sales(df_item, upper_date, '売価'))
        return pd.concat(list_of_dfs)

    @staticmethod
    def select_calced_week_season_factor(df, sql_cli, floor_date, upper_date):
        df_f_weekend_season= pd.DataFrame
        df_adjusted = df[['store_cd', 'dept_cd']]
        df_adjusted = df_adjusted[~df_adjusted.duplicated(['store_cd', 'dept_cd'], keep=False)]
        dummy_date = ','.join(['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                               range((datetime.date(2018, 12, 31) - datetime.date(2018, 1, 1)).days + 1)])
        for row in df_adjusted.iterrows():
            for i in range((upper_date - floor_date).days + 1):
                sql = SQL_DICT['select_season_weekend_factor'].format(
                    store_cd=row[1]['store_cd'], dept_cd=row[1]['dept_cd'], tgt_date=floor_date + datetime.timedelta(i),
                    dummy_date=dummy_date)
                df_from_sql = pd.read_sql(sql, sql_cli.conn)
                if df_from_sql.empty:
                    continue
                if df_f_weekend_season.empty:
                    df_f_weekend_season = df_from_sql
                else:
                    df_f_weekend_season = df_f_weekend_season.append(df_from_sql)
        return df_f_weekend_season

    @staticmethod
    def extract_tgt_period(df, floor_date, upper_date):
        return df[df['日付'].between(floor_date, upper_date)]

    @staticmethod
    def shape_values(df):
        df['日付'] = pd.to_datetime(df['日付'])
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

    def csv_to_df(self, file_path, does_0_adjust=False, does_set_dtype=False):
        df = pd.read_csv(file_path, encoding='cp932', engine='python', dtype={'販売数': 'float'})
        if does_0_adjust:
            df = self.preproc.adjust_0_filled(df)
        if does_set_dtype:
            for c in df.columns:
                if c == '日付':
                    df[c] = pd.to_datetime(df[c])
                elif c == '販売数':
                    df[c] = df[c].astype(float)
        return df
