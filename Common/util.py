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


class Util:
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

    def select_ec_total_sales_by_chanel(self, sql_cli, floor_date='2018/7/1', upper_date='2018/7/31',
                                        does_output=False, dir=None, file_name=None) -> pd.DataFrame:
        # sql = SQL_DICT['select_ec_total_sales_by_chanel'].format(floor_date=floor_date, upper_date=upper_date)
        sql = SQL_DICT['select_ec_total_sales_by_chanel_and_item'].format(floor_date=floor_date, upper_date=upper_date)
        df = pd.read_sql(sql, sql_cli.conn
                         # , index_col='HIRE_DATE'
                         # , parse_dates='HIRE_DATE'
                         )
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
    def select_price_by_item(sql_cli, store_cd, item_cd, floor_date=datetime.date(2017, 8, 1),
                             upper_date=datetime.date(2018, 7, 31)) -> pd.DataFrame:
        sql_li = [SQL_DICT['select_price_by_item'].format(store_cd=store_cd, item_cd=item_cd,
                                                          tgt_date=floor_date + datetime.timedelta(i)) for i in
                  range((upper_date - floor_date).days + 1)]
        sql = 'union all'.join(sql_li)
        df_price = pd.read_sql(sql, sql_cli.conn)

        for i in range((upper_date - floor_date).days + 1):
            if len(df_price[df_price["日付"].astype(str) == str(floor_date + datetime.timedelta(i))]) == 0:
                df_no_sales = pd.DataFrame([(floor_date + datetime.timedelta(i), store_cd, item_cd, 0)],
                                           columns=['日付', 'store_cd', 'item_cd', '売価'])
                df_price = pd.concat([df_price, df_no_sales])
            df_price['日付'] = pd.to_datetime(df_price.日付)
        return df_price.sort_values("日付")

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
        sql = SQL_DICT['select_all_item_using_dept'].format(store_cd=store_cd, dept_cd=dept_cd, tgt_date=tgt_date)
        return pd.read_sql(sql, sql_cli.conn)

    @staticmethod
    def adjust_0_sales(df, floor_date, upper_date):
        df = df.reset_index(drop=True)
        for date in [floor_date, upper_date]:
            date = datetime.datetime.strptime(str(date), '%Y-%m-%d')
            if len(df[df['日付'] == date]) == 0:
                df = pd.concat(
                    [df, pd.DataFrame([[date, 0]], columns=["日付", '販売数'])])
        df_day = df.set_index('日付')
        df_day = df_day.resample('D').mean().reset_index().fillna(0)
        df_merged = pd.merge(df_day, df, how='left', on=['日付','販売数'])
        for c in df.columns:
            if c in ["日付", '販売数']:
                continue
            df_merged[c] = df[:1][c][0]
        return df_merged.sort_values('日付')

    def select_ec_sales(self, sql_cli, item_cd_li, floor_date, upper_date) -> pd.DataFrame:
        tgt_date_li = ['\'' + str(floor_date + datetime.timedelta(i)) + '\'' for i in
                       range((upper_date - floor_date).days + 1)]
        tgt_date = ','.join(tgt_date_li)
        sql_li = [SQL_DICT['select_ec_sales'].format(item_cd=item_cd, tgt_date=tgt_date) for item_cd in item_cd_li]
        sql = "union all".join(sql_li)
        df_sales = pd.read_sql(sql, sql_cli.conn)
        df_all_sales = pd.DataFrame
        for item_cd in item_cd_li:
            df_tgt_item = df_sales[df_sales['item_cd'] == item_cd]
            for chanel_cd in list(set(df_tgt_item['chanel_cd'].tolist())):
                df_tgt_chanel = df_sales[(df_sales['item_cd'] == item_cd) & (df_sales['chanel_cd'] == chanel_cd)]
                if df_tgt_chanel.empty:
                    continue
                df_adjusted = self.adjust_0_sales(df_tgt_chanel, floor_date, upper_date)
                if df_all_sales.empty:
                    df_all_sales = df_adjusted
                else:
                    df_all_sales = df_all_sales.append(df_adjusted)
        return df_all_sales

