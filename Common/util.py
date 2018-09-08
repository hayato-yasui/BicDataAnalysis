# -*- coding: utf-8; -*-
import os.path
import openpyxl as px
import pandas as pd
import csv
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple

from Common.DB.sql import *


class Util:

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

    def extract_tgt_itm_info(self, sql_cli, item_cd_li, floor_date='2018/7/18', upper_date='2018/7/19',
                             does_output=False, dir=None, file_name=None) -> pd.DataFrame:
        item_cd = ','.join(["\'" + str(i) + "\'" for i in item_cd_li])
        sql = SQL_DICT['select_item_info'].format(item_cd=item_cd, floor_date=floor_date, upper_date=upper_date)
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
        sql = SQL_DICT['select_ec_total_sales_by_chanel'].format(floor_date=floor_date, upper_date=upper_date)
        df = pd.read_sql(sql, sql_cli.conn
                         # , index_col='HIRE_DATE'
                         # , parse_dates='HIRE_DATE'
                         )
        if does_output:
            self.df_to_csv(df, dir, file_name)
        return df



