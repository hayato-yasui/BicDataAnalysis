import os.path
import openpyxl as px
import pandas as pd
import matplotlib.pyplot as plt


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
