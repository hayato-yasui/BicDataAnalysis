import pyodbc
from contextlib import contextmanager

import dataset
import pandas as pd
import numpy as np
from config.config import DBConfig


class SQLServerClient:
    def __init__(self):
        self.config = DBConfig()
        self.conn = self._connect()
        self.cur = self.conn.cursor()

    def _connect(self):
        self.default_schema = self.config.PG_MAIN_SCHEMA
        try:
            return pyodbc.connect(
                r'DRIVER='+self.config.DRIVER + ';'
                r'SERVER='+self.config.PG_HOST+';'
                r'DATABASE='+self.config.PG_DB+';'
                r'UID='+self.config.PG_MAIN_USER+';'
                r'PWD='+self.config.PG_MAIN_PASS+';'
                # r'MARS_Connection= yes'
            )
            # return pyodbc.connect(
            #     'DRIVER ={ODBC Driver 13 for SQL Server};SERVER = localhost\CPX-U6IAP7U7ID7\MSSQLSERVER2;DATABASE = Bic_EC;Trusted_Connection = yes ;')

        except Exception as e:
            raise e

    def execute_sql(self, query):
        try:
            result = self.cur.execute(query)
            return result
        except Exception as e:
            raise e

    def fetch_one(self, table_name, **kwargs):
        # self.logger.info('Fetch one from: %s' % table_name)
        # self.logger.info('Fetch one conditions: %s' % kwargs)
        result = self.__database.load_table(table_name).find_one(**kwargs)
        return result

    def fetch_all(self, table_name, **kwargs):
        # self.logger.info('Fetch all from: %s' % table_name)
        # self.logger.info('Fetch all conditions: %s' % kwargs)
        result = self.__database.load_table(table_name).find(**kwargs)
        return result

    def distinct(self, table_name, *args, **_filter):
        # self.logger.info(f'get {args} distinct value in filter {_filter} from table {table_name}')
        result = self.__database.load_table(table_name).distinct(*args, **_filter)
        return result

    def insert_many(self, table_name, rows):
        """
        Insert multiple records into table by method in dataset lib
        """
        # self.logger.info('Insert many into: %s' % table_name)
        self.__database.load_table(table_name).insert_many(rows, ensure=False)

    def count(self, table_name, **kwargs):
        # self.logger.info('Select count from: %s' % table_name)
        # self.logger.info('Count conditions: %s' % kwargs)
        result = self.__database.load_table(table_name).count(**kwargs)
        return result

    def set_schema(self, schema):
        # self.logger.info('set schema : {schema}'.format(schema=schema))
        self.__database.schema = schema

    def get_columns(self, table_name, schema=None) -> list:
        if schema is not None:
            self.__database.schema = schema
        return self.__database.load_table(table_name).columns

    # def get_col_def(self, table_name, verbose=False, schema=None):
    #     if schema is None:
    #         schema = self.default_schema
    #
    #     if verbose:
    #         selection = 'column_name, ordinal_position, data_type, is_nullable'
    #     else:
    #         selection = '*'
    #
    #     qstr = f"select {selection} " \
    #            f"from information_schema.columns " \
    #            f"where table_schema='{schema}' and " \
    #            f"table_name='{table_name}'" \
    #            f"order by ordinal_position;\n"
    #
    #     return self.execute_sql(qstr).next()

    @contextmanager
    def transaction(self):
        """
        Transaction context manager.
        See test code to know how to use this.
        This can handle nested transaction.
        """
        # self.logger.info('Begin a transaction.')
        if self._transaction_nest_count == 0:
            self.__database.begin()
        else:
            self._begin_nested()
        try:
            yield
            self.__database.commit()
            # self.logger.info('Commit the transaction')
        except Exception as e:
            self.__database.rollback()
            # self.logger.error(traceback.format_exc())
            # self.logger.info('Rollback the transaction.')
            raise e

    def _begin_nested(self):
        """
        Extends Dataset's begin()
        See https://dataset.readthedocs.io/en/latest/_modules/dataset/database.html#Database
        """
        if self.__database.in_transaction is False:
            # self.logger.error("begin_nested is called but not in a transaction now.")
            raise Exception
        self.__database.local.tx.append(self.__database.executable.begin_nested())

    @property
    def _transaction_nest_count(self) -> int:
        if self.__database.in_transaction is False:
            return 0
        else:
            return len(self.__database.local.tx)

    # def apply_csv_to_table(
    #         self, table_name, csv_file_path,
    #         if_exists='replace', index=False, skipinitialspace=True, *args, **kwargs):
    #     """
    #     Only for Test Data Loading!
    #     Wapper of pandas.DataFrame.to_sql()
    #     https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html#pandas.DataFrame.to_sql
    # 
    #     Uses pandas read_csv
    #     https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
    # 
    #     Note:
    #     - As default settings of to_sql: an existing table is replaced (if_exists='replace')
    #     - As default settings of read_csv: head line of CSV should be column name
    #     - Spaces after separator(,) are skipped (skipinitialspace=True)
    #     """
    # 
    #     column_dtype = {'prd_pln_id': np.str,
    #                     'ord_prcs_cd': np.str,
    #                     'prd_pln_data_type': np.str}
    # 
    #     # Don't use Pandas' 'replace'. It drops table. Instead, truncate table here.
    #     if if_exists == 'replace':
    #         self.execute_sql('CREATE TABLE IF NOT EXISTS {tbl} (dummy int);'.format(tbl=table_name))
    #         self.truncate(table_name)
    # 
    #     df = pd.read_csv(
    #         csv_file_path, skipinitialspace=skipinitialspace, *args, **kwargs, dtype=column_dtype
    #     )
    #     with self.transaction():
    #         df.to_sql(
    #             table_name, self.__database.engine,
    #             if_exists='append', index=index, schema=self.default_schema, *args, **kwargs
    #         )

    def run_sql_file(self, filename):
        """
        Only for Test Data Loading!
        :param filename:
        :return: None
        """
        with open(filename, 'r') as f:
            sql_text = f.read()
            self.execute_sql(sql_text)

#
# # This method is isolated because the function is a little different from other functions above
# def apply_tsv_to_table(filename, schema, tablename):
#     """
#     Only for Test Data Loading using tsvfile!
#     Header of tsv is unnecessary.
#     :param filename,tablename:
#     :return: None
#     """
#     config = ProdPlnConfigSelector.get_config_instance()
#     conn = psycopg2.connect(dbname=config['PG_DB'],
#                             user=config['PG_MAIN_USER'],
#                             host=config['PG_HOST'],
#                             password=config['PG_MAIN_PASS'],
#                             port=config['PG_PORT'])
#     cur = conn.cursor()
#     conn.set_isolation_level(0)
#     with open(filename, mode='r', encoding='utf-8') as f:
#         # Before execute copy process, you should replace null to \N and set delimiter using tab.
#         cur.copy_from(f, schema + "." + tablename, null='\\N', sep='\t')
#     conn.commit()
