import os
import sys
import time
import re
import click
import pymysql
import datetime
import pandas as pd
from sdk.datasource import UpdateDataSource


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
# print(sys.path)
from CN_MACRO.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_MACRO_DIC
from common import change_fields_type


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history, is_auto):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_MACRO_DIC['host'], port=PRO_MACRO_DIC['port'], user=PRO_MACRO_DIC['user'],
                                  password=PRO_MACRO_DIC['password'], database=PRO_MACRO_DIC['database'])
        self.alias = alias
        self.start_date = start_date
        self.end_date = end_date

        self.table = TABLES_MAPS[self.alias]['source_table']
        self.date_field = TABLES_MAPS[self.alias]["date"]
        self.date_format = TABLES_MAPS[self.alias]['date_format']
        self.instrument_name = TABLES_MAPS[self.alias]['instrument']
        self.schema = TABLES_MAPS[self.alias]['schema']
        self.is_history = is_history
        self.is_auto = is_auto

    def _read_table_data(self):
        """暂只获取对应的edb_code"""
        #start_condition = "edb_code in ('M0009808')"
        if self.date_field and self.is_history:
            start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").strftime(self.date_format)
            end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d").strftime(self.date_format)
            condition = f" {self.date_field} BETWEEN '{start_date}' AND '{end_date}'"
        elif self.is_history:
            condition = ""
        elif not self.is_history:
            condition = f" UPDATE_TIME BETWEEN '{self.start_date + ' 00:00:00'}' AND " \
                        f"'{self.end_date + ' 23:59:59'}'"
        else:
            raise Exception('func: _read_db_data read_condition error')

        # 只获取特定的字段，不select * from xxx
        fields_lst = list(self.schema.get('fields').keys())
        if 'date' in fields_lst:
            fields_lst.remove('date')
        if 'instrument' in fields_lst:
            fields_lst.remove('instrument')
        params_str = ','.join(fields_lst)
        df = self.select_data(table=self.table, params=f'{params_str}', where_condition=condition)
        return df

    def calculate_col(self, df):
        # 将所有列转为小写
        df.columns = [x.lower() for x in df.columns]

        # 1.添加date列
        if self.date_format and self.date_field:
            print(self.date_field, self.date_format)
            df['date'] = df[self.date_field].tolist()
            df['date'] = pd.to_datetime(df['date'], format=self.date_format)

        # MACRO_ZSYHEDB表直接添加curv_cd为edb_code
        if self.alias in ['MACRO_ZSYHEDB']:
            df['instrument'] = df.edb_code
        # test venv code----------------------------------
        df = df[df.instrument.notnull()]
        # ------------------------------------------------

        schema_col = list(self.schema['fields'])
        data_col = df.columns.tolist()
        assert not list(set(schema_col) ^ set(data_col)), f"写入字段与schema定义不同，" \
                                                          f"schema: {schema_col}, data_col: {data_col}, " \
                                                          f"diff name: {list(set(schema_col) ^ set(data_col))}"
        return df

    def run(self):
        print('开始运行 run函数')
        # 调整开始时间
        df = self._read_table_data()

        # if df.shape[0] > 1000000 and not (self.schema['partition_date']):
        #     raise Exception("to update Partition date")
        print(f'_read_db_data函数运行完成， df.shape：{df.shape}')
        if df.empty:
            print('have no data read from database')
            return
        print(df.dtypes)
        df = self.calculate_col(df)
        df = change_fields_type(df, self.schema)
        print(df.shape)
        if df.empty:
            return
        # print(df[['index_date', 'instrument', 'date', 'update_time', 'create_time']])
        UpdateDataSource().update(df=df, alias=self.alias, schema=self.schema, update_schema=True,
                                  write_mode='update')
        print(f'>>>>>>>>{self.alias}update finish ...., update_shape: {df.shape}')
        print(df.dtypes)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(3)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
@click.option("--alias", default=','.join(list(TABLES_MAPS.keys())), help="table_name")
@click.option("--is_history", default=False, help="is update history data")
@click.option("--is_auto", default=False, help="is auto to update data by read oracle database")
def entry(start_date, end_date, alias, is_history, is_auto):
# def entry(alias=','.join(list(TABLES_MAPS.keys())), start_date=datetime.datetime.now().strftime("%Y-%m-%d"), end_date=datetime.datetime.now().strftime("%Y-%m-%d"), is_history=False, is_auto=True):
    alias_lst = alias.split(",")
    print(f'此次运行总共需要运行的表数量有：{len(alias_lst)}')
    for single_alias in alias_lst:
        if single_alias not in list(TABLES_MAPS.keys()):
            assert KeyError(f"{single_alias} not effective")
        print('>>>>>>>>start fetch table: ', single_alias)
        ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history, is_auto)
        ct_obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    #entry(start_date='1949-01-01',is_history=False)
    entry()


