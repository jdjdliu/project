import os
import sys
import datetime
import pandas as pd
from sdk.datasource import DataSource, UpdateDataSource

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# print(BASE_DIR)
sys.path.append(BASE_DIR)
from common import change_fields_type


class Build:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.schema = None
        self.alias = None
        self.write_mode = None

    @staticmethod
    def _read_DataSource_all(table, fields=None):
        if fields:
            fields.append('sys_isvld')
            try:
                df = DataSource(table).read(fields=fields)
            except KeyError:
                df = DataSource(table).read(fields=fields + ['date'])
                del df['date']
        else:
            df = DataSource(table).read()
        if not isinstance(df, pd.DataFrame):
            print(f'read DataSource: {table} is None')
            return
        if 'sys_isvld' in df.columns.tolist():
            df = df[df.sys_isvld != 0]
            del df['sys_isvld']
        return df

    def _read_DataSource_by_date(self, table, fields=None, start_date=None, end_date=None):
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
        if fields:
            fields.append('sys_isvld')
            try:
                df = DataSource(table).read(start_date=start_date, end_date=end_date, fields=fields)
            except KeyError:
                df = DataSource(table).read(start_date=start_date, end_date=end_date, fields=fields + ['date'])
                del df['date']
        else:
            df = DataSource(table).read(start_date=start_date, end_date=end_date)
        if not isinstance(df, pd.DataFrame):
            print(f'read DataSource: {table} on {start_date}--{end_date} is None')
            return
        # 对于加工表xxx_CN_STOCK_A是没有sys_isvld字段的
        if 'sys_isvld' in df.columns.tolist():
            df = df[df.sys_isvld != 0]
            del df['sys_isvld']
        return df

    def _update_data(self, df, alias=None, schema=None):
        if alias is None:
            alias = self.alias
        if schema is None:
            schema = self.schema
        print('update data: ', 'alias: ', alias, 'write_mode: ', self.write_mode)
        print(schema)
        df = df[list(schema['fields'].keys())]
        df = change_fields_type(df, schema=schema)
        print(df.shape)
        print(df.dtypes)
        if self.write_mode and (not df.empty):
            UpdateDataSource().update(df=df, alias=alias, schema=schema, write_mode=self.write_mode)
