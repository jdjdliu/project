import os
import sys
import click
import pymysql
import datetime
import pandas as pd
from sdk.datasource import UpdateDataSource


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
print(sys.path)

from GLOBAL_TABLE.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_GLOBAL_DIC
from common import change_fields_type


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_GLOBAL_DIC['host'], port=PRO_GLOBAL_DIC['port'], user=PRO_GLOBAL_DIC['user'],
                                  password=PRO_GLOBAL_DIC['password'], database=PRO_GLOBAL_DIC['database'])
        self.alias = alias
        self.start_date = start_date
        self.end_date = end_date

        self.table = TABLES_MAPS[self.alias]['source_table']
        self.date_field = TABLES_MAPS[self.alias]["date"]
        self.date_format = TABLES_MAPS[self.alias]['date_format']
        self.instrument_name = TABLES_MAPS[self.alias]['instrument']
        self.schema = TABLES_MAPS[self.alias]['schema']
        self.is_history = is_history

    def _read_table_data(self):
        if self.alias in ['no table']: # ['REF_CALENDAR', 'REF_REGION']:  # 交易日历和常量表
            condition = ""   # 每次查询全量
        else:
            if self.date_field and self.is_history:
                start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").strftime(self.date_format)
                end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d").strftime(self.date_format)
                condition = f"{self.date_field} BETWEEN '{start_date}' AND '{end_date}'"
            elif self.is_history:
                condition = ""
            elif not self.is_history:
                condition = f"SYS_UPD_TM BETWEEN '{self.start_date + ' 00:00:00'}' AND " \
                            f"'{self.end_date + ' 23:59:59'}'"
            else:
                raise Exception('func: _read_db_data read_condition error')
        fields_lst = list(self.schema.get('fields').keys())
        if 'date' in fields_lst:
            fields_lst.remove('date')
        if 'instrument' in fields_lst:
            fields_lst.remove('instrument')
        params_str = ','.join(fields_lst)
        df = self.select_data(table=self.table, params=params_str, where_condition=condition)
        return df

    def calculate_col(self, df):
        # 将所有列转为小写
        df.columns = [x.lower() for x in df.columns]

        # 1.添加date列
        if self.date_format and self.date_field:
            print(self.date_field, self.date_format)
            print('-------------------')
            # 预防0001-01-01这类的数据
            df = df[(df[self.date_field] >= datetime.date(1800, 1, 1)) |
                    (df[self.date_field].isnull())]
            df['date'] = df[self.date_field].tolist()

            df['date'] = pd.to_datetime(df['date'], format=self.date_format)
            # 针对交易日历处理：最多获取未来一年的数据
            df = df[df.date <= (datetime.datetime.now() + datetime.timedelta(days=365))]

        # 2.添加instrument列：这里的instrument列只是为了给DataSource做过滤字段用，全局表中instrument不加后缀
        if self.instrument_name:
            df['instrument'] = df[self.instrument_name].tolist()

        schema_col = list(self.schema['fields'])
        data_col = df.columns.tolist()
        assert not list(set(schema_col) ^ set(data_col)), f"写入字段与schema定义不同，" \
                                                          f"schema: {schema_col}, data_col: {data_col}, " \
                                                          f"diff name: {list(set(schema_col) ^ set(data_col))}"
        return df

    def run(self):
        import datetime
        print('开始运行 run函数')
        # 调整开始时间
        # self.adjust_start_date()
        df = self._read_table_data()
        print(f'_read_db_data函数运行完成， df.shape：{df.shape}')
        if df.empty:
            print('have no data read from database')
            return
        print(df.dtypes)
        df = self.calculate_col(df)
        # print(df)
        df = change_fields_type(df, self.schema)
        print(df.shape)
        if df.empty:
            return
        # add in 20221031------------
        print(self.date_field, self.end_date, datetime.datetime.today().strftime("%Y-%m-%d"))
        if (self.alias not in ['REF_CALENDAR']) and self.date_field and (self.end_date >= datetime.datetime.today().strftime("%Y-%m-%d")):
            print("self.end_date is today, drop today data, keep T+1")
            print(f"before drop today: {df.shape}")
            df = df[df['date'] < pd.to_datetime(datetime.datetime.today())]        
            print(f"after drop today: {df.shape}")
        # ---------------------------

        UpdateDataSource().update(df=df, alias=self.alias, schema=self.schema, update_schema=True,
                                  write_mode='update')  # 每次全量更新用rewrite好像不会删文件夹，用full

        # add in 20220804--------
        import pickle
        base_path = '/var/app/data/bigquant/datasource/data_build/source_update_backup/'
        table_path = os.path.join(base_path, self.alias)
        if not os.path.exists(table_path):
            os.mkdir(table_path)
        import datetime
        second_tm = datetime.datetime.now().strftime("%H%M%S")
        file_path = os.path.join(table_path, f"{self.start_date}_{self.end_date}_{second_tm}.pkl")
        try:
            with open(file_path, mode="ab") as f:
                pickle.dump(df, f) 
        except Exception as e:
            print(f"wirte pickle have error:\n {e}")
        # --------------------------

        print(f'>>>>>>>>{self.alias}update finish ...., update_shape: {df.shape}')
        print(df.dtypes)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(3)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
@click.option("--alias", default=','.join(list(TABLES_MAPS.keys())), help="table_name")
@click.option("--is_history", default=False, help="is update history data")
def entry(start_date, end_date, alias, is_history):
# def entry(alias=','.join(list(TABLES_MAPS.keys())), start_date=datetime.datetime.now().strftime("%Y-%m-%d"), end_date=datetime.datetime.now().strftime("%Y-%m-%d"), is_history=False, is_auto=True, table_maps_num=None):
    alias_lst = alias.split(',')
    print(f'此次运行总共需要运行的表数量有：{len(alias_lst)}')
    for single_alias in alias_lst:
        if single_alias not in list(TABLES_MAPS.keys()):
            assert KeyError(f"{single_alias} not effective")
        print('>>>>>>>>start fetch table: ', single_alias)
        ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history)
        ct_obj.run()


if __name__ == "__main__":
    # entry(start_date='2021-01-01', alias='REF_CALENDAR', is_history=True)
    import warnings
    warnings.filterwarnings('ignore')
    entry()
