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
from CN_BOND.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_BOND_DIC
from common import change_fields_type


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history, is_auto):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_BOND_DIC['host'], port=PRO_BOND_DIC['port'], user=PRO_BOND_DIC['user'],
                                  password=PRO_BOND_DIC['password'], database=PRO_BOND_DIC['database'])
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
        """暂只获取对应的curv_cd收益率曲线"""
        start_condition = "std_trm in ('0.00000000','0.08000000','0.17000000','0.25000000','0.50000000','0.75000000','1.00000000','2.00000000','3.00000000','5.00000000','7.00000000','10.00000000','15.00000000','20.00000000','30.00000000','40.00000000')"

        if self.date_field and self.is_history:
            start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").strftime(self.date_format)
            end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d").strftime(self.date_format)
            #condition = f"{start_condition} AND {self.date_field} BETWEEN '{start_date}' AND '{end_date}'"
            if self.alias in ['FI_CURV_CNBD']:
                condition = f"{start_condition} AND {self.date_field} BETWEEN '{start_date}' AND '{end_date}'"
            else:
                condition = f"{self.date_field} BETWEEN '{start_date}' AND '{end_date}'"
        elif self.is_history:
            if self.alias in ['FI_CURV_CNBD']:
                condition = start_condition
            else:
                condition = ""
        elif not self.is_history:
            if self.alias in ['FI_CURV_CNBD']:
                condition = f"{start_condition} AND SYS_UPD_TM BETWEEN '{self.start_date + ' 00:00:00'}' AND " \
                            f"'{self.end_date + ' 23:59:59'}'"
            else:
                condition = f"SYS_UPD_TM BETWEEN '{self.start_date + ' 00:00:00'}' AND " \
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

    def _read_basic_smb(self, basic_table, code_lst):
        params = 'bnd_cd, bnd_smb, exg_cd'
        if code_lst:
            if len(code_lst) == 1:
                code_params = f"('{code_lst[0]}')"
            else:
                code_params = str(tuple(code_lst))
            sql_str = f"""
                    select {params} from {basic_table} where bnd_cd in {code_params}
            """
            basic_df = self.select_data(table=basic_table, sql_str=sql_str)

        else:
            basic_df = pd.DataFrame(columns=params.split(','))
        return basic_df

    @staticmethod
    def add_instrument(tmp_df):
        tmp_df.loc[tmp_df.exg_cd == 'SZ', 'instrument'] = tmp_df.bnd_smb + '.SZ'
        tmp_df.loc[tmp_df.exg_cd == 'SH', 'instrument'] = tmp_df.bnd_smb + '.SH'
        tmp_df.loc[tmp_df.exg_cd == 'IB', 'instrument'] = tmp_df.bnd_smb + '.IB'
        return tmp_df


    def calculate_col(self, df):
        # 将所有列转为小写
        df.columns = [x.lower() for x in df.columns]

        # 1.添加date列
        if self.date_format and self.date_field:
            print(self.date_field, self.date_format)
            df['date'] = df[self.date_field].tolist()
            df['date'] = pd.to_datetime(df['date'], format=self.date_format)

        # fi_curv_cnbd表直接添加curv_cd为instrument
        if self.alias in ['FI_CURV_CNBD','FI_CURV_SAMPLE']:
            df['instrument'] = df.curv_cd
        elif self.alias in ['IR_MNYMKTPRICE','IR_GLBIBOR']:
            df['instrument'] = df.ir_cd
        else:
            rel_lst = df.columns.tolist()
            if ('bnd_smb' in rel_lst) and ('exg_cd' in rel_lst):
                df = self.add_instrument(df)
            else:
                basic_df = self._read_basic_smb(basic_table="AIWEBOND.FI_BASICINFO", code_lst=df.bnd_cd.unique().tolist())
                basic_df = self.add_instrument(basic_df)
                # SZ,SH,IB
                basic_df = basic_df[['bnd_cd', 'instrument']]
                df = pd.merge(df, basic_df, on=['bnd_cd'], how='left')
        # test venv code----------------------------------
        df = df[df.instrument.notnull()]
        # ------------------------------------------------
        # 李雄老师说的是exg_cd只有 SZ\SH\OF，但是还是需要加一个保护措施，如果有instrument为None的，生产需要raise；研发环境有脏数据
        error_df = df[df.instrument.isnull()]
        if not error_df.empty:
            print(error_df[['bnd_cd', 'instrument', 'bnd_smb', 'exg_cd']])
            raise Exception('have instrument value is Null')

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
    # entry(start_date='2021-01-01',is_history=True)
    entry()
