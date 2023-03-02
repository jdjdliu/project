import os
import sys
import time
import click
import pymysql
import datetime
import pandas as pd
# from bigdatasource.api import UpdateDataSource
from sdk.datasource import UpdateDataSource, DataSource


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
print(sys.path)
from CN_STOCK_A.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_STOCK_DIC
from common import change_fields_type
print(BASE_DIR)


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history, is_auto, stk_cd=None):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_STOCK_DIC['host'], port=PRO_STOCK_DIC['port'], user=PRO_STOCK_DIC['user'],
                                  password=PRO_STOCK_DIC['password'], database=PRO_STOCK_DIC['database'])
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
        self.stk_cd = stk_cd

    def _read_table_data(self):
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
        if self.stk_cd:
            if condition:
                condition = f"stk_cd in {str(tuple(self.stk_cd))} and " + condition
            else:
                condition = f"stk_cd in {str(tuple(self.stk_cd))}"

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
        params = 'stk_cd, smb, exg_cd'
        if code_lst:
            if len(code_lst) == 1:
                code_params = f"('{code_lst[0]}')"
            else:
                code_params = str(tuple(code_lst))
            sql_str = f"""
                    select {params} from {basic_table} where stk_cd in {code_params}
            """
            basic_df = self.select_data(table=basic_table, sql_str=sql_str)

        else:
            basic_df = pd.DataFrame(columns=params.split(','))
        return basic_df

    @staticmethod
    def add_instrument(tmp_df):
        tmp_df.loc[tmp_df.exg_cd == 'SZ', 'instrument'] = tmp_df.smb + '.SZA'
        tmp_df.loc[tmp_df.exg_cd == 'SH', 'instrument'] = tmp_df.smb + '.SHA'
        tmp_df.loc[tmp_df.exg_cd == 'BJ', 'instrument'] = tmp_df.smb + '.BJA'
        tmp_df.loc[tmp_df.exg_cd == 'NQ', 'instrument'] = tmp_df.smb + '.NQ'
        tmp_df.loc[tmp_df.exg_cd == 'SZHK', 'instrument'] = tmp_df.smb + '.SZHK'
        tmp_df.loc[tmp_df.exg_cd == 'SHHK', 'instrument'] = tmp_df.smb + '.SHHK'
        # 其中NQ-三板市场 SZHK-深港通中的深股通  SHHK-沪港通中的沪股通没有转换
        return tmp_df

    def calculate_col(self, df):
        # 将所有列转为小写
        df.columns = [x.lower() for x in df.columns]

        # 1.添加date列
        if self.date_format and self.date_field:
            print(self.date_field, self.date_format)
            df['date'] = df[self.date_field].tolist()
            df['date'] = pd.to_datetime(df['date'], format=self.date_format)

        # 2.添加instrument列:需要根据表来区分是否只有内部编码，需要从basicinfo表中join得到标准代码
        rel_lst = df.columns.tolist()
        if ('smb' in rel_lst) and ('exg_cd' in rel_lst):
            # df['instrument'] = df.smb
            df = self.add_instrument(df)
        else:
            basic_df = self._read_basic_smb(basic_table="AIWESTK.STK_BASICINFO", code_lst=df.stk_cd.unique().tolist())
            basic_df = self.add_instrument(basic_df)
            # 其中NQ-三板市场 SZHK-深港通中的深股通  SHHK-沪港通中的沪股通没有转换
            basic_df = basic_df[['stk_cd', 'instrument']]
            df = pd.merge(df, basic_df, on=['stk_cd'], how='left')

        # test venv code----------------------------------
        df = df[df.instrument.notnull()]
        # ------------------------------------------------

        # 李雄老师说的是exg_cd只有 SZ\SH\OF，但是还是需要加一个保护措施，如果有instrument为None的，生产需要raise；研发环境有脏数据
        error_df = df[df.instrument.isnull()]
        if not error_df.empty:
            print(error_df[['stk_cd', 'instrument', 'smb', 'exg_cd']])
            raise Exception('have instrument value is Null')

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
        # print(df.head(5))
        print(df.shape)
        if df.empty:
            return
        # add in 20221012------------
        print(self.date_field, self.end_date, datetime.datetime.today().strftime("%Y-%m-%d"))
        if self.date_field and (self.end_date >= datetime.datetime.today().strftime("%Y-%m-%d")):
            print("self.end_date is today, drop today data, keep T+1")
            print(f"before drop today: {df.shape}")
            df = df[df['date'] < pd.to_datetime(datetime.datetime.today())]        
            print(f"after drop today: {df.shape}")
        # ---------------------------

        UpdateDataSource().update(df=df, alias=self.alias, schema=self.schema, update_schema=True,
                                  write_mode='update')

        # add in 20220804--------
        import pickle
        base_path = '/var/app/data/bigquant/datasource/data_build/source_update_backup/'
        table_path = os.path.join(base_path, self.alias)
        if not os.path.exists(table_path):
            os.mkdir(table_path)
        second_tm = datetime.datetime.now().strftime("%H%M%S")
        file_path = os.path.join(table_path, f"{self.start_date}_{self.end_date}_{second_tm}.pkl")
        if not self.is_history:
            try:
                with open(file_path, mode="ab") as f:
                    pickle.dump(df, f) 
            except Exception as e:
                print(f"wirte pickle have error:\n {e}")
        # --------------------------

        print(f'>>>>>>>>{self.alias}update finish ...., update_shape: {df.shape}')
        print(df.dtypes)

    def _read_basic_stk_cd_all(self):
        sql_str = "select stk_cd from stk_basicinfo"
        basic_df = self.select_data(table='stk_basicinfo', sql_str=sql_str)
        return basic_df


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(3)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
@click.option("--alias", default=','.join(list(TABLES_MAPS.keys())), help="table_name")
@click.option("--is_history", default=False, help="is update history data")
@click.option("--is_auto", default=False, help="is auto to update data by read oracle database")
def entry(start_date, end_date, alias, is_history, is_auto):
# def entry(alias=','.join(list(TABLES_MAPS.keys())), start_date=datetime.datetime.now().strftime("%Y-%m-%d"), end_date=datetime.datetime.now().strftime("%Y-%m-%d"), is_history=False, is_auto=True):
    run_num = 200
    from sdk.datasource import DataSource
    alias_lst = alias.split(",")
    print(f'此次运行总共需要运行的表数量有：{len(alias_lst)}')
    for single_alias in alias_lst:
        if single_alias not in list(TABLES_MAPS.keys()):
            assert KeyError(f"{single_alias} not effective")
        print('>>>>>>>>start fetch table: ', single_alias)
        
        if single_alias in ['STK_EODPRICE', 'STK_VALUATION', 'STK_TA_VOLUME']:
            basic_df = DataSource("basic_info_CN_STOCK_A").read(fields=['stk_cd'])
            stk_cd_lst = basic_df.stk_cd.unique().tolist()
            # ===============================
            mysql_obj = MysqlFetch()
            mysql_obj.db = pymysql.connect(host=PRO_STOCK_DIC['host'], port=PRO_STOCK_DIC['port'], user=PRO_STOCK_DIC['user'],
                                  password=PRO_STOCK_DIC['password'], database=PRO_STOCK_DIC['database'])
            count_sql = f"select count(1) as count_num from aiwestk.{single_alias} where SYS_UPD_TM BETWEEN '{start_date + ' 00:00:00'}' and '{end_date + ' 23:59:59'}'"
            if not is_history:
                count_df = mysql_obj.select_data(table=single_alias, sql_str=count_sql)
                count_num = count_df.count_num.tolist()[0]
            else:
                count_num = 1

            if count_num <= 500000:
                run_num = len(stk_cd_lst) + 1
                all_flag = True
            else:
                all_flag = False
            # ===============================
            start = end = 0
            while end < len(stk_cd_lst) + 1:
                end += run_num
                tmp_lst = stk_cd_lst[start: end]
                start = end
                print(f'>> fetch split stk_cd: total {len(stk_cd_lst)}, now {end}')
                if all_flag:
                    tmp_lst = stk_cd_lst[:]
                ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history, is_auto, tmp_lst)
                ct_obj.run()
        else:
            ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history, is_auto)
            ct_obj.run()


if __name__ == "__main__":
    # entry(start_date='2021-01-01', alias='STK_BASICINFO', is_history=True)
    import warnings
    warnings.filterwarnings('ignore')
    entry()

