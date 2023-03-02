import os
import sys
import time
import click
import pymysql
import datetime
import pandas as pd
from sdk.datasource import UpdateDataSource


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from CN_FUND.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_FUND_DIC
from common import change_fields_type


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history, is_auto, id_lst=None):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_FUND_DIC['host'], port=PRO_FUND_DIC['port'], user=PRO_FUND_DIC['user'],
                                  password=PRO_FUND_DIC['password'], database=PRO_FUND_DIC['database'])
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
        self.id_lst = id_lst

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
        if self.id_lst:
            if condition:
                condition = f"fnd_cd in {str(tuple(self.id_lst))} and " + condition
            else:
                condition = f"fnd_cd in {str(tuple(self.id_lst))}"
        fields_lst = list(self.schema.get('fields').keys())
        if 'date' in fields_lst:
            fields_lst.remove('date')
        if 'instrument' in fields_lst:
            fields_lst.remove('instrument')
        params_str = ','.join(fields_lst)
        df = self.select_data(table=self.table, params=f'{params_str}', where_condition=condition)
        return df

    def _read_basic_smb(self, basic_table, fnd_cd_lst):
        params = 'fnd_cd, smb, exg_cd'
        if fnd_cd_lst:
            if len(fnd_cd_lst) == 1:
                fnd_cd_params = f"('{fnd_cd_lst[0]}')"
            else:
                fnd_cd_params = str(tuple(fnd_cd_lst))
            sql_str = f"""
                    select {params} from {basic_table} where fnd_cd in {fnd_cd_params}
            """
            basic_df = self.select_data(table=basic_table, sql_str=sql_str)

        else:
            basic_df = pd.DataFrame(columns=params.split(','))
        return basic_df

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
            df.loc[df.exg_cd == 'SZ', 'instrument'] = df.smb + '.ZOF'
            df.loc[df.exg_cd == 'SH', 'instrument'] = df.smb + '.HOF'
            df.loc[df.exg_cd == 'OF', 'instrument'] = df.smb + '.OFCN'
        else:
            basic_df = self._read_basic_smb(basic_table="AIWEFUND.FUND_BASICINFO", fnd_cd_lst=df.fnd_cd.unique().tolist())
            basic_df.loc[basic_df.exg_cd == 'SZ', 'instrument'] = basic_df.smb + '.ZOF'
            basic_df.loc[basic_df.exg_cd == 'SH', 'instrument'] = basic_df.smb + '.HOF'
            basic_df.loc[basic_df.exg_cd == 'OF', 'instrument'] = basic_df.smb + '.OFCN'
            basic_df = basic_df[['fnd_cd', 'instrument']]
            df = pd.merge(df, basic_df, on=['fnd_cd'], how='left')

        # test env code, pro env add too----------------------------------
        df = df[df.instrument.notnull()]
        # ------------------------------------------------

        # 李雄老师说的是exg_cd只有 SZ\SH\OF，但是还是需要加一个保护措施，如果有instrument为None的，生产需要raise；研发环境有脏数据
        error_df = df[df.instrument.isnull()]
        if not error_df.empty:
            print(error_df[['fnd_cd', 'instrument', 'smb', 'exg_cd']])
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
        UpdateDataSource().update(df=df, alias=self.alias, schema=self.schema, update_schema=True,
                                  write_mode='update')

        # add in 20220804--------
        import pickle
        import datetime
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
        print(df.dtypes)
        print(f'>>>>>>>>{self.alias}update finish ...., update_shape: {df.shape}')


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(3)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
@click.option("--alias", default=','.join(list(TABLES_MAPS.keys())), help="table_name")
@click.option("--is_history", default=False, help="is update history data")
@click.option("--is_auto", default=False, help="is auto to update data by read oracle database")
def entry(start_date, end_date, alias, is_history, is_auto):
# def entry(alias=','.join(list(TABLES_MAPS.keys())), start_date=datetime.datetime.now().strftime("%Y-%m-%d"), end_date=datetime.datetime.now().strftime("%Y-%m-%d"), is_history=False, is_auto=True):
    run_num = 500
    from sdk.datasource import DataSource
    alias_lst = alias.split(",")
    print(f'此次运行总共需要运行的表数量有：{len(alias_lst)}')
    for single_alias in alias_lst:
        if single_alias not in list(TABLES_MAPS.keys()):
            assert KeyError(f"{single_alias} not effective")
        print('>>>>>>>>start fetch table: ', single_alias)
        # if single_alias in ['FUND_NETVALUE', 'FUND_NETVALUE_MMF', 'FUND_EODPRICE', 'FUND_SHARES', 'FUND_PFL_SECLIST']:
        if (single_alias in ['FUND_PFL_SECLIST', 'FUND_NETVALUE', 'FUND_EODPRICE', 'FUND_NETVALUE_MMF', 'FUND_PFL_INDUSTRY',
                             'FUND_PFL_ASSET']) and (not is_history):  # change in 20220804
            basic_df = DataSource('FUND_BASICINFO').read(alias=['fnd_cd'])
            fnd_cd_lst = basic_df.fnd_cd.tolist()
            # ================================================
            mysql_obj = MysqlFetch()
            mysql_obj.db = pymysql.connect(host=PRO_FUND_DIC['host'], port=PRO_FUND_DIC['port'], user=PRO_FUND_DIC['user'],
                                  password=PRO_FUND_DIC['password'], database=PRO_FUND_DIC['database'])
            count_sql = f"select count(1) as count_num from aiwefund.{single_alias} where SYS_UPD_TM BETWEEN '{start_date + ' 00:00:00'}' AND '{end_date + ' 23:59:59'}'"
            count_df = mysql_obj.select_data(table=single_alias, sql_str=count_sql)
            print(count_df)
            if count_df.count_num.tolist()[0] <= 500000:
                run_num = len(fnd_cd_lst) + 1
                all_flag = True
            else:
                all_flag = False
            # ================================================
            start = end = 0
            while end < len(fnd_cd_lst) + 1:
                end += run_num
                tmp_lst = fnd_cd_lst[start: end]
                start = end
                print(f'>> fetch split fnd_cd: total {len(fnd_cd_lst)}, now {end}')
                if all_flag:
                    tmp_lst = []
                ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history, is_auto, tmp_lst)
                ct_obj.run()
        else:
            ct_obj = FetchMysqlDBData(single_alias, start_date, end_date, is_history, is_auto, [])
            ct_obj.run()


if __name__ == "__main__":
    # entry(start_date='2021-01-01', end_date='2021-01-01', alias='FUND_BASICINFO', is_history=True)
    import warnings
    warnings.filterwarnings('ignore')
    entry()

