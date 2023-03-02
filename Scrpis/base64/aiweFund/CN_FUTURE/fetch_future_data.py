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
from CN_FUTURE.schema import TABLES_MAPS
from DB.db_handler import MysqlFetch
from DB.res import PRO_FUTURE_DIC
from common import change_fields_type


class FetchMysqlDBData(MysqlFetch):

    def __init__(self, alias, start_date, end_date, is_history, is_auto):
        super(FetchMysqlDBData, self).__init__()
        self.db = pymysql.connect(host=PRO_FUTURE_DIC['host'], port=PRO_FUTURE_DIC['port'], user=PRO_FUTURE_DIC['user'],
                                  password=PRO_FUTURE_DIC['password'], database=PRO_FUTURE_DIC['database'])
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

    @staticmethod
    def _conv_czce_symbol(symbol: str, trade_year_month: int):
        """
        转换郑商所品种代码日期4位：MA901 -> MA1901
        """
        if len(symbol) == 5:
            # 201905 -> 19, 202006 -> 20
            year_suffix = int(trade_year_month / 100) % 100
            # MA901 -> 9, MA009 -> 0
            symbol_year = int(symbol[2])
            min_diff = 10
            v = None
            for i in range(5):
                cur_suffix = i * 10 + symbol_year
                temp_diff = year_suffix - cur_suffix

                if cur_suffix >= year_suffix and abs(temp_diff) <= 1:
                    v = cur_suffix
                    break

                if abs(temp_diff) < min_diff and temp_diff >= -1:
                    min_diff = abs(temp_diff)
                    v = cur_suffix
            if v == 0:
                v = "00"
            symbol_whole = '{}{}{}'.format(symbol[:2], v, symbol[3:])
            return symbol_whole
        else:
            return symbol

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
        params = 'ftr_cd, smb, exg_cd, last_trd_dt'
        if code_lst:
            if len(code_lst) == 1:
                code_params = f"('{code_lst[0]}')"
            else:
                code_params = str(tuple(code_lst))
            sql_str = f"""
                    select {params} from {basic_table} where ftr_cd in {code_params}
            """
            basic_df = self.select_data(table=basic_table, sql_str=sql_str)

        else:
            basic_df = pd.DataFrame(columns=params.split(','))
        return basic_df

    def conv_czce_symbol(self, t_series):
        smb, last_trd_dt = list(t_series)
        if not last_trd_dt:
            return smb
        else:
            # 只处理三位数字结尾的，2位的不管，有可能是 近月、连一、连二这一类
            smb_year_month = re.findall('\d+$', smb)
            if smb_year_month and (len(smb_year_month[0]) == 3):
                res = self._conv_czce_symbol(smb, int(pd.to_datetime(last_trd_dt).strftime("%Y%m")))
                print(f"convert smb {smb} --> {res}, last_trd_dt is {last_trd_dt}")
                return res
            else:
                return smb

    def add_instrument(self, tmp_df):
        # CFFEX-中国金融期货交易所，SHF-上海期货交易所，INE-上海国际能源交易中心
        # DCE-大连商品交易所，CZCE-郑州商品交易所
        # IB-银行间市场
        exg_map_dic = {"CFFEX": "CFX", "SHF": "SHF", "INE": "INE", "DCE": "DCE", "CZCE": "CZC", "IB": "IB", 'GFEX': 'GFE'}

        tmp_df['suffix'] = tmp_df.exg_cd.map(exg_map_dic)
        tmp_df['instrument'] = tmp_df.smb
        # CZC smb 三位数字代码处理
        tmp_df.loc[tmp_df.suffix == 'CZC',
                   'instrument'] = tmp_df[['smb', 'last_trd_dt']].apply(lambda x: self.conv_czce_symbol(x), axis=1)

        print(tmp_df[tmp_df.suffix.isnull()])
        tmp_df = tmp_df[tmp_df.suffix.notnull()]  # 只保留五大期货交易所的
        tmp_df['instrument'] = tmp_df.instrument + "." + tmp_df.suffix
        tmp_df['instrument'] = tmp_df.instrument.str.upper()
        del tmp_df['suffix']
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
            df = self.add_instrument(df)
        else:
            basic_df = self._read_basic_smb(basic_table="AIWEFTROPT.FTR_BASICINFO", code_lst=df.ftr_cd.unique().tolist())
            basic_df = self.add_instrument(basic_df)
            basic_df = basic_df[['ftr_cd', 'instrument']]
            df = pd.merge(df, basic_df, on=['ftr_cd'], how='left')
        print('before instrument not null shape', df.shape)
        print(df[df.instrument.isnull()][['ftr_cd', 'instrument']].drop_duplicates())
        # print(df[df.instrument.isnull()][['ftr_cd', 'smb', 'exg_cd', 'last_trd_dt', 'eft_dt']])
        # test venv code----------------------------------
        df = df[df.instrument.notnull()]
        # ------------------------------------------------

        error_df = df[df.instrument.isnull()]
        if not error_df.empty:
            print(error_df[['ftr_cd', 'instrument', 'smb', 'exg_cd']])
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
        print(df.shape, '--------------')
 
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
    # entry(start_date='2021-01-01', alias='FTR_BASICINFO', is_history=True)
    import warnings
    warnings.filterwarnings('ignore')
    entry()


