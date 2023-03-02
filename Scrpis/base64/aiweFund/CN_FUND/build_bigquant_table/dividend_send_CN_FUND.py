import os
import sys
import click
import datetime
import numpy as np
import pandas as pd
from sdk.datasource import UpdateDataSource, DataSource

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from common import change_fields_type


class BuildDividend:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

        self.rename_map = {
            'fnd_cd': 'fnd_cd',
            'instrument': 'instrument',
            'date': 'date',
            'anc_dt_impl': 'imp_anndate',
            'bas_dt_inc_pay': 'base_date',
            'reg_dt': 'record_date',
            'anc_dt': 'ann_date',
            'dtrb_dt_on_exg': 'pay_date',   # 用的场内红利发放日，还有一个场外红利发放日可以用于构建场外分红信息
            'inc_pay_dt': 'earpay_date',    # INC_PAY_DT
            'xd_dt_on_exch': 'net_ex_date',  # 净值除权日,场内的用的场内除息日，这里可能存在争议
            'dvd_prgr': 'div_proc',
            'unt_dvd_bfr_tax': 'div_cash',
            'bas_lot': 'base_unit',
            'pay_dvd_amt': 'ear_distr',
            'act_dvd_amt': 'ear_amount',
            'acct_dt_reinv': 'account_date',
            'dvd_year': 'base_year',
                      }

        self.schema = {
            "desc": "基金(场内)分红",
            "active": True,
            "date_field": 'date',
            'category': '场内基金数据/行情数据',
            'rank': 1002006,
            "friendly_name": "基金(场内)分红",
            'primary_key': ['date', 'instrument', 'fnd_cd'],
            'fields': {
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'date': {'desc': '除息日', 'type': 'datetime64[ns]'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},

                'imp_anndate': {'desc': '分红实施公告日', 'type': 'datetime64[ns]'},
                'base_date': {'desc': '分配收益基准日', 'type': 'datetime64[ns]'},
                'record_date': {'desc': '权益登记日', 'type': 'datetime64[ns]'},
                'ann_date': {'desc': '公告日期', 'type': 'datetime64[ns]'},
                'pay_date': {'desc': '派息日', 'type': 'datetime64[ns]'},
                'earpay_date': {'desc': '收益支付日', 'type': 'datetime64[ns]'},
                'net_ex_date': {'desc': '净值除权日', 'type': 'datetime64[ns]'},
                'div_proc': {'desc': '方案进度', 'type': 'str'},
                'div_cash': {'desc': '每股派息(元)', 'type': 'float64'},
                'base_unit': {'desc': '基准基金份额(份)', 'type': 'float64'},
                'ear_distr': {'desc': '可分配收益(元)', 'type': 'float64'},
                'ear_amount': {'desc': '收益分配金额(元)', 'type': 'float64'},
                'account_date': {'desc': '红利再投资到账日', 'type': 'datetime64[ns]'},
                'base_year': {'desc': '份额基准年度', 'type': 'datetime64[ns]'},

                      },
            'version': '5.0',
            'partition_date': 'Y',
            'file_type': 'bdb'
        }

        self.alias = "dividend_send_CN_FUND"

    def _read_source_data(self):
        fields = list(self.rename_map.keys())
        fields.append('sys_isvld')
        df = DataSource('FUND_DIVIDEND').read(start_date=self.start_date, end_date=self.end_date, fields=fields)
        df = df[df.sys_isvld != 0]
        del df['sys_isvld']
        return df


    def run(self):
        # UpdateDataSource().update_metadata(alias=self.alias, schema=self.schema)
        print(f'start deal {self.start_date} -- {self.end_date} -----------------')
        source_df = self._read_source_data()
        if (source_df is None) or source_df.empty:
            return
        basic_df = DataSource('basic_info_CN_FUND').read(fields=['instrument', 'fnd_cd'])

        # 截取分红数据的instrument需要全在场内基金的basic_info里面---------
        source_df = pd.merge(source_df, basic_df, on=['instrument', 'fnd_cd'], how='inner')
        # ---------------------------------------------------------

        source_df = source_df.rename(columns=self.rename_map)
        # source_df['instrument'] = source_df.instrument.apply(lambda x: x.replace('SZ', 'ZOF').replace('SH', 'HOF'))
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        suffix_lst = source_df.suffix.unique().tolist()
        assert {'ZOF', 'HOF'} >= set(suffix_lst), print(f'have error suffix code: {suffix_lst}')

        source_df = source_df[list(self.schema['fields'].keys())]
        source_df['base_year'] = pd.to_datetime(source_df.base_year + '-01-01')
        print(source_df.sort_values('base_year'))
        source_df = change_fields_type(source_df, schema=self.schema)
        print(source_df)
        print(source_df.dtypes)
        if source_df.empty:
            return
        UpdateDataSource().update(df=source_df, alias=self.alias, schema=self.schema)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):

    obj = BuildDividend(start_date=start_date, end_date=end_date)
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()


