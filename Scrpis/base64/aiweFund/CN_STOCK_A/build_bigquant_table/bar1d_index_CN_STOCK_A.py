import click
import datetime
import numpy as np
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import bar1d_index_CN_STOCK_A as category_info


class BuildBar1dIndex(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBar1dIndex, self).__init__(start_date, end_date)
        self.map_dic = {
            'idx_cd': 'idx_cd',
            # 'instrument': 'instrument',  # 不用原始表的instrument, 没后缀
            'trd_dt': 'date',
            'amt': 'amount',
            'cls_prc': 'close',
            'hi_prc': 'high',
            'lo_prc': 'low',
            'opn_prc': 'open',
            'vol': 'volume',
            # 'None':'adjust_factor',
            # 'None':'turn',
        }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'partition_date': 'Y',
            'primary_key': ['date', 'instrument'],
            'fields': {
                'idx_cd': {'desc': '指数内部编码', 'type': 'str'},
                'instrument': {'desc': '指数代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'},
                'amount': {'desc': '交易额', 'type': 'float64'},
                'close': {'desc': '收盘价', 'type': 'float64'},
                'high': {'desc': '最高价', 'type': 'float64'},
                'low': {'desc': '最低价', 'type': 'float64'},
                'open': {'desc': '开盘价', 'type': 'float64'},
                'volume': {'desc': '交易量', 'type': 'float64'},
            },
        }

        self.write_mode = write_mode
        self.alias = 'bar1d_index_CN_STOCK_A'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='IDX_EODVALUE', fields=fields_lst)
        del source_df['instrument']
        del source_df['date']

        source_df = source_df.rename(columns=self.map_dic)
        # 去basic_info_index_CN_STOCK_A中读取instrument(开头含有SW-HIX, 39-ZIX, 00-HIX)
        basic_index_df = self._read_DataSource_all(table='basic_info_index_CN_STOCK_A', fields=['idx_cd', 'instrument'])
        source_df = pd.merge(source_df, basic_index_df, on=['idx_cd'], how='inner')

        # !!!!原表中没有adjust_factor、turn；丢弃；bigquant线上adjust_factor全为1，turn有很多None
        # source_df['adjust_factor'] = 1
        # source_df['turn'] = np.nan

        source_df = source_df[list(self.schema['fields'].keys())]

        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['ZIX', 'HIX', 'BIX'])]  # 暂时无BIX
        source_df = source_df[~(source_df.close.isnull())]
        source_df = source_df[~(source_df.close == 0)]
        self._update_data(source_df)

        source_df['instrument'] = source_df.instrument.apply(lambda x: x.replace('HIX', 'SHA').replace('ZIX', 'SZA').replace('BIX', 'BJA'))
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SHA', 'SZA', 'BJA'])]  # 暂时无BJA

        self._update_data(source_df, alias='bar1d_CN_STOCK_A_index', schema=self.schema)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildBar1dIndex(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
