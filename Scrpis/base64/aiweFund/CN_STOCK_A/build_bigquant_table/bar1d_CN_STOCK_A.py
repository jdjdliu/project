import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import bar1d_CN_STOCK_A as category_info


class BuildBar1d(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBar1d, self).__init__(start_date, end_date)
        self.map_dic = {
            'stk_cd': 'stk_cd',
            'instrument': 'instrument',
            'date': 'date',
            'adj_fct': 'adjust_factor',
            'amt': 'amount',
            'cls_prc_adj': 'close',
            'hi_prc_adj': 'high',
            'lo_prc_adj': 'low',
            'opn_prc_adj': 'open',
            'deals': 'deal_number',
            'trn_rat': 'turn',
            'vol': 'volume',
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
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},

                'close': {'desc': '收盘价(后复权)', 'type': 'float32'},
                'high': {'desc': '最高价(后复权)', 'type': 'float32'},
                'low': {'desc': '最低价(后复权)', 'type': 'float32'},
                'open': {'desc': '开盘价(后复权)', 'type': 'float32'},
                'adjust_factor': {'desc': '复权因子', 'type': 'float32'},
                'amount': {'desc': '交易额', 'type': 'float64'},
                'volume': {'desc': '交易量', 'type': 'float64'},
                'deal_number': {'desc': '成交笔数', 'type': 'float64'},
                'turn': {'desc': '换手率', 'type': 'float64'},
            },
        }
        self.write_mode = write_mode
        self.alias = 'bar1d_CN_STOCK_A'

    def run(self):
        # FDS表STK_EODPRICE对停牌做了插补处理
        # eg: 600072在2021-12-29 --2022-01-13停牌，但是交易日还是有数据
        # select * from aiwestk.stk_basicinfo where smb = '600072'
        # select * from aiwestk.stk_eodprice where stk_cd = 'SEC034247194' and trd_dt > '2022-01-01'
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='STK_EODPRICE', fields=fields_lst)
        source_df = source_df.rename(columns=self.map_dic)

        print('>>>>before filter shape: ', source_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument', 'list_date', 'delist_date'])
        source_df = pd.merge(source_df, basic_df, on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', source_df.shape)
        source_df = source_df[(source_df.date >= source_df.list_date) & ((source_df.date <= source_df.delist_date) | (source_df.delist_date.isnull()))]
        del source_df['list_date']
        del source_df['delist_date']
        print('>>>>basic list_date and delist_date filtered shape: ', source_df.shape)
        print('>>>>instrument len: ', len(source_df.instrument.unique()))
        for col in ['volume', 'deal_number', 'amount']:
            if col in source_df.columns:
                source_df[col].fillna(0, inplace=True)
        # add 20221101
        source_df = source_df[~(source_df.close.isnull())]
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SZA', 'SHA', 'BJA'])]
        print(source_df.groupby(['date']).instrument.count())
        print(source_df.sort_values(['instrument']))
        self._update_data(source_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildBar1d(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
