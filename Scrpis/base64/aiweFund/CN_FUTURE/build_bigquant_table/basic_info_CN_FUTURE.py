import re
import click
import datetime
import pandas as pd

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from CN_FUTURE.schema_category import basic_info_CN_FUTURE as category_info
from CN_STOCK_A.build_bigquant_table.template import Build


class BuildBasic(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBasic, self).__init__(start_date, end_date)
        self.map_dic = {
            'ftr_cd': 'ftr_cd',
            'instrument': 'instrument',
            'last_trd_dt': 'delist_date',
            'exg_cd': 'exchange',
            'last_dlv_dt': 'last_ddate',
            'eft_dt': 'list_date',
            'ctr_mul': 'multiplier',
            'chn_sht_nm': 'name',
            'quo_unt': 'price_tick_desc',
            'min_chg_prc': 'price_tick',
            # 'None': 'price_tick_desc',
            'obj_smb': 'product_code',
            'smb': 'trading_code',   # ????
        }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': None,
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'primary_key': ['instrument'],
            'active': True,
            'fields': {
                'ftr_cd': {'desc': '期货内部编码', 'type': 'str'},
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'delist_date': {'desc': '退市日期, 期指和期货主力默认为2099-01-01', 'type': 'datetime64[ns]'},
                'exchange': {'desc': '交易市场', 'type': 'str'},
                'last_ddate': {'desc': '最后交割日, 期指和期货主力默认为2099-01-01', 'type': 'datetime64[ns]'},
                'list_date': {'desc': '上市日期, 期指和期货主力默认为1993-01-01', 'type': 'datetime64[ns]'},
                'multiplier': {'desc': '合约乘数', 'type': 'float64'},
                'name': {'desc': '期货合约名称', 'type': 'str'},
                # 'symbol': {'desc': '交易标识', 'type': 'str'},
                'price_tick': {'desc': '报价单位', 'type': 'float64'},
                'price_tick_desc': {'desc': '报价单位说明', 'type': 'str'},
                'product_code': {'desc': '品种代码', 'type': 'str'},
                'trading_code': {'desc': '交易标识', 'type': 'str'},
            },
            'partition_date': None,
            'partition_field': None,  # 'product_code',
            "file_type": "bdb",
        }

        self.write_mode = write_mode
        self.alias = 'basic_info_CN_FUTURE'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        # 每次都是读取的源表全量，也需要读全量，因为要计算主力合约的list_date
        source_df = self._read_DataSource_all(table='FTR_BASICINFO')
        # 招商孙孟老师2023-01-09：wind源表还是没有对应于crt_sts的字段，只能依赖于财汇源补充该字段的值。现在财汇源没有工业硅的的相关数据，
        # 所以现在入不了相关值，只能为空
        gfe_df = source_df[source_df.exg_cd.isin(['GFEX', 'GFE'])]

        source_df = source_df[source_df.ctr_sts.isin(['002', '004'])]  # 002-正常上市，001-等待上市，004-终止上市

        gfe_df['instrument_num'] = gfe_df.instrument.apply(lambda x: x.split('.')[0][-4:].isdigit())
        gfe_df = gfe_df[gfe_df.instrument_num]
        gfe_df = gfe_df[fields_lst]
        print("--=-=-=-=-=-=-============")
        source_df = source_df[fields_lst]
        source_df = pd.concat([source_df, gfe_df], ignore_index=True)
        source_df = source_df.drop_duplicates()

        if (not isinstance(source_df, pd.DataFrame)) or source_df.empty:
            return

        source_df = source_df.rename(columns=self.map_dic)

        # basic_info_CN_FUTURE里面只获取正常六大交易所的数据：这个会影响后面的其他加工表过滤
        source_df = source_df[source_df.exchange.isin(['CZCE', 'SHF', 'DCE', 'CFFEX', 'INE', 'GFEX', 'GFE'])]
        source_df['price_tick'] = source_df.price_tick.apply(lambda x: re.findall("^[0-9\.]+", x)[0])
        # 对instrument做判断过滤
        source_df = source_df[list(self.schema['fields'].keys())]
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['CZC', 'SHF', 'DCE', 'CFX', 'INE', 'GFE'])]
        source_df['product_code'] = source_df.instrument.apply(lambda x: re.findall(r"\D+", x.split('.')[0])[0])

        # 计算主力合约：8888，指数合约0000，前复权主力：9999  对应的ftr_cd是最新合约的ftr_cd
        # 主力、指数的开始时间为第一个合约的开始时间; 但是其他的数据需要和最新的合约信息保持一致，如price_tick等会有变更的数据
        # his_df['instrument'] = his_df.instrument.apply(lambda x: x.split('.')[0])
        dominant_88_df = source_df.copy()
        min_list_df = dominant_88_df.groupby(['product_code'], as_index=False).list_date.min()

        dominant_88_df = dominant_88_df.sort_values(['product_code', 'list_date'])
        dominant_88_df = dominant_88_df[~dominant_88_df.product_code.isin(['IFL'])]
        dominant_88_df = dominant_88_df.groupby(['product_code']).tail(1)  # 拿到对应的ftr_cd是最新合约的ftr_cd
        del dominant_88_df['list_date']  # 通过min_list_df得到该品种最小的list_date作为主力的起始日期
        dominant_88_df = pd.merge(dominant_88_df, min_list_df, on=['product_code'], how='left')

        dominant_88_df['instrument'] = dominant_88_df.instrument.apply(lambda x: x.split('.')[0])

        # dominant_88_df = future_df.drop_duplicates(['product_code'])

        dominant_00_df = dominant_88_df.copy()
        dominant_99_df = dominant_88_df.copy()

        dominant_88_df['instrument'] = dominant_88_df.instrument.apply(lambda x: re.sub('\d+$', '8888', x))
        dominant_88_df['trading_code'] = dominant_88_df.trading_code.apply(lambda x: re.sub('\d+$', '8888', x))
        dominant_88_df['name'] = dominant_88_df.name.apply(lambda x: re.sub('\d+$', '主力', x))

        # 暂不构建：群里咨询的文韬，有需求再加上 on 2022-08-24
        # dominant_99_df['instrument'] = dominant_99_df.instrument.apply(lambda x: re.sub('\d+$', '9999', x))
        # dominant_99_df['trading_code'] = dominant_99_df.trading_code.apply(lambda x: re.sub('\d+$', '9999', x))
        # dominant_99_df['name'] = dominant_99_df.name.apply(lambda x: re.sub('\d+$', '前复权主力', x))

        dominant_00_df['instrument'] = dominant_00_df.instrument.apply(lambda x: re.sub('\d+$', '0000', x))
        dominant_00_df['trading_code'] = dominant_00_df.trading_code.apply(lambda x: re.sub('\d+$', '0000', x))
        dominant_00_df['name'] = dominant_00_df.name.apply(lambda x: re.sub('\d+$', '指数', x))

        dominant_df = pd.concat([dominant_88_df, dominant_00_df, dominant_99_df], ignore_index=True)

        dominant_df['delist_date'] = datetime.datetime(2099, 1, 1)
        dominant_df['last_ddate'] = datetime.datetime(2099, 1, 1)
        # dominant_df['list_date'] = datetime.datetime(1993, 1, 1)

        dominant_df.loc[dominant_df.product_code == 'WS', 'last_ddate'] = datetime.datetime(2013, 5, 31)
        dominant_df.loc[dominant_df.product_code == 'ME', 'last_ddate'] = datetime.datetime(2015, 5, 19)
        dominant_df.loc[dominant_df.product_code == 'ER', 'last_ddate'] = datetime.datetime(2013, 5, 27)
        dominant_df.loc[dominant_df.product_code == 'TC', 'last_ddate'] = datetime.datetime(2016, 4, 12)
        dominant_df.loc[dominant_df.product_code == 'RO', 'last_ddate'] = datetime.datetime(2013, 5, 17)
        dominant_df.loc[dominant_df.product_code == 'WT', 'last_ddate'] = datetime.datetime(2012, 11, 30)
        dominant_df['instrument'] = dominant_df.instrument + '.' + dominant_df.suffix

        rel_df = pd.concat([dominant_df, source_df])
        self._update_data(rel_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildBasic(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
