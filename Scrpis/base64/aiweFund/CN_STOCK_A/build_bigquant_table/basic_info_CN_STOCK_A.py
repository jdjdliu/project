import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import basic_info_CN_STOCK_A as category_info


class BuildBasic(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBasic, self).__init__(start_date, end_date)
        self.map_dic = {
            'pty_cd': 'pty_cd',   # 用来join pty_basicinfo表获取公司类型，名称，省份，成立日期字段
            'stk_cd': 'stk_cd',
            'instrument': 'instrument',
            'delst_dt': 'delist_date',
            'lst_brd': 'list_board',  # 需要映射处理
            'chn_sht_nm': 'name',
            'lst_dt': 'list_date',
        }
        self.pty_map_dic = {
            'pty_cd': 'pty_cd',
            'ecn_chr': 'company_type',  # 需要映射处理
            'chn_nm': 'company_name',
            'pvc': 'pvc_code',
            'found_dt': 'company_found_date',
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
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'delist_date': {'desc': '退市日期，如果未退市，则为 pandas.NaT', 'type': 'datetime64[ns]'},
                'company_type': {'desc': '公司类型', 'type': 'str'},
                'company_name': {'desc': '公司名称', 'type': 'str'},
                'company_province': {'desc': '公司省份', 'type': 'str'},
                'list_board': {'desc': '上市板', 'type': 'str'},
                'company_found_date': {'desc': '公司成立日期', 'type': 'datetime64[ns]'},
                'name': {'desc': '证券名称', 'type': 'str'},
                'list_date': {'desc': '上市日期', 'type': 'datetime64[ns]'}},
            'partition_date': None,
        }
        self.write_mode = write_mode
        self.alias = 'basic_info_CN_STOCK_A'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_all(table='STK_BASICINFO', fields=fields_lst)
        source_df = source_df[list(self.map_dic.keys())]

        if (not isinstance(source_df, pd.DataFrame)) or source_df.empty:
            return
        source_df = source_df[~source_df.instrument.str.startswith('A')]
        source_df = source_df[~source_df.instrument.str.startswith('T')]
        source_df['prefix'] = source_df.instrument.apply(lambda x: x[:1])
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df['pre_suf'] = source_df.prefix + source_df.suffix
        source_df = source_df[source_df.pre_suf.isin(['0SZA', '6SHA', '3SZA', '4BJA', '8BJA'])]

        # 001 - 主板，002 - 中小企业板，003 - 创业板，004 - 科创板，005 - 三板
        # source_df = source_df[source_df.lst_brd.isin(['001', '002', '003', '004', '005'])]  # 测试环境的BJ的lst_brd为None
        # source_df = source_df[source_df.lst_brd.isin(['001', '002', '003', '004'])]  # 丢弃三板市场的数据
        source_df = source_df[source_df.lst_brd != '005']  # 丢弃三板市场的数据
        source_df['lst_brd'] = source_df.lst_brd.map({'001': '主板', '002': '中小板', '003': '创业板',
                                                      '004': '科创板', '005': '三板'})
        source_df.loc[((source_df.lst_brd.isnull()) & (source_df.instrument.str.endswith("BJA"))), 'lst_brd'] = "北证"
        source_df = source_df.rename(columns=self.map_dic)

        pty_df = self._read_DataSource_all(table='PTY_BASICINFO', fields=list(self.pty_map_dic.keys()))
        if not isinstance(pty_df, pd.DataFrame):
            pty_df = pd.DataFrame(columns=list(self.pty_map_dic.keys()))
        pty_df = pty_df[list(self.pty_map_dic.keys())]
        # 002-国企，003-国有控股企业，004-集体所有制企业，005-民营企业，006-中外合资(中外合作)企业，007-外商独资企业，008-事业单位，999-其它；
        pty_df['ecn_chr'] = pty_df.ecn_chr.map({'002': '国企', '003': '国有控股企业', '004': '集体所有制企业',
                                                '005': '民营企业', '006': '中外合资(中外合作)企业', '007': '外商独资企业',
                                                '008': '事业单位', '999': '其它',
                                                '2': '国企', '3': '国有控股企业', '4': '集体所有制企业',
                                                '5': '民营企业', '6': '中外合资(中外合作)企业', '7': '外商独资企业',
                                                '8': '事业单位', '9': '其它'})
        pty_df = pty_df.rename(columns=self.pty_map_dic)

        area_cons_df = self._read_DataSource_all(table='REF_REGION', fields=['rgn_cd', 'chn_nm'])
        area_cons_df.rename(columns={'rgn_cd': 'pvc_code', 'chn_nm': 'company_province'}, inplace=True)
        pty_df = pd.merge(pty_df, area_cons_df, on=['pvc_code'], how='left')

        # 从主体表中获取公司的名称、省份、成立日期、公司类型，如果没有获取到就是None,而不会过滤掉
        source_df = pd.merge(source_df, pty_df, on=['pty_cd'], how='left')
        
        source_df = source_df[~(source_df.list_date.isnull())]
        # print(source_df[(source_df.company_found_date.isnull()) | (source_df.company_type.isnull()) | (source_df.company_province.isnull())])
        # print('----------------')
        # basic_info_CN_STOCK_A里面只获取上交所、深交所、北交所的数据：这个会影响后面的其他加工表过滤
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SHA', 'SZA', 'BJA'])]
        source_df = source_df[list(self.schema['fields'].keys())]
        self._update_data(source_df)


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
