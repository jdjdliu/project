import click
import datetime
import pandas as pd
from template import Build
from sdk.datasource import DataSource, D
from CN_STOCK_A.schema_catetory import index_constituent_CN_STOCK_A as category_info


class BuildConstituent(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildConstituent, self).__init__(start_date, end_date)
        self.map_dic = {

        }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            "primary_key": ["date", "instrument"],
            "partition_date": "Y",
            "fields": {
                'date': {'desc': '日期', 'type': 'datetime64'},
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'in_sse50': {'type': 'int8', 'desc': '是否属于上证50指数成份'},
                'in_csi300': {'type': 'int8', 'desc': '是否属于沪深300指数成份'},
                'in_csi500': {'type': 'int8', 'desc': '是否属于中证500指数成份'},
                'in_csi800': {'type': 'int8', 'desc': '是否属于中证800指数成份'},
                'in_sse180': {'type': 'int8', 'desc': '是否属于上证180指数成份'},
                'in_csi100': {'type': 'int8', 'desc': '是否属于中证100指数成份'},
                'in_szse100': {'type': 'int8', 'desc': '是否属于深证100指数成份'},
            },
        }

        self.write_mode = write_mode
        self.alias = 'index_constituent_CN_STOCK_A'

    def run(self):
        # 成分表：每次先读取全量
        source_df = self._read_DataSource_by_date(table='IDX_COMPONENTS',
                                                  fields=['idx_cd', 'cpn_cd', 'sel_dt', 'out_dt'],
                                                  start_date='1980-01-01')
        del source_df['instrument']
        # source_df = source_df[(source_df.sel_dt <= self.end_date) | (source_df.out_dt >= self.start_date)]

        # 读取加工后的basic_info_index_CN_STOCK_A; 指数的其它表的代码都应该在basic_info_index_CN_STOCK_A
        basic_index_df = self._read_DataSource_all(table='basic_info_index_CN_STOCK_A', fields=['instrument', 'idx_cd'])

        # 找到指数instrument 有 SWxxxxxx.HIX, 0xxxxx.HIX, 39xxxxx.ZIX
        source_df = pd.merge(basic_index_df, source_df, on=['idx_cd'], how='inner')

        # 只需要下面几个指数的成分信息
        index_dic = {'000300.HIX': 'in_csi300', '000903.HIX': 'in_csi100', '399330.ZIX': 'in_szse100',
                     '000010.HIX': 'in_sse180', '000016.HIX': 'in_sse50', '000906.HIX': 'in_csi800',
                     '000905.HIX': 'in_csi500'}
        source_df = source_df[source_df.instrument.isin(list(index_dic.keys()))]
        print('index have_data: ',source_df.instrument.unique().tolist())
        bar1d_df = self._read_DataSource_by_date(table='bar1d_CN_STOCK_A', fields=['stk_cd', 'instrument', 'date', 'close'])

        # ------------------------------
        trading_days = D.trading_days(start_date='2000-01-01', end_date=self.end_date)
        df_result = None
        null_index_lst = []
        for index in sorted(index_dic.keys()):
            print(index)
            df_list = []
            instrument_df = source_df[source_df.instrument == index]
            if instrument_df.empty:
                print(index, ' have no data....')
                null_index_lst.append(index_dic[index])
                continue
            for stk_cd, ins_df in instrument_df.groupby('cpn_cd'):
                for i in range(0, len(ins_df)):
                    st = str(ins_df['sel_dt'].iloc[i])
                    et = str(ins_df['out_dt'].iloc[i])
                    # st = st[:4] + '-' + st[4:6] + '-' + st[6:8]
                    if (et == 'None') or (et == 'NaT'):
                        df_tem = trading_days[trading_days.date >= st]
                    else:
                        # et = et[:4] + '-' + et[4:6] + '-' + et[6:8]
                        if et < self.start_date:
                            continue
                        df_tem = trading_days[trading_days.date.between(st, et)]
                    df_tem[index_dic[index]] = 1
                    df_tem['stk_cd'] = stk_cd
                    df_list.append(df_tem)
            df_tem = pd.concat(df_list, ignore_index=True)
            if df_result is None:
                df_result = df_tem
            else:
                df_result = df_result.merge(df_tem, on=['date', 'stk_cd'], how='outer')
        for col in null_index_lst:
            df_result[col] = None
        # df_result['instrument'] = df_result['instrument'].apply(lambda x: x + 'A')
        # df_result['instrument'] = df_result['instrument'].apply(lambda x: x + 'A' if 'A' not in x else x)
        df = bar1d_df.merge(df_result, on=['date', 'stk_cd'], how='left')
        df = df.fillna(0)
        df = df[~((df.in_csi100 == 0) & (df.in_csi300 == 0) & (df.in_csi500 == 0) & (df.in_csi800 == 0) & (
                df.in_sse180 == 0) & (df.in_sse50 == 0) & (df.in_szse100 == 0))]
        df = df[df.date.between(self.start_date, self.end_date)]

        df['suffix'] = df.instrument.apply(lambda x: x.split('.')[1])
        suffix_lst = df.suffix.unique().tolist()
        assert {'SHA', 'SZA', 'BJA'} >= set(suffix_lst), print(f'have other suffix: {suffix_lst}')
        self._update_data(df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildConstituent(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
