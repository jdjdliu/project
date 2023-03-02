import click
import datetime
import numpy as np
import pandas as pd
from sdk.datasource import DataSource, UpdateDataSource, D
from template import Build as TempBuild
from CN_STOCK_A.schema_catetory import financial_statement_ff_CN_STOCK_A as category_info
from CN_STOCK_A.build_bigquant_table.features_build.common_features import parallel

# start_date = args.start_date
# end_date = args.end_date
start_date = None
end_date = None


forward_days = 365 * 5


def remove_tzinfo(date):
    if isinstance(date, pd.Timestamp) or isinstance(date, datetime.datetime):
        return date.replace(tzinfo=None)
    else:
        return date


def truncate(df, date_col, start_date, end_date):
    # TODO: support minutes?
    return df[(df[date_col] >= remove_tzinfo(start_date)) & (df[date_col] <= remove_tzinfo(end_date))]


class Build(object):
    __cached_trading_days = {}

    def start(self, s_date, e_date):
        global start_date
        start_date = s_date
        global end_date
        end_date = e_date
        print(start_date, end_date, '>>>>>>>>>>>>>>>>>>>>>>')
        # self.build()
        self.build_ff()

    def build_ff(self):
        schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'partition_date': 'Y',
            'primary_key': ['date', 'instrument'],
            'fields':
                {
                    'date': {'desc': '日期', 'type': 'datetime64[ns]'},
                    'instrument': {'desc': '证券代码', 'type': 'str'},
                    'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                    'pty_cd': {'desc': '主体内部编码', 'type': 'str'},

                    'fs_quarter': {'desc': '财报对应的年份季度，如20151231', 'type': 'str'},
                    'fs_quarter_year': {'desc': '财报对应年份', 'type': 'int16'},
                    'fs_quarter_index': {'desc': '财报对应季度 1：一季报，2：半年报，3：三季报，4：年报，9：其他', 'type': 'int8'},
                    'fs_publish_date': {'desc': '公告日期', 'type': 'datetime64[ns]'},
                    # 在financial_statement_ff_CN_STOCK_A里面需要加fs_publish_date, 因为date是交易日期插补得到

                    'roa2': {'desc': '总资产报酬率', 'type': 'float64'},
                    'roa': {'desc': '总资产净利率', 'type': 'float64'},
                    'op_gr': {'desc': '利润率', 'type': 'float64'},
                    'pro_tog': {'desc': '营业总收入利润率', 'type': 'float64'},
                    'gc_gr': {'desc': '营业总成本率', 'type': 'float64'},
                    'ebit_gr': {'desc': '息税前利润同营业总收入比', 'type': 'float64'},
                    'gro_pro_mar': {'desc': '销售毛利率', 'type': 'float64'},
                    'net_pro_mar': {'desc': '销售净利率', 'type': 'float64'},
                    'cog_sal': {'desc': '销售成本率', 'type': 'float64'},
                    'yoy_eps_bas': {'desc': '同比增长率-基本每股收益(%)', 'type': 'float64'},
                    'yoy_eps_dil': {'desc': '同比增长率-稀释每股收益(%)', 'type': 'float64'},
                    'yoy_ocf': {'desc': '同比增长率-每股经营活动产生的现金流量净额(%)', 'type': 'float64'},
                    'yoy_op': {'desc': '同比增长率-营业利润(%)', 'type': 'float64'},
                    'yoy_ebt_tot': {'desc': '同比增长率-利润总额(%)', 'type': 'float64'},
                    'yoy_net_pro': {'desc': '同比增长率-归属母公司股东的净利润(%)', 'type': 'float64'},
                    'yoy_tr': {'desc': '同比增长率-营业总收入(%)', 'type': 'float64'},
                    'yoy_or': {'desc': '同比增长率-营业收入(%)', 'type': 'float64'},
                    'yoy_bps': {'desc': '相对年初增长率-每股净资产(%)', 'type': 'float64'},
                    'yoy_ass': {'desc': '相对年初增长率-资产总计(%)', 'type': 'float64'},
                    'yoy_equ_sha': {'desc': '相对年初增长率-归属母公司的股东权益(%)', 'type': 'float64'},
                    'acc_lia_pro': {'desc': '资产负债率(%)', 'type': 'float64'},
                    'ass_equ': {'desc': '权益乘数', 'type': 'float64'},
                    'lia_tot_equ': {'desc': '产权比率', 'type': 'float64'},
                    'ebit_int': {'desc': '已获利息倍数(EBIT/利息费用)', 'type': 'float64'},
                    'cur_rat': {'desc': '流动比(%)', 'type': 'float64'},
                    'qui_rat': {'desc': '速动比(%)', 'type': 'float64'},
                    'cas_rat': {'desc': '保守速动比率', 'type': 'float64'},
                    'cat_ass': {'desc': '流动资产占总资产比率', 'type': 'float64'},
                    'ocf_lia_cur': {'desc': '经营活动产生的现金流量净额占流动负债比率', 'type': 'float64'},
                    'equ_deb': {'desc': '归属于母公司的股东权益占总负债比率', 'type': 'float64'},
                    'ocf_deb': {'desc': '经营活动产生的现金流量净额同总负债比', 'type': 'float64'},
                    'inv_rn': {'desc': '存货周转率', 'type': 'float64'},
                    'art_rn': {'desc': '应收账款周转率', 'type': 'float64'},
                    'art_day': {'desc': '应收账款周转天数', 'type': 'float64'},
                    'fat_rn': {'desc': '固定资产周转率', 'type': 'float64'},
                    'cat_rn': {'desc': '流动资产周转率', 'type': 'float64'},
                    'ass_rn': {'desc': '总资产周转率', 'type': 'float64'},
                    'fcff': {'desc': '企业自由现金流量(FCFF)', 'type': 'float64'},
                    'wor_cap': {'desc': '营运资金', 'type': 'float64'},
                    'net_wor_cap': {'desc': '营运流动资本', 'type': 'float64'},
                    'eps_bas': {'desc': '基本每股收益', 'type': 'float64'},
                    'eps_dlt': {'desc': '稀释每股收益', 'type': 'float64'},
                    'bps': {'desc': '每股净资产', 'type': 'float64'},
                    'ocfps': {'desc': '每股经营活动产生的现金流量净额', 'type': 'float64'},
                    'gr_ps': {'desc': '每股营业总收入', 'type': 'float64'},
                    'orps': {'desc': '每股营业收入', 'type': 'float64'},
                    'sur_cap': {'desc': '每股资本公积', 'type': 'float64'},
                    'sur_res': {'desc': '每股盈余公积', 'type': 'float64'},
                    'und_ps': {'desc': '每股未分配利润', 'type': 'float64'},
                    'ret_ps': {'desc': '每股留存收益', 'type': 'float64'},
                    'cfps': {'desc': '每股现金流量净额', 'type': 'float64'},
                    'ebit_ps': {'desc': '每股息税前利润', 'type': 'float64'},
                    'fcff_ps': {'desc': '每股企业自由现金流量', 'type': 'float64'},
                    'fcfe_ps': {'desc': '每股股东自由现金流量', 'type': 'float64'},
                }

        }
        back_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') - datetime.timedelta(days=forward_days)
        back_date = back_date.strftime('%Y-%m-%d')
        df = DataSource('financial_statement_CN_STOCK_A').read(start_date=back_date, end_date=end_date)
        df['fs_publish_date'] = df.date.tolist()
        
        # df = df[df.instrument == '000001.SZA']
        # print("financial_df: ", '\n', df[df.instrument == '000001.SZA'])
        # uid = before_update({'table': 'financial_statements_ff_CN_STOCK_A'})
        # df = self.full_df
        print('got %s rows from financial_statement' % len(df))

        self.__start_date = start_date
        self.__end_date = end_date
        # self.__forward_fill_index = 0
        # df = df.sort_values(['instrument', 'fs_quarter'])
        df = df.sort_values(['instrument', 'fs_quarter', 'date'])
        df = df.drop_duplicates(['instrument', 'fs_quarter'], keep='last')  # 按时间排序有多个同一fs_quarter,保留最新的date

        df = df.drop_duplicates(['instrument', 'date'], keep='last')  # 同一date有多个不同fs_quarter的保留最新的fs_quarter

        df.set_index('date', inplace=True)

        # print("keep last financial_df", '\n', df)

        items = [_df for i, _df in df.groupby("instrument")]

        df_list = parallel.map(func=self._forward_fill_fs_for_instrument, items=items, processes_count=12,
                               show_progress=False, chunksize=40)
        df = pd.concat(df_list, ignore_index=True)
        # print('parralel df', '\n' , df)
        if not df['date'].is_monotonic_increasing:
            print('sort by date ..')
            df = df.sort_values(['date', 'instrument'])
        df = truncate(df, 'date', start_date, end_date)
        # print('result df', '\n',  df)
        # field names
        df.columns = [c if c in ['date', 'instrument', 'stk_cd', 'pty_cd'] else c + '_0' for c in df.columns]
        data_list = []
        df_basic = DataSource('basic_info_CN_STOCK_A').read()
        df_basic = df_basic[~df_basic.delist_date.isnull()]
        for i, j in df.groupby('instrument'):
            df_tem = df_basic[df_basic.instrument == i]
            if df_tem.empty:
                data_list.append(j)
            else:
                delist_date = df_tem.delist_date.iloc[0]
                list_date = df_tem.list_date.iloc[0]
                data_list.append(j[(j.date >= list_date) & (delist_date > j.date)])
        df = pd.concat(data_list, ignore_index=True)
        basic_df = DataSource("basic_info_CN_STOCK_A").read()[["instrument", "list_date", "delist_date"]]
        df = pd.merge(df, basic_df, on=['instrument'], how='left')
        df = df[(df.date >= df.list_date) & ((df.date <= df.delist_date) | (df.delist_date.isnull()))]
        del df['list_date']
        del df['delist_date']
        print(df.shape)
        print(df.head())
        table = 'financial_statement_ff_CN_STOCK_A'
        fields_dic = {}
        for col in schema['fields']:
            new_dic = schema['fields'].get(col)

            if col in ['instrument', 'date', 'stk_cd', 'pty_cd']:
                fields_dic[col] = new_dic
                continue
            else:
                # schema['fields'][col+'_0'] = new_dic
                fields_dic[col+'_0'] = new_dic
        schema['fields'] = fields_dic
        print(df)
        UpdateDataSource().update(df=df, alias=table, schema=schema)

    def _forward_fill_fs_for_instrument(self, df):
        if len(df) == 0:
            return df

        while True:
            df_s = df.shape
            if not df.index.is_monotonic_increasing:
                # 季报在年报前发了：已知如下三只 000047.SZ / 600752.SH / 600769.SH
                df = df[df.index < pd.Series(df.index).shift(-1).fillna(pd.to_datetime('2099-12-31'))]
            df_e = df.shape
            if df_s == df_e:
                break

        df = df.fillna(-np.inf, inplace=False)  # protect NaN
        # print("fill func")
        # print('before fill')
        df = df.reindex(index=pd.date_range(df.index.min(), self.__end_date), method='ffill')
        # print(df)

        df.index.name = 'date'
        df = df.replace(-np.inf, np.nan, inplace=False)  # restore NaN
        # merge: to remove non trading days
        # =========================================
        # try:
        #     self.__get_trading_days(df['instrument'].iloc[0])
        # except Exception as e:
        #     log.error("error in get trading days {} {}".format(e, df.shape))
        #     return df
        # tdays = self.__get_trading_days(df['instrument'].iloc[0])
        # =========================================
        tdays = D.trading_days(start_date=self.__start_date, end_date=self.__end_date)
        tdays = tdays[['date']]
        # print("tdays", "\n", tdays)
        df = df.merge(tdays, left_index=True, right_on='date', how='inner')
        # print(df)
        print('success _forward_fill_fs_for_instrument {} ... '.format(df.instrument.unique()))
        return df


@click.command()
@click.option('--start_date', default=(datetime.date.today() - datetime.timedelta(5)).isoformat(), help='start_date')
@click.option('--end_date', default=datetime.date.today().isoformat(), help='end_date')
def entry(start_date, end_date):
    Build().start(start_date, end_date)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    entry()
