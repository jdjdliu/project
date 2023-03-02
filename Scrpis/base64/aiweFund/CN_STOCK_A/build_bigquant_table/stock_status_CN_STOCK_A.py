import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import stock_status_CN_STOCK_A as category_info


class BuildStockStatus(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildStockStatus, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'date_field': 'date',
            'active': True,
            "primary_key": ["date", "instrument"],
            "partition_date": "Y",
            "fields": {
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'},
                'price_limit_status': {'type': 'int8', 'desc': '股价在收盘时的涨跌停状态：1跌停，2未涨跌停，3涨停'},
                'suspended': {'type': 'bool', 'desc': '是否停牌'},
                'suspend_type': {'type': 'str', 'desc': '停牌类型'},
                'suspend_reason': {'type': 'str', 'desc': '停牌原因'},
                'st_status': {'type': 'int8', 'desc': 'ST状态：0：正常股票，1：ST，2：*ST'},    # ，11：暂停上市
            },
        }

        self.write_mode = write_mode
        self.alias = 'stock_status_CN_STOCK_A'

    def run(self):
        bar_df = self._read_DataSource_by_date(table='STK_EODPRICE', fields=['instrument', 'stk_cd', 'date',
                                                                             'prc_lmt_sts'])
        # 股价收盘时涨跌停状态price_limit_status: 1表示跌停，2表示未涨跌停，3则表示涨停 --> 涨跌停状态 prc_lmt_sts  001-涨停，002-非涨跌停，003-跌停；
        map_prc_status = {'001': 3, '002': 2, '003': 1}
        bar_df['price_limit_status'] = bar_df.prc_lmt_sts.map(map_prc_status)

        suspend_df = self._read_DataSource_by_date(table='STK_SUSPENSION', fields=['instrument', 'stk_cd', 'date',
                                                                                   'ssp_trm', 'ssp_rsn'])

        # 停牌类型suspend_type ----> 停牌期限 ssp_trm
        # 001-停牌一天，002-上午停牌，003-下午停牌，004-停牌半天，005-停牌一小时，006-停牌半小时，007-连续停牌，008-盘中停牌，009-暂停；
        map_sus_type = {
            '001': '停牌一天', '002': '上午停牌', '003': '下午停牌', '004': '停牌半天', '005': '停牌一小时', '006': '停牌半小时',
            '007': '连续停牌', '008': '盘中停牌', '009': '暂停'}
        suspend_df['suspend_type'] = suspend_df.ssp_trm.map(map_sus_type)
        suspend_df['suspended'] = True
        # 停牌原因suspend_reason --> 停牌原因 ssp_rsn
        # 001-股票价格异常波动停牌，002-召开股东大会停牌，003-公共媒体报道澄清停牌，004-违法违规被查停牌，005-违反交易所规则停牌，
        # 006-定期报告延期披露停牌，007-重大资产重组停牌，008-披露重大信息停牌，009-要约收购停牌，010-风险警示停牌，
        # 011-重大差错拒不改正停牌，012-股改事宜停牌，013-刊登股价敏感资料，999-其他；
        map_sus_reason = {
            '001': '股票价格异常波动停牌', '002': '召开股东大会停牌', '003': '公共媒体报道澄清停牌', '004': '违法违规被查停牌',
            '005': '违反交易所规则停牌', '006': '定期报告延期披露停牌', '007': '重大资产重组停牌', '008': '披露重大信息停牌',
            '009': '要约收购停牌', '010': '风险警示停牌', '011': '重大差错拒不改正停牌', '012': '股改事宜停牌',
            '013': '刊登股价敏感资料', '999': '其他'}
        suspend_df['suspend_reason'] = suspend_df.ssp_rsn.map(map_sus_reason)

        cal_df = pd.merge(bar_df, suspend_df, on=['stk_cd', 'instrument', 'date'], how='outer')
        cal_df['price_limit_status'] = cal_df.price_limit_status.fillna(2)   # 停牌且在日行情数据中没有的设置为未涨跌停
        cal_df['suspended'] = cal_df.suspended.fillna(False)

        st_df = self._read_DataSource_all(table="STK_SPCTREAT")
        # 001-ST，002-*ST，003-PT，004-退市整理，005-退市，006-正常交易，007-暂停上市，008-创业板暂停上市风险警示，999-其他；
        st_df = st_df[st_df.st_typ.isin(['001', '002'])]
        # 获取撤销日期小于self.end_date且撤销后类型不再是 001，002的，将st的end_date设置为 撤销日期-wdw_dt
        # 方式1：
        # st_start_date = pd.to_datetime(self.start_date)
        # st_end_date = pd.to_datetime(self.end_date)
        # st_df['end_date'] = st_end_date
        # st_df.loc[((st_df.wdw_dt < st_end_date) &
        #            (~(st_df.st_typ_nxt.isin(['001', '002'])))), 'end_date'] = st_df.wdw_dt
        # st_df = st_df[(st_df.end_date >= st_start_date) | (st_df.impl_dt <= st_end_date)]
        # st_df = st_df[['stk_cd', 'impl_dt', 'end_date', 'st_typ']]
        # st_df = st_df.sort_values(['stk_cd', 'impl_dt'])
        #
        # st_df['next_impl_dt'] = st_df.groupby(['stk_cd']).impl_dt.shift(-1)
        # st_df.loc[((st_df.next_impl_dt.notnull()) & (st_df.end_date >= st_df.next_impl_dt)),
        #           'end_date'] = st_df.next_impl_dt - datetime.timedelta(days=1)
        # 方式2：
        st_df = st_df[st_df.st_typ.isin(['001', '002'])]
        # 获取撤销日期小于self.end_date且撤销后类型不再是 001，002的，将st的end_date设置为 撤销日期-wdw_dt
        st_end_date = pd.to_datetime(self.end_date)
        st_df['end_date'] = st_end_date
        st_df.loc[(st_df.wdw_dt < st_end_date), 'end_date'] = st_df.wdw_dt
        st_df = st_df[['stk_cd', 'impl_dt', 'end_date', 'st_typ']]

        st_df = pd.merge(st_df, bar_df[['stk_cd', 'date']], on=['stk_cd'], how='right')
        st_df = st_df[(st_df.impl_dt <= st_df.date) & (st_df.end_date >= st_df.date)]
        st_df = st_df.drop_duplicates(['stk_cd', 'date'])
        rel_df = cal_df.merge(st_df[['stk_cd', 'date', 'st_typ']], on=['stk_cd', 'date'], how='left')
        rel_df['st_status'] = rel_df.st_typ.map({"000": 0, "001": 1, "002": 2})
        rel_df['st_status'] = rel_df.st_status.fillna(0)
        # rel_df = cal_df.merge(ins_df, on=['stk_cd', 'instrument', 'date'], how='left')
        # ----------------------------------------------------------------------------

        rel_df = rel_df[list(self.schema['fields'].keys())]

        print('>>>>before filter shape: ', rel_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument'])
        rel_df = pd.merge(rel_df, basic_df, on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', rel_df.shape)

        rel_df['suffix'] = rel_df.instrument.apply(lambda x: x.split('.')[1])
        rel_df = rel_df[rel_df.suffix.isin(['SZA', 'SHA', 'BJA'])]
        
        self._update_data(rel_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildStockStatus(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
