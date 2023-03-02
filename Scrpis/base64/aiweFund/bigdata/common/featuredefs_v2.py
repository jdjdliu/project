from bigdata.common.historydatafielddefs import HistoryDataFieldDefs
from bigdata.common.market import index_constituent_list
from bigdata.common.market import Market


class Transformer:
    def __init__( self, expr_s):
        self.__expr_s = expr_s
        self.__expr = None

    def __call__(self, x):
        if self.__expr_s is None:
            return x
        import numpy as np
        import numexpr as ne
        if self.__expr is None:
            # accelerate with numexpr
            s = self.__expr_s
            if not 'replace' in s and 'clip' not in s and not 'ne.' in s:
                s = s.replace('np.', '')
                s = s.replace('lambda x: ', '')
                self.__expr = lambda x: ne.evaluate(s)
            else:
                self.__expr = eval(s, {'ne': ne, 'np': np}, {})
        return self.__expr(x)

    def __repr__(self):
        return str(self.__expr_s)

BAR_FIELDS = ['close', 'open', 'high', 'low', 'volume', 'amount']
class _FEATURETUPE:
    def __init__(self, field, range, expr, desc, category, markets='all', base_field=None, field_group=1, type='float32'):
        self.field = field
        self.range = range
        self.expr = expr
        self.desc = desc
        self.category = category
        self.markets = self._parse_markets(markets)
        self.base_field = base_field
        self.field_group = field_group if not self.field.startswith('rank_') else field_group + 300
        self.type = type
        # serilize lambda for multiprocessing

    def _parse_markets(self,markets):
        if markets == 'all':
            markets = [x.symbol for x in Market.MG_ALL.sub_groups]
        elif isinstance(markets, str):
            markets = markets.split(',')
        elif not isinstance(markets, list):
            markets = [markets]
        return set(x if isinstance(x, str) else x.symbol for x in markets)

    def eval_expr(self, df, i, context=None):
        import pandas as pd, numpy as np
        if self.expr is None:
            return None
        if len(df) == 0:
            return pd.Series([])
        from talib import abstract as ta_abstract
        try:
            return eval(self.expr, ta_abstract.__dict__, {'df': df, 'i': i, 'context': context})
        except Exception as e:
            print('feature expr eval exception: %s, %s, %s' % (e, self.field, len(df)))
            return pd.Series([np.NaN] * len(df))

    def expand_fields(self, with_params=False, low_1=None, up_1=None, with_desc=False):
        if not self.range:
            return [(self.field, [])] if with_params else [self.field]

        # TODO: support more 2+D
        s = []
        for i in self.range:
            if low_1 is not None and i < low_1:
                continue
            if up_1 is not None and i >= up_1:
                continue
            if '$i' in self.field:
                name = self.field.replace('$i', str(i))
            elif self.field.endswith('_0'):
                name = self.field
            else:
                name = '%s_%s' % (self.field, i)
            if with_desc:
                s.append((name, self.desc.replace('i日','%s日'%i).replace('timeperiod=i','timeperiod=%s'%i).
                          replace('i个','%s个'%i)))
            else:
                s.append((name, [i]) if with_params else name)
        return s

def F(field, range, expr, desc, markets=None, base_field=None, type='float32'):
    if base_field is None and not field.startswith('rank_'):
        base_field = field
    if markets:
        return lambda category, default_markets, field_group: _FEATURETUPE(field, range, expr, desc, category, markets, base_field, field_group, type=type)
    else:
        return lambda category, default_markets, field_group: _FEATURETUPE(field, range, expr, desc, category, default_markets, base_field, field_group, type=type)

def H(field, expr=None, desc=None, rank=False, markets=None, range=[0], base_field=None, type='float32'):
    # make feature form history fields
    if expr is None: expr = 'df.%s' % field
    if base_field is None:
        base_field = field
    feature = F(field, range, expr, desc, markets=markets, base_field=base_field, type=type)
    if not rank:
        return feature
    rank_feature = F('rank_' + field, range, None, desc + '，升序百分比排名', markets=markets, base_field=None, type='float32')
    return [feature, rank_feature]

def R(field, expr=None, desc=None, markets=None, range=[0], base_field=None):
    # make feature form history fields
    if expr is None:
        expr = 'df.%s' % field
    return F('rank_' + field, range, None, desc + '，升序百分比排名', markets=markets, base_field=base_field, type='float32')



def __make(items):
    category = ''
    markets = 'all'
    results = []
    for item in items:
        if isinstance(item, str):
            if item.startswith('category:'):
                category = item[9:].strip()
            elif item.startswith('markets:'):
                markets = item[8:].strip()
            elif item.startswith('field_group:'):
                field_group = int(item[12:].strip())
        elif isinstance(item, list):
            results.extend([f(category, markets, field_group) for f in item])
        else:
            results.append(item(category, markets, field_group))
    return results
def _r(start, stop, step=1):
    return list(range(start, stop, step))


## NOTTE： 文档更新： bigdocs/docs/deps/updatedeps.py

R_0 = [0]
R_1 = [0, 1, 3, 4, 5, 6, 10, 15, 20]
R_2 = [0, 1, 3, 4, 5, 6, 10, 15, 20, 25] + _r(30, 61, 10)
R_3 = [0, 1, 3, 4, 5, 6, 10, 15, 20, 25] + _r(30, 121, 10)


#TRANSFORM_SLOG = ' (np.sign(x)*np.log(np.abs(x)).replace(-np.inf, 0) + 25) * 10000'
TRANSFORM_SLOG = 'ne.evaluate("(log(where(x > 1, x, where(x < -1, -1/x, 1))) + 30) * 10000")'
_FEATURES = __make([

    'category:基本信息',
    'markets:CN_STOCK_A',
    'field_group:1',
    F('list_days', R_0, '(df.date - df.list_date).dt.days', '已经上市的天数，按自然日计算', markets='CN_STOCK_A,US_STOCK,HK_STOCK', base_field='list_date'),
    H('list_board', expr='df.list_board.map({"主板": 1, "中小企业板": 2, "创业板": 3})', desc='上市板，主板：1，中小企业板：2，创业板：3'),
    F('company_found_date', R_0, '(df.date - df.company_found_date).dt.days', '公司成立天数'),

    'category:量价因子',
    'markets:all',
    'field_group:2',
    F('open', R_1, 'df.open.shift(i)', '第前i个交易日的开盘价'),
    F('high', R_1, 'df.high.shift(i)', '第前i个交易日的最高价'),
    F('low', R_1, 'df.low.shift(i)', '第前i个交易日的最低价'),
    F('volume', R_1, 'df.volume.shift(i)', '第前i个交易日的交易量', type='int64'),
    F('adjust_factor', R_1, 'df.adjust_factor.shift(i)', '第前i个交易日的复权因子', markets='CN_STOCK_A,CN_FUND,US_STOCK,HK_STOCK'),
    F('deal_number', R_1, 'df.deal_number.shift(i)', '第前i个交易日的成交笔数', markets='CN_STOCK_A', type='int32'),
    F('price_limit_status', [0], 'df.price_limit_status.shift(i)', '第前i个交易日的股价在收盘时的涨跌停状态，1表示跌停，2表示未涨跌停，3则表示涨停', markets='CN_STOCK_A,CN_FUND', type='int8'),
    F('close', R_3, 'df.close.shift(i)', '第前i个交易日的收盘价'),

    'field_group:3',
    F('return', R_3, 'df.close / df.close.shift(i + 1)', '过去i个交易日的收益', base_field='close'),
    F('rank_return', R_3, None, '过去i个交易日的收益排名'),

    'field_group:4',
    F('amount', R_1, 'df.amount.shift(i)', '第前i个交易日的交易额', markets='CN_STOCK_A,HK_STOCK'),
    F('rank_amount', R_1, None, '第前i个交易日的交易额百分比排名'),
    F('avg_amount', R_2, 'df.amount.rolling(i + 1).mean()', '过去i个交易日的平均交易额', base_field='amount'),
    F('rank_avg_amount', R_2, None, '过去i个交易日的平均交易额，百分比排名'),

    'category:换手率因子',
    'markets:CN_FUND,CN_STOCK_A,US_STOCK,HK_STOCK',
    'field_group:5',
    F('turn', R_1, 'df.turn.shift(i)', '第前i个交易日的换手率'),
    F('rank_turn', R_1, None, '过去i个交易日的换手率排名'),
    F('avg_turn', R_1, 'df.turn.rolling(i+1).mean()', '过去i个交易日的平均换手率', base_field='turn'),
    F('rank_avg_turn', R_1, None, '过去i个交易日的平均换手率排名'),

    'category:估值因子',
    'markets:CN_STOCK_A,US_STOCK,HK_STOCK',
    'field_group:6',
    R('market_cap', desc='总市值', base_field='market_cap'),
    R('market_cap_float', desc='流通市值', base_field='market_cap_float'),
    R('pe_ttm', desc='总市值', base_field='pe_ttm'),
    R('pe_lyr', desc='市盈率 (LYR)', base_field='pe_lyr'),
    R('pb_lf', markets='CN_STOCK_A',desc='市净率 (LF)', base_field='pb_lf'),
    R('ps_ttm', desc='市销率 (TTM)', base_field='ps_ttm'),

    'category:资金流',
    'markets:CN_STOCK_A',
    'field_group:7',
    F('mf_net_amount', R_1, 'df.mf_net_amount.shift(i)', '第前i个交易日净主动买入额，= 买入金额 - 卖出金额 (包括超大单、大单、中单或小单)'),
    F('avg_mf_net_amount', R_1, 'df.mf_net_amount.rolling(i).mean()', '过去i个交易日平均净主动买入额', base_field='mf_net_amount'),
    F('rank_avg_mf_net_amount', R_1, 'df.mf_net_amount.rolling(i).mean()', '过去i个交易日平均净主动买入额排名'),

    'category:财务因子',
    'markets:CN_STOCK_A',
    'field_group:8',
    R('fs_net_profit_yoy_0', desc='归属母公司股东的净利润同比增长率'),
    R('fs_net_profit_qoq_0', desc='归属母公司股东的净利润单季度环比增长率'),
    R('fs_roe_0', desc='净资产收益率'),
    R('fs_roe_ttm_0', desc='净资产收益率 (TTM)'),
    R('fs_roa_0', desc='总资产报酬率'),
    R('fs_roa_ttm_0', desc='总资产报酬率 (TTM)'),
    R('fs_operating_revenue_yoy_0', desc='营业收入同比增长率'),
    R('fs_operating_revenue_qoq_0', desc='营业收入单季度环比增长率'),
    R('fs_eps_0', desc='每股收益'),
    R('fs_eps_yoy_0', desc='每股收益同比增长率'),
    R('fs_bps_0', desc='每股净资产'),
    R('fs_cash_ratio_0', desc='现金比率'),

    'category:股东因子',
    'markets:CN_STOCK_A',
    'field_group:9',
    R('sh_holder_avg_pct_0',desc='户均持股比例'),
    R('sh_holder_avg_pct_3m_chng_0', desc='户均持股比例季度增长率'),
    R('sh_holder_avg_pct_6m_chng_0', desc='户均持股比例半年增长率'),
    R('sh_holder_num_0', desc='股东户数'),

    'category:技术分析因子',
    'markets:all',
    'field_group:10',
    F('ta_sma_$i_0', [5, 10, 20, 30, 60], 'MA(context.ta_input_data, timeperiod=i)', '收盘价的i日简单移动平均值', base_field=BAR_FIELDS),
    F('ta_rsi_$i_0', [14, 28], 'RSI(context.ta_input_data, timeperiod=i)', 'RSI指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_atr_$i_0', [14, 28], 'ATR(context.ta_input_data, timeperiod=i)', 'ATR指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_cci_$i_0', [14,28], 'CCI(context.ta_input_data, timeperiod=i)', 'CCI指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_mom_$i_0', [10, 20, 30, 60], 'MOM(context.ta_input_data, timeperiod=i)', 'MOM指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_ema_$i_0', [5, 10, 20, 30, 60], 'EMA(context.ta_input_data, timeperiod=i)', '收盘价的i日指数移动平均值', base_field=BAR_FIELDS),
    F('ta_wma_$i_0', [5, 10, 20, 30, 60], 'WMA(context.ta_input_data, timeperiod=i)', '收盘价的i日加权移动平均值', base_field=BAR_FIELDS),

    'field_group:11',
    F('ta_ad', [0], 'AD(context.ta_input_data)', '收集派发指标', base_field=BAR_FIELDS),
    F('ta_aroon_down_$i_0', [14, 28], 'context.ta("AROON", i)[0]', '阿隆指标aroondown，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_aroon_up_$i_0', [14, 28], 'context.ta("AROON", i)[1]', '阿隆指标aroonup，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_aroonosc_$i_0', [14, 28], 'AROONOSC(context.ta_input_data, timeperiod=i)', 'AROONOSC指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_bbands_upperband_$i_0', [14, 28], 'context.ta("BBANDS", i)[0]', 'BBANDS指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_bbands_middleband_$i_0', [14, 28], 'context.ta("BBANDS", i)[1]', 'BBANDS指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_bbands_lowerband_$i_0', [14, 28], 'context.ta("BBANDS", i)[2]', 'BBANDS指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_adx_$i_0', [14,28], 'ADX(context.ta_input_data, timeperiod=i)', 'ADX指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_macd_macd_12_26_9', [0], 'context.ta("MACD")[0]', 'MACD', base_field=BAR_FIELDS),
    F('ta_macd_macdsignal_12_26_9', [0], 'context.ta("MACD")[1]', 'MACD', base_field=BAR_FIELDS),
    F('ta_macd_macdhist_12_26_9', [0], 'context.ta("MACD")[2]', 'MACD', base_field=BAR_FIELDS),
    F('ta_obv', [0], 'context.ta("OBV")', 'OBV指标', base_field=BAR_FIELDS),
    F('ta_stoch_slowk_5_3_0_3_0', [0], 'context.ta("STOCH")[0]', 'STOCH (KDJ) 指标K值', base_field=BAR_FIELDS),
    F('ta_stoch_slowd_5_3_0_3_0', [0], 'context.ta("STOCH")[1]', 'STOCH (KDJ) 指标D值', base_field=BAR_FIELDS),
    F('ta_mfi_$i_0', [14, 28], 'MFI(context.ta_input_data, timeperiod=i)', 'MFI指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_trix_$i_0', [14, 28], 'TRIX(context.ta_input_data, timeperiod=i)', 'TRIX指标，timeperiod=i', base_field=BAR_FIELDS),
    F('ta_sar', [0], 'SAR(context.ta_input_data)', 'SAR指标', base_field=BAR_FIELDS),
    F('ta_willr_$i_0', [14, 28], 'WILLR(context.ta_input_data, timeperiod=i)', 'WILLR指标，timeperiod=i', base_field=BAR_FIELDS),

    'category:波动率',
    'markets:all',
    'field_group:12',
    H('swing_volatility_$i_0',  range=[5, 10, 30, 60], expr='((df.high - df.low)/df.close).rolling(center=False, window=i).std()', desc='振幅波动率，timeperiod=i',  rank=True, base_field=BAR_FIELDS),
    H('volatility_$i_0',  range=[5, 10, 30, 60], expr='(df.close/df.close.shift(1)-1).rolling(center=False, window=i).std()', desc='波动率，timeperiod=i',  rank=True, base_field=BAR_FIELDS),

    'category:BETA值',
    'markets:CN_STOCK_A',
    H('beta_sse50_$i_0',  range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000016.SHA" ,timeperiod=i)', desc='BETA值(上证50)，timeperiod=i',  rank=True, base_field=BAR_FIELDS),
    H('beta_csi300_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000300.SHA" ,timeperiod=i)', desc='BETA值(沪深300)，timeperiod=i', rank=True, base_field=BAR_FIELDS),
    H('beta_csi500_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000905.SHA" ,timeperiod=i)', desc='BETA值(中证500)，timeperiod=i', rank=True, base_field=BAR_FIELDS),
    H('beta_csi800_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000906.SHA" ,timeperiod=i)', desc='BETA值(中证800)，timeperiod=i', rank=True, base_field=BAR_FIELDS),
    H('beta_sse180_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000010.SHA" ,timeperiod=i)', desc='BETA值(上证180)，timeperiod=i', rank=True, base_field=BAR_FIELDS),
    H('beta_csi100_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000903.SHA" ,timeperiod=i)', desc='BETA值(中证100)，timeperiod=i', rank=True, base_field=BAR_FIELDS),
    H('beta_szzs_$i_0',   range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000001.SHA" ,timeperiod=i)', desc='BETA值(上证综指)，timeperiod=i',rank=True, base_field=BAR_FIELDS),
    H('beta_gem_$i_0',    range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("399006.SZA" ,timeperiod=i)', desc='BETA值(创业板)，timeperiod=i',  rank=True, base_field=BAR_FIELDS),
    H('beta_industry_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("INDUSTRY" ,timeperiod=i)', desc='BETA值(所在行业)，timeperiod=i',  rank=True, base_field=BAR_FIELDS),
])



class FeatureDefs:
    FEATURE_LIST = _FEATURES

    FEATURE_MAP = {f.field: f for f in FEATURE_LIST}

    # 特征抽取时，需要用到的历史数据的天数，1 来自 shift里的增加，根据如上FEATURE中的expr来更新此处
    # FEATURE_DAYS_LOOK_BACK = int((max(max(f.range) if f.range else 0 for f in FEATURE_LIST if not f.field.startswith('rank_')) + 1)*1.5)
    max_range = 0
    for f in FEATURE_LIST:
        if not f.field.startswith('rank_'):
            if f.range and max(f.range) > max_range:
                max_range = max(f.range)
    FEATURE_DAYS_LOOK_BACK = int((max_range + 1) * 2.5)


if __name__ == '__main__':
    # test code
    #print(FeatureDefs.FEATURE_DAYS_LOOK_BACK)
    import pandas as pd
    import datetime
    import numpy as np

    #arr = pd.Series(np.random.rand(1000000) * 1000)
    #s = datetime.datetime.now()

    #print((datetime.datetime.now() - s).total_seconds())
    num = 0
    for item in _FEATURES:
        desc = item.desc
        for s in item.expand_fields(with_desc=True):
            if isinstance(s,tuple):
                field,desc = s
            else:
                field,desc = s, desc
            family = 'F%03d'%item.field_group
            print("'%s': {'family': '%s', 'type': '%s', 'desc': '%s'},"%(field, family, item.type, desc))
            num += 1
