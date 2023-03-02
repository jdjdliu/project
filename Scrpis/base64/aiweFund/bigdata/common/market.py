# -*- coding:utf-8 -*-
# This is be removed, go to marketv2.py
import datetime
import re
from collections import namedtuple
from bigdata.common.industrydefs import IndustryDefs


market_tuple = namedtuple('market_tuple', ['symbol', 'desc'])
market_group_tuple = namedtuple('market_group_tuple', ['symbol', 'markets', 'sub_groups', 'name'])


class Market:
    M_CHINA_STOCK_SHA = market_tuple('SHA', '中国上海证券交易所')
    M_CHINA_STOCK_SZA = market_tuple('SZA', '中国深圳证券交易所')
    M_CHINA_STOCK_BJA = market_tuple('BJA', '中国北京证券交易所')

    M_CHINA_FUND_ZOF = market_tuple('ZOF', '基金(深圳)')
    M_CHINA_FUND_HOF = market_tuple('HOF', '基金(上海)')
    M_CHINA_FUND_BOF = market_tuple('BOF', '基金(北京)')

    M_CHINA_FUTURE_CFE = market_tuple('CFE', '中国金融期货交易所')
    M_CHINA_FUTURE_CFX = market_tuple('CFX', '中国金融期货交易所')
    M_CHINA_FUTURE_INE = market_tuple('INE', '上海国际能源交易中心')
    M_CHINA_FUTURE_SHF = market_tuple('SHF', '上海期货交易所')
    M_CHINA_FUTURE_DCE = market_tuple('DCE', '大连商品交易所')
    M_CHINA_FUTURE_CZC = market_tuple('CZC', '郑州商品交易所')

    M_CHINA_OPTION_SHO = market_tuple('SHO', '上海证券交易所(期权)')
    M_CHINA_OPTION_SZO = market_tuple('SZO', '深圳证券交易所(期权)')

    M_HK_STOCK = market_tuple('HKEX', '香港交易所')

    M_US_STOCK_NYSE = market_tuple('NYSE', '纽约商业交易所')
    M_US_STOCK_NASDAQ= market_tuple('NSDQ', '纳斯达克交易所')

    M_CC_EXCHANGE_BIA = market_tuple('BIA', 'Binance')
    M_CC_EXCHANGE_OKE = market_tuple('OKE', 'OKEx')
    M_CC_EXCHANGE_ZB = market_tuple('ZB', 'ZB')
    M_CC_EXCHANGE_BFN = market_tuple('BFN', 'Bitfinex')
    M_CC_EXCHANGE_HBI = market_tuple('HBI', 'Huobi')

    MG_CHINA_STOCK_A = market_group_tuple('CN_STOCK_A', [M_CHINA_STOCK_SHA, M_CHINA_STOCK_SZA, M_CHINA_STOCK_BJA], [], 'A股')
    MG_CHINA_FUND = market_group_tuple('CN_FUND', [M_CHINA_FUND_ZOF, M_CHINA_FUND_HOF, M_CHINA_FUND_BOF], [], '基金')
    MG_CHINA_FUTURE = market_group_tuple('CN_FUTURE', [M_CHINA_FUTURE_CFE, M_CHINA_FUTURE_SHF, M_CHINA_FUTURE_INE,
                                                       M_CHINA_FUTURE_DCE, M_CHINA_FUTURE_CZC, M_CHINA_FUTURE_CFX], [], '期货')
    MG_CHINA_OPTION = market_group_tuple('CN_OPTION', [M_CHINA_OPTION_SHO, M_CHINA_OPTION_SZO], [], '期权')

    MG_HK_STOCK = market_group_tuple('HK_STOCK', [M_HK_STOCK], [], '港股')

    MG_US_STOCK = market_group_tuple('US_STOCK', [M_US_STOCK_NASDAQ, M_US_STOCK_NYSE], [], '美股')

    MG_CC_EXCHANGE = market_group_tuple('CC_EXCHANGE', [M_CC_EXCHANGE_BIA, M_CC_EXCHANGE_OKE, M_CC_EXCHANGE_ZB,
                                                        M_CC_EXCHANGE_BFN, M_CC_EXCHANGE_HBI], [], '数字货币交易所')

    MG_CHINA = market_group_tuple('CN', [*MG_CHINA_STOCK_A.markets, *MG_CHINA_FUND.markets, *MG_CHINA_FUTURE.markets, *MG_CHINA_OPTION.markets],
                                  [MG_CHINA_STOCK_A, MG_CHINA_FUTURE, MG_CHINA_FUND, MG_CHINA_OPTION], '中国市场')

    MG_US = market_group_tuple('US', [*MG_US_STOCK.markets], [MG_US_STOCK], '美国市场')

    MG_HK = market_group_tuple('HK', [*MG_HK_STOCK.markets], [MG_HK_STOCK], '香港市场')

    MG_ALL = market_group_tuple('ALL', [*MG_CHINA.markets, *MG_US.markets, *MG_HK.markets],
                                [*MG_CHINA.sub_groups, *MG_US.sub_groups, *MG_HK.sub_groups], '全部市场')

    TRADING_DAYS_MARKET_MAP = {
        **{x.symbol: market for x, market in zip(MG_CHINA.markets, [MG_CHINA.symbol] * len(MG_CHINA.markets))},
        **{x.symbol: market for x, market in zip(MG_CHINA.sub_groups, [MG_CHINA.symbol] * len(MG_CHINA.markets))},

        **{x.symbol: market for x, market in zip(MG_US.markets, [MG_US.symbol] * len(MG_US.markets))},
        **{x.symbol: market for x, market in zip(MG_US.sub_groups, [MG_US.symbol] * len(MG_US.markets))},

        **{x.symbol: market for x, market in zip(MG_HK.markets, [MG_HK.symbol] * len(MG_HK.markets))},
        **{x.symbol: market for x, market in zip(MG_HK.sub_groups, [MG_HK.symbol] * len(MG_HK.markets))},

        MG_CHINA.symbol: MG_CHINA.symbol,
        MG_US.symbol: MG_US.symbol,
        MG_HK.symbol: MG_HK.symbol,
    }

    MG_CHINA_FUTURE_SYMBOLS = set([x.symbol for x in MG_CHINA_FUTURE.markets])
    MG_CHINA_OPTION_SYMBOLS = set([x.symbol for x in MG_CHINA_OPTION.markets])
    MG_CC_EXCHANGE_SYMBOLS = set([x.symbol for x in MG_CC_EXCHANGE.markets])


market_to_group = {
    **{market.symbol: Market.MG_CHINA_STOCK_A.symbol for market in Market.MG_CHINA_STOCK_A.markets},
    **{market.symbol: Market.MG_CHINA_FUTURE.symbol for market in Market.MG_CHINA_FUTURE.markets},
    **{market.symbol: Market.MG_CHINA_FUND.symbol for market in Market.MG_CHINA_FUND.markets},
    **{market.symbol: Market.MG_CHINA_OPTION.symbol for market in Market.MG_CHINA_OPTION.markets},
    **{market.symbol: Market.MG_US_STOCK.symbol for market in Market.MG_US_STOCK.markets},
    **{market.symbol: Market.MG_HK_STOCK.symbol for market in Market.MG_HK_STOCK.markets},
    **{market.symbol: Market.MG_CC_EXCHANGE.symbol for market in Market.MG_CC_EXCHANGE.markets},
}

symbol_to_group= {
    **{sub_group.symbol: sub_group for sub_group in Market.MG_ALL.sub_groups},
    Market.MG_ALL.symbol: Market.MG_ALL
}

market_symbols_wind_to_bq = {
    'SH': Market.M_CHINA_STOCK_SHA.symbol,
    'SZ': Market.M_CHINA_STOCK_SZA.symbol,
    'CFE': Market.M_CHINA_FUTURE_CFE.symbol,
    'INE': Market.M_CHINA_FUTURE_INE.symbol,
    'SHF': Market.M_CHINA_FUTURE_SHF.symbol,
    'DCE': Market.M_CHINA_FUTURE_DCE.symbol,
    'CZC': Market.M_CHINA_FUTURE_CZC.symbol,
    'ZOF': Market.M_CHINA_FUND_ZOF.symbol,
    'HOF': Market.M_CHINA_FUND_HOF.symbol,
    'O': Market.M_US_STOCK_NASDAQ.symbol,
    'N': Market.M_US_STOCK_NYSE.symbol,
    'HK': Market.M_HK_STOCK.symbol,

}

market_options_symbols_wind_to_bq = {
    'SH': Market.M_CHINA_OPTION_SHO.symbol,
    'SZ': Market.M_CHINA_OPTION_SZO.symbol,
}


market_symbols_bq_to_wind = dict([(v, k) for k, v in market_symbols_wind_to_bq.items()])

market_options_symbols_bq_to_wind = {v: k for k, v in market_options_symbols_wind_to_bq.items()}

future_normal_code_map = {'WS': 'WH', 'RO': 'OI', 'ER': 'RI', 'ME': 'MA', 'TC': 'ZC'}


def _convert_instrument(instrument, market_symbol_map):
    s = instrument.rsplit('.', 1)
    s[1] = market_symbol_map[s[1]]
    return s[0] + '.' + s[1]

def future_name_split(instrument):
    code, market_name= instrument.rsplit('.', 1)
    match = re.match(r'([A-Z]+)(\d*)', code)
    assert match is not None
    code_type, code_id = match.groups()
    return code_type, code_id, market_name

def is_old_future_name(instrument):
    code_type, code_id, market_name = future_name_split(instrument)
    if market_name in Market.MG_CHINA_FUTURE_SYMBOLS and code_type in future_normal_code_map:
        return True
    return False


def _future_name_normal(instrument):
    code_type, code_id, market_name = future_name_split(instrument)
    p = datetime.date.today().strftime('%y')
    if len(code_id) == 3:
        if code_id[0] < p[1] and code_id[0] <= '5':
            prefix = str(((int(p[0]) + 1) % 10))
        else:
            prefix = p[0]
        code_id = prefix + code_id
        instrument = '{}{}.{}'.format(code_type, code_id, market_name)
    if not code_id:
        code_id = '8888'
        instrument = '{}{}.{}'.format(code_type, code_id, market_name)
    if market_name in Market.MG_CHINA_FUTURE_SYMBOLS and code_type in future_normal_code_map:
        instrument = '%s%s.%s' % (future_normal_code_map[code_type], code_id, market_name)
    return instrument


def wind_to_bq_instrument(instrument, market=None):
    if market == Market.MG_CHINA_OPTION.symbol:
        symbol_mapping = market_options_symbols_wind_to_bq
    else:
        symbol_mapping = market_symbols_wind_to_bq
        if instrument in __code_map:
            return __code_map[instrument]
        if instrument.rsplit('.', 1)[1] in Market.MG_CHINA_FUTURE_SYMBOLS:
            instrument = _future_name_normal(instrument)
    return _convert_instrument(instrument, symbol_mapping)


def bq_to_wind_instrument(instrument):
    if instrument in __code_map:
        return __code_map[instrument]
    return _convert_instrument(instrument, market_symbols_bq_to_wind)


MARKET_INDEX = namedtuple('MARKET_INDEX', ['instrument', 'instrument_wind', 'name', 'list_date', 'feature_name'])

def __build_market_index(instrument_wind, name, list_date, feature_name=None, instrument_bq=None):
    if instrument_bq:
        return MARKET_INDEX(instrument_bq,instrument_wind, name, list_date, feature_name)
    else:
        return MARKET_INDEX(_convert_instrument(instrument_wind,market_symbols_wind_to_bq), instrument_wind, name, list_date, feature_name)


def __build_sw_market_index(instrument_wind, name, list_date='2015-01-01', feature_name=None):
    assert instrument_wind.endswith('.SI')
    sw_code=IndustryDefs.SWL1_NAME_TO_CODE[name.strip('(申万)')]
    bq_instrument = 'SW%s.SHA'%sw_code
    return MARKET_INDEX(bq_instrument, instrument_wind, name, list_date, feature_name)

__market_index_list_cna = [
    __build_market_index('000001.SH', '上证综指', '1991-07-15'),
    __build_market_index('000016.SH', '上证50', '2004-01-02'),
    __build_market_index('000300.SH', '沪深300', '2005-04-08'),
    __build_market_index('000905.SH', '中证500', '2007-01-15'),
    __build_market_index('000906.SH', '中证800', '2007-01-15'),
    __build_market_index('399001.SZ', '深证成指', '1995-01-23'),
    __build_market_index('399006.SZ', '创业板指', '2010-01-01'),
    __build_market_index('000010.SH', '上证180', '2002-07-01'),
    __build_market_index('000903.SH', '中证100', '2006-05-29'),
    __build_market_index('399330.SZ', '深证100', '2006-01-24'),
    __build_market_index('399333.SZ', '中小板R', '2006-12-27'),

    __build_sw_market_index('801010.SI', '农林牧渔(申万)'),
    __build_sw_market_index('801020.SI', '采掘(申万)'),
    __build_sw_market_index('801030.SI', '化工(申万)'),
    __build_sw_market_index('801040.SI', '钢铁(申万)'),
    __build_sw_market_index('801050.SI', '有色金属(申万)'),
    __build_sw_market_index('801080.SI', '电子(申万)'),
    __build_sw_market_index('801110.SI', '家用电器(申万)'),
    __build_sw_market_index('801120.SI', '食品饮料(申万)'),
    __build_sw_market_index('801130.SI', '纺织服装(申万)'),
    __build_sw_market_index('801140.SI', '轻工制造(申万)'),
    __build_sw_market_index('801150.SI', '医药生物(申万)'),
    __build_sw_market_index('801160.SI', '公用事业(申万)'),
    __build_sw_market_index('801170.SI', '交通运输(申万)'),
    __build_sw_market_index('801180.SI', '房地产(申万)'),
    __build_sw_market_index('801200.SI', '商业贸易(申万)'),
    __build_sw_market_index('801210.SI', '休闲服务(申万)'),
    __build_sw_market_index('801230.SI', '综合(申万)'),
    __build_sw_market_index('801710.SI', '建筑材料(申万)'),
    __build_sw_market_index('801720.SI', '建筑装饰(申万)'),
    __build_sw_market_index('801730.SI', '电气设备(申万)'),
    __build_sw_market_index('801740.SI', '国防军工(申万)'),
    __build_sw_market_index('801750.SI', '计算机(申万)'),
    __build_sw_market_index('801760.SI', '传媒(申万)'),
    __build_sw_market_index('801770.SI', '通信(申万)'),
    __build_sw_market_index('801780.SI', '银行(申万)'),
    __build_sw_market_index('801790.SI', '非银金融(申万)'),
    __build_sw_market_index('801880.SI', '汽车(申万)'),
    __build_sw_market_index('801890.SI', '机械设备(申万)'),
]


__market_index_list_us = [
    __build_market_index('IXIC.GI', '纳斯达克指数', '1971-02-08', instrument_bq='IXIC.%s'%Market.M_US_STOCK_NASDAQ.symbol),
    __build_market_index('DJI.GI', '道琼斯工业指数', '1896-05-26', instrument_bq='DJI.%s'%Market.M_US_STOCK_NYSE.symbol),
    __build_market_index('SPX.GI', '标准普尔500指数', '1923-01-01', instrument_bq='SPX.%s'%Market.M_US_STOCK_NYSE.symbol),
]

__market_index_list_hk = [
    __build_market_index('HSI.HI', '恒生指数', '1969-01-01', instrument_bq='HSI.%s'%Market.M_HK_STOCK.symbol),
    __build_market_index('HSCEI.HI', '国企指数', '2000-01-03', instrument_bq='HSCEI.%s'%Market.M_HK_STOCK.symbol),
]


market_indexes_us = {m.instrument:m for m in __market_index_list_us}
market_index_wind_instruments_us = set(m.instrument_wind for m in __market_index_list_us)

market_indexes_cna = {m.instrument:m for m in __market_index_list_cna}
market_index_wind_instruments_cna = set(m.instrument_wind for m in __market_index_list_cna)

market_indexes_hk = {m.instrument:m for m in __market_index_list_hk}
market_index_wind_instruments_hk = set(m.instrument_wind for m in __market_index_list_hk)

__code_map = {
    **{x.instrument_wind:x.instrument for x in __market_index_list_cna},
    **{x.instrument:x.instrument_wind for x in __market_index_list_cna},

    **{x.instrument_wind:x.instrument for x in __market_index_list_us},
    **{x.instrument:x.instrument_wind for x in __market_index_list_us},

    **{x.instrument_wind:x.instrument for x in __market_index_list_hk},
    **{x.instrument:x.instrument_wind for x in __market_index_list_hk},
}

index_constituent_list = [
    __build_market_index('000016.SH', '上证50', '2004-01-02', 'in_sse50'),
    __build_market_index('000300.SH', '沪深300', '2005-04-08', 'in_csi300'),
    __build_market_index('000905.SH', '中证500', '2007-01-15', 'in_csi500'),
    __build_market_index('000906.SH', '中证800', '2007-01-15', 'in_csi800'),
    __build_market_index('000010.SH', '上证180', '2002-07-01', 'in_sse180'),
    __build_market_index('000903.SH', '中证100', '2006-05-29', 'in_csi100'),
    __build_market_index('399330.SZ', '深证100', '2006-01-24', 'in_szse100'),
]

if __name__ == '__main__':
    # test code
    print(_future_name_normal('RS909.CZC'))
    print(Market.MG_CHINA)
    print(Market.MG_ALL)
    print(__code_map)
    print(wind_to_bq_instrument('TC.CZC'))
    print(market_to_group)
    print(Market.TRADING_DAYS_MARKET_MAP)
