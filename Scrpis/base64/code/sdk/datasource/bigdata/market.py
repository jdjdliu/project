
from collections import namedtuple


market_tuple = namedtuple('market_tuple', ['symbol', 'desc'])
market_group_tuple = namedtuple('market_group_tuple', ['symbol', 'markets', 'sub_groups', 'name'])


class Market:
    M_CHINA_STOCK_SHA = market_tuple('SHA', '中国上海股票交易所')
    M_CHINA_STOCK_SZA = market_tuple('SZA', '中国深圳股票交易所')

    M_CHINA_FUND_ZOF = market_tuple('ZOF', '基金(深圳)')
    M_CHINA_FUND_HOF = market_tuple('HOF', '基金(上海)')

    M_CHINA_FUTURE_CFE = market_tuple('CFE', '中国金融期货交易所')
    M_CHINA_FUTURE_INE = market_tuple('INE', '上海国际能源交易中心')
    M_CHINA_FUTURE_SHF = market_tuple('SHF', '上海期货交易所')
    M_CHINA_FUTURE_DCE = market_tuple('DCE', '大连商品交易所')
    M_CHINA_FUTURE_CZC = market_tuple('CZC', '郑州商品交易所')

    M_CHINA_OPTION_SHAO = market_tuple('SHAO', '上海证券交易所(期权)')

    M_HK_STOCK = market_tuple('HKEX', '香港交易所')

    M_US_STOCK_NYSE = market_tuple('NYSE', '纽约商业交易所')
    M_US_STOCK_NASDAQ = market_tuple('NSDQ', '纳斯达克交易所')

    M_CC_EXCHANGE_BIA = market_tuple('BIA', 'Binance')
    M_CC_EXCHANGE_OKE = market_tuple('OKE', 'OKEx')
    M_CC_EXCHANGE_ZB = market_tuple('ZB', 'ZB')
    M_CC_EXCHANGE_BFN = market_tuple('BFN', 'Bitfinex')
    M_CC_EXCHANGE_HBI = market_tuple('HBI', 'Huobi')

    MG_CHINA_STOCK_A = market_group_tuple('CN_STOCK_A', [M_CHINA_STOCK_SHA, M_CHINA_STOCK_SZA], [], 'A股')
    MG_CHINA_FUND = market_group_tuple('CN_FUND', [M_CHINA_FUND_ZOF, M_CHINA_FUND_HOF], [], '基金')
    MG_CHINA_FUTURE = market_group_tuple('CN_FUTURE', [M_CHINA_FUTURE_CFE, M_CHINA_FUTURE_SHF, M_CHINA_FUTURE_INE,
                                                       M_CHINA_FUTURE_DCE, M_CHINA_FUTURE_CZC], [], '期货')
    MG_CHINA_OPTION = market_group_tuple('CN_OPTION', [M_CHINA_OPTION_SHAO], [], '期权')

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
