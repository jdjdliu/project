import re
from collections import namedtuple
from bigdata.common.industrydefs import IndustryDefs


MARKET = namedtuple('MARKET', ['symbol', 'desc', 'trading_calendar', 'wind_symbol'])
MARKETS = [
    MARKET('SHA', '中国上海股票交易所', 'CN', 'SH'),
    MARKET('SZA', '中国深圳股票交易所', 'CN', 'SZ'),
]


class MarketManager:
    def __init__(self):
        self.__market_to_trading_calendar_map = self.__build_markets_map('symbol', 'trading_calendar')
        self.__wind_symbol_to_bq_symbol_map = self.__build_markets_map('wind_symbol', 'symbol')

    def __build_markets_map(self, key_field, value_field):
        return {getattr(m, key_field): getattr(m, value_field) for m in MARKETS}

    def get_trading_calendar(self, symbol):
        '''
        获取指定市场对应的交易日历名字
        '''
        return self.__market_to_trading_calendar_map.get(symbol, None)

    def get_bq_symbol(self, wind_symbol):
        '''
        获取指定市场wind代码对应的BQ代码
        '''
        return self.__wind_symbol_to_bq_symbol_map.get(wind_symbol, None)


Markets = MarketManager()


if __name__ == '__main__':
    # test code
    pass
