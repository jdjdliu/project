# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz
from learning.module2.common.data import Outputs
from sdk.auth import Credential

# from bigdata.api.datareader import DataReaderV3
from sdk.datasource import DataSource, Market, constants
from sdk.strategy.client import StrategyClient
from sdk.strategy.schemas import BacktestDailyPerformanceSchema, CreateBacktestPerformanceRequest
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

from .globalvalue import GlobalValue
from .perf_render import get_stats as get_stats_stock
from .perf_render_future import get_stats as get_stats_future

log = BigLogger("backtest")

bigquant_cacheable = True
bigquant_public = False
PRICE_FIELDS = ["open", "high", "low", "close"]

WAP_FIELDS = frozenset(
    [
        "twap_1",
        "twap_2",
        "twap_3",
        "twap_4",
        "twap_5",
        "twap_6",
        "twap_7",
        "twap_8",
        "twap_9",
        "twap_10",
        "twap_11",
        "vwap_1",
        "vwap_2",
        "vwap_3",
        "vwap_4",
        "vwap_5",
        "vwap_6",
        "vwap_7",
        "vwap_8",
        "vwap_9",
        "vwap_10",
        "vwap_11",
    ]
)

BACKTEST_FIELDS = ["open", "high", "low", "close", "volume"]
BACKTEST_FIELDS_EX = ["delist_date", "adjust_factor", "amount", "settle", "price_limit_status"]

CN_OPT_MARKETS = ["SHAO", "SZAO", "SHO", "SZO"]
CN_SEC_MARKETS = ["SHA", "SH", "SSE", "SZA", "SZ", "SZSE", "BJA", "BSE", "HOF", "ZOF", "HIX", "ZIX"] + CN_OPT_MARKETS
CN_FUT_MARKETS = ["DCE", "CZC", "ZCE", "CZCE", "SHF", "SHFE", "INE", "CFE", "CFX", "CFFEX", "GFE", "GFEX"]
CN_MARKETS = set(CN_SEC_MARKETS + CN_FUT_MARKETS)

BIGQUANT_Back_V8_Version = "V8.6.2"
bigquant_debug_log = False
bigquant_has_panel = True if pd.__version__ <= "0.23" else False

# D3 = DataReaderV3()
RUN_MODE = os.getenv("RUN_MODE", "")


class ContextMock:
    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            if name not in self.__dict__:
                setattr(self, name, value)


# for forward test imports


def set_is_stock(value):
    GlobalValue.set_is_stock(value)


def get_is_stock():
    return GlobalValue.get_is_stock()


def get_is_option():
    return GlobalValue.get_is_option()


def read_benchmark_df(instrument, start=None, end=None):
    table_name = ""
    if instrument.endswith(("HIX", "ZIX")):
        table_name = "bar1d_index_CN_STOCK_A"
    elif instrument.endswith(tuple(CN_FUT_MARKETS)):
        table_name = "bar1d_CN_FUTURE"
    elif instrument.endswith(("SHA", "SH", "SSE")) and instrument.startswith("00"):
        instrument = instrument[:6] + ".HIX"
        table_name = "bar1d_index_CN_STOCK_A"
    elif instrument.endswith(("SZA", "SZ", "SZSE")) and instrument.startswith("39"):
        instrument = instrument[:6] + ".ZIX"
        table_name = "bar1d_index_CN_STOCK_A"
    else:
        table_name = "bar1d_CN_STOCK_A"

    ohlc_df = DataSource(table_name).read([instrument], start_date=start, end_date=end)
    return ohlc_df


def create_stock_data_df(
    instruments,
    data_frequency="daily",
    start=None,
    end=None,
    try_index_source=False,
    is_price_type_real=False,
    price_field_buy="open",
    price_field_sell="close",
):
    # print('creating stock data ', instruments, start, end)
    from zipline.finance.slippage import get_wap_fields

    # @20181102 stock index source changed
    if try_index_source:
        index = 0
        for instrument in instruments:
            if "SHA" in instrument:
                instruments[index] = instrument.replace("SHA", "HIX")
            elif "SZA" in instrument:
                instruments[index] = instrument.replace("SZA", "ZIX")
            index += 1
        id_ohlc = "bar1d_index_CN_STOCK_A"
        ohlc_df = DataSource(id_ohlc).read(instruments, start_date=start, end_date=end)
        if ohlc_df is not None and len(ohlc_df) > 0:
            return ohlc_df

    id_stat = "stock_status_CN_STOCK_A"
    stat_df = DataSource(id_stat).read(instruments, start_date=start, end_date=end, fields=["price_limit_status", "suspended", "st_status"])
    # print('stat_df:\n', stat_df[:2])

    id_basicinfo = "basic_info_CN_STOCK_A"
    basicinfo_df = DataSource(id_basicinfo).read(instruments, start_date=start, end_date=end, fields=["list_date", "delist_date"])
    # print('basicinfo_df:\n', basicinfo_df[:2])

    id_instr_info = "instruments_CN_STOCK_A"
    instr_info_df = DataSource(id_instr_info).read(instruments, start_date=start, end_date=end, fields=["name"])
    # print('instr_info_df:\n', instr_info_df[:2])

    info_df = pd.merge(stat_df, basicinfo_df, how="outer")
    # print('info_df len:', len(info_df))
    if not len(info_df):
        msg = "creating stock data none info start:{}, end:{}, stat_len:{}, basiclen:{}".format(start, end, len(stat_df), len(basicinfo_df))
        log.warn(msg)

    daily_df = DataSource("bar1d_CN_STOCK_A").read(instruments, start_date=start, end_date=end)
    daily_df = pd.merge(daily_df, info_df, on=["date", "instrument"], how="inner")
    daily_df = pd.merge(daily_df, instr_info_df, on=["date", "instrument"], how="inner")

    if data_frequency == "daily":
        if price_field_buy in WAP_FIELDS or price_field_sell in WAP_FIELDS:
            wap_fields = get_wap_fields(price_field_buy) + get_wap_fields(price_field_sell)
            wap_fields = list(set(wap_fields))

            wap_df = None
            id_wap = ""
            if price_field_buy in WAP_FIELDS:
                id_wap = "bar1d_" + price_field_buy[1:]
                if not is_price_type_real:
                    id_wap += "_adj"

                wap_df = DataSource(id_wap).read(instruments, start_date=start, end_date=end, fields=wap_fields)
            if price_field_buy != price_field_sell and price_field_sell in WAP_FIELDS:
                id_wap2 = "bar1d_" + price_field_sell[1:]
                if not is_price_type_real:
                    id_wap2 += "_adj"

                if id_wap != id_wap2:
                    wap_df2 = DataSource(id_wap2).read(instruments, start_date=start, end_date=end, fields=wap_fields)

                    if wap_df is not None:
                        wap_df = pd.merge(wap_df, wap_df2, on=["date", "instrument"])
                    else:
                        wap_df = wap_df2

            if wap_df is not None and len(wap_df) > 0:
                if "adjust_factor" in wap_df.columns:
                    del wap_df["adjust_factor"]
            else:
                log.warn("read empty data from %s table" % id_wap)
        else:
            wap_df = None

        ohlc_df = daily_df
        if wap_df is not None:
            ohlc_df = pd.merge(ohlc_df, wap_df, on=["date", "instrument"], how="inner")

        # @202004 read all index data for old strategy compatible
        index_df = DataSource("bar1d_index_CN_STOCK_A").read(instruments=None, start_date=start, end_date=end)
        if index_df is not None and len(index_df):
            # index_df.instrument = index_df.instrument.str \
            #     .replace("HIX", "SHA").replace("ZIX", "SZA")
            ohlc_df = pd.concat([ohlc_df, index_df])
    else:
        ohlc_df = DataSource("bar1m_CN_STOCK_A").read(instruments, start_date=start, end_date=end)
        # print('###', len(ohlc_df))

    # print('stock_data len:{}'.format(len(ohlc_df)))
    return ohlc_df


def create_fund_data_df(
    instruments, data_frequency="daily", start=None, end=None, is_price_type_real=False, price_field_buy="open", price_field_sell="close"
):
    id_fund_info = "basic_info_CN_FUND"
    fund_info_df = DataSource(id_fund_info).read(instruments, start_date=start, end_date=end, fields=["list_date", "delist_date", "name"])
    if not len(fund_info_df):
        msg = "creating fund info none start:{} end:{}".format(start, end)
        log.warn(msg)
        return
    if data_frequency == "daily":
        daily_df = DataSource("bar1d_CN_FUND").read(instruments, start_date=start, end_date=end)
        daily_df = pd.merge(daily_df, fund_info_df, how="inner", on=["instrument"])
        ohlc_df = daily_df
    else:
        fund_df = DataSource("bar1d_CN_FUND").read(instruments, start_date=start, end_date=end)
        ohlc_df = pd.concat(fund_df)
    return ohlc_df


def create_fut_data_df(instruments, data_frequency="daily", start=None, end=None):
    # 从BQ数据源 创建期货数据 日线/分钟

    norm_instruments = instruments
    dom_instruments = []
    for instrument in instruments:
        if "8888" in instrument:
            dom_instruments.append(instrument)

    # 连续主力
    daily_start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    daily_end_date = pd.Timestamp(end).strftime("%Y-%m-%d")

    dom_df = None
    if len(dom_instruments):
        dom_df = DataSource("dominant_CN_FUTURE").read(dom_instruments, start_date=daily_start_date, end_date=daily_end_date)
        norm_instruments.extend(list(set(dom_df.dominant)))

    # 日线数据
    ohlc_df = DataSource("bar1d_CN_FUTURE").read(norm_instruments, start_date=daily_start_date, end_date=daily_end_date)

    if dom_df is not None:
        ohlc_df = pd.merge(ohlc_df, dom_df, on=["date", "instrument"], how="outer")
    else:
        ohlc_df["dominant"] = ""

    # @20180809 分钟模式下，需要额外获取结算价，主力合约
    # min_df = None
    if data_frequency == "minute":
        minute_df = DataSource("bar1m_CN_FUTURE").read(instruments, start_date=start, end_date=end)
        ohlc_df = pd.concat([ohlc_df, minute_df], ignore_index=True)

    # no future status data

    # no need future basic info

    # print('future_data len:{}'.format(len(ohlc_df)))
    return ohlc_df


def create_option_data_df(instruments, data_frequency="daily", start=None, end=None):
    """创建期权数据"""
    # 标的数据
    basic_df1 = DataSource("basic_info_CN_FUND").read(instruments)[["instrument", "name", "list_date", "delist_date"]]
    ohlc_df1 = DataSource("bar1d_CN_FUND").read(instruments, start_date=start, end_date=end)
    ohlc_df1 = pd.merge(ohlc_df1, basic_df1, on=["instrument"])
    for field in PRICE_FIELDS:
        ohlc_df1[field] = ohlc_df1[field] / ohlc_df1["adjust_factor"]
    # print(ohlc_df1[:2])
    # print('-' * 60)

    # 期权数据, FIXME: only read the option instruments of the underlying instrument
    name_df = DataSource("instruments_CN_OPTION").read(instruments=None, start_date=start, end_date=end)[["instrument", "name"]]
    name_dict = name_df.set_index("instrument").to_dict("index")
    name_df = pd.DataFrame(name_dict).T
    name_df["instrument"] = name_df.index
    option_instruments = list(name_df.instrument.unique())

    basic_df2 = DataSource("basic_info_CN_OPTION").read(option_instruments)
    basic_df2 = pd.merge(basic_df2, name_df, on="instrument")
    # print(basic_df2[:2])
    # print('=' * 60)

    ohlc_df2 = DataSource("bar1d_CN_OPTION").read(option_instruments, start_date=start, end_date=end)
    ohlc_df2 = pd.merge(ohlc_df2, basic_df2, on="instrument")
    # print(ohlc_df2[:2])
    # print('-' * 60)

    ohlc_df = pd.concat([ohlc_df1, ohlc_df2])

    return ohlc_df


def create_dcc_data_df(instruments, data_frequency=constants.frequency_daily, start=None, end=None):
    # 从BQ数据源 创建数据货币 目前只读日线
    # print('creating dcc data ', instruments, start, end)
    id_ohlcs = set()
    ohlc_dfs = []
    for instrument in instruments:
        if ".BIA" in instrument:
            id_ohlcs.add("BIA")
        elif ".HBI" in instrument:
            id_ohlcs.add("HBI")
        elif ".ZB" in instrument:
            id_ohlcs.add("ZB")
        elif ".BFN" in instrument:
            id_ohlcs.add("BFN")
        elif ".OKE" in instrument:
            id_ohlcs.add("OKE")

    for id in id_ohlcs:
        id_prefix = "bar1d_" if data_frequency == constants.frequency_daily else "bar1m_"
        id = id_prefix + id
        df = DataSource(id).read(instruments, start_date=start, end_date=end)
        #                                 fields=['open', 'high', 'low', 'close', 'volume'])
        if df is None or len(df) == 0:
            log.info("read empty data from {}".format(id))
            continue
        ohlc_dfs.append(df)

    # concat each df
    ohlc_df = pd.concat(ohlc_dfs)

    if "adjust_factor" not in ohlc_df.columns:
        ohlc_df["adjust_factor"] = 1.0

    # print('dcc_data len:{}'.format(len(ohlc_df)))
    return ohlc_df


def create_panel_by_datasource(
    instruments,
    start_date,
    end_date,
    price_type,
    data_frequency=constants.frequency_daily,
    is_write_ds=True,
    price_field_buy="open",
    price_field_sell="close",
):
    """
    instruments: list of instrument
    """
    # log.info('create_panel_by_datasource begin')

    if bigquant_debug_log:
        log.info(
            "backtestv8 create_panel_by_datasource instruments:{0}, sdate:{1}, edate:{2}, \
            price_type:{3}, frequnecy:{4}".format(
                len(instruments), start_date, end_date, price_type, data_frequency
            )
        )

    isDCC = False
    is_fund = False
    for instrument in instruments:
        if ".BIA" in instrument or ".HBI" in instrument or ".OKE" in instrument or ".ZB" in instrument or ".BFN" in instrument:
            isDCC = True
        elif ".ZOF" in instrument or ".HOF" in instrument or ".OFA" in instrument:
            is_fund = True
            break

    is_price_type_real = (price_type == constants.price_type_original) or (price_type == constants.price_type_real)
    df = None
    if isDCC:
        df = create_dcc_data_df(instruments, data_frequency=data_frequency, start=start_date, end=end_date)
        log.info("读取加密货币行情完成:{}".format(len(df)))
    elif is_fund:
        df = create_fund_data_df(
            instruments=instruments,
            data_frequency=data_frequency,
            start=start_date,
            end=end_date,
            is_price_type_real=is_price_type_real,
            price_field_buy=price_field_buy,
            price_field_sell=price_field_sell,
        )
        log.info("读取基金行情完成:{}".format(len(df)))
    elif get_is_stock():
        df = create_stock_data_df(
            instruments,
            data_frequency=data_frequency,
            start=start_date,
            end=end_date,
            is_price_type_real=is_price_type_real,
            price_field_buy=price_field_buy,
            price_field_sell=price_field_sell,
        )
        log.info("读取股票行情完成:{}".format(len(df)))
    elif get_is_option():
        df = create_option_data_df(instruments, data_frequency=data_frequency, start=start_date, end=end_date)
        log.info("读取期权行情完成:{}".format(len(df)))
    else:
        df = create_fut_data_df(instruments, data_frequency=data_frequency, start=start_date, end=end_date)
        log.info("读取期货行情完成:{}".format(len(df)))

    # check data fields
    if df is None or len(df) == 0:
        raise Exception("read empty ohlc {} data".format(data_frequency))
    for fld in BACKTEST_FIELDS:
        if fld not in df.columns:
            raise Exception("BigQuant needs data fields:{0}".format(BACKTEST_FIELDS))

    if "adjust_factor" in df.columns:
        df.adjust_factor.fillna(1.0, inplace=True)
    elif "adjust_factor" not in df.columns:
        df["adjust_factor"] = 1.0

    if is_price_type_real and data_frequency == constants.frequency_daily:
        for field in PRICE_FIELDS:
            df[field] = df[field] / df["adjust_factor"]

    if bigquant_debug_log:
        log.info("backtestv8 create_panel_by_datasource read() {0} records".format(len(df)))
        print(df[0:1])
        print(df[-1:])

    # !!temp disable here!!
    # if data_frequency == constants.frequency_daily:
    #     ret = df
    # else:
    #     # temp use panel when minute (since zipline is not ready)
    #     ret = df.set_index(['instrument', 'date']).to_panel().swapaxes(
    #         0, 1).swapaxes(1, 2)
    # log.info('create_panel_by_datasource end')

    # return dataframe directly, don't return panel @202102
    ret = df

    # return ret
    if is_write_ds:
        if isinstance(ret, pd.DataFrame):
            return Outputs(data=DataSource.write_df(ret))
        else:
            return Outputs(data=DataSource.write_pickle(ret))
    else:
        return Outputs(data=ret)


class BigQuantModule:
    def __init__(
        self,
        start_date,
        end_date,
        handle_data,
        instruments=None,
        prepare=None,
        initialize=None,
        before_trading_start=None,
        volume_limit=0.025,
        order_price_field_buy="open",
        order_price_field_sell="close",
        capital_base=float("1.0e6"),
        auto_cancel_non_tradable_orders=True,
        data_frequency="daily",
        plot_charts=True,
        show_progress_interval=0,
        show_debug_info=False,
        price_type=constants.price_type_post_right,
        product_type=None,
        history_data=None,
        benchmark_data=None,
        treasury_data=None,
        trading_calendar=None,
        options=None,
    ):
        """
        回测模块
        :param instruments: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param handle_data: 每个交易日的处理函数， handle_data(context, data)
        :param initialize: 初始化函数，initialize(context)
        :param before_trading_start: 在每个交易日开始前的处理函数，before_trading_start(context, data)
        :param order_price_field_buy: 买入价格字段，默认为 open
        :param order_price_field_sell: 卖出价格字段，默认为 close
        :param capital_base: 初始资金，默认为 1000000
        :param auto_cancel_non_tradable_orders: 是否自动取消不能成交的订单（停牌、张跌停），默认为 True
        :param price_type:回测价格类型，有pre_right(前复权)/post_right(后复权)/real(真实价格)，默认为post_right(后复权)
        :param product_type:回测产品类型，如 stock/future/option等，一般不用指定，系统自动根据合约代码判断产品类型
        :param history_data:回测历史数据，已做复权处理
        :param benchmark_data:回测基准数据, 可以是DataSource，DataFrame，或者股票/指数代码(如000300.HIX)
        :param treasury_data:回测无风险收益数据（暂时未用到）
        :param options: 其他参数从这里传入，可以在 handle_data 等函数里使用
        """
        # assert instruments is not None
        self.__prepare = prepare
        self._initialize = initialize
        self._handle_data = handle_data
        self._before_trading_start = before_trading_start
        self._start_date = start_date
        self._end_date = end_date
        self._instruments = instruments
        self._data_panel = None
        assert start_date is not None, "backtestv8: start_date param must not be None!"
        assert end_date is not None, "backtestv8: end_date param must not be None!"

        self._volume_limit = volume_limit
        self._order_price_field_buy = order_price_field_buy
        self._order_price_field_sell = order_price_field_sell
        self._capital_base = capital_base
        # self._benchmark = self._get_benchmark_symbol(benchmark_data)
        self._auto_cancel_non_tradable_orders = auto_cancel_non_tradable_orders
        self._show_progress_interval = show_progress_interval
        self._options = options
        self._show_debug_info = show_debug_info
        self._data_frequency = data_frequency
        self._is_daily = data_frequency == "daily"
        self._price_type = price_type
        self._plot_charts = plot_charts
        self._product_type = product_type

        self._history_data = history_data
        self._benchmark_data = benchmark_data
        self._treasury_data = treasury_data
        self._trading_calendar = trading_calendar
        self._bm_symbol = ""
        self._market = ""
        self._perf_raw_object = 0
        self._run_begin_time = datetime.now()

        GlobalValue.set_data_frequency(data_frequency)

    def _bq_initialize(self, context):
        from zipline.finance.commission import PerOrder

        # TODO: move this to TradingAlgorithm class
        extend_class_methods(context, has_unfinished_sell_order=BigQuantModule._has_unfinished_sell_order)
        extend_class_methods(context, has_unfinished_buy_order=BigQuantModule._has_unfinished_buy_order)

        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        context.extension = {}
        context.first_trading_date = self._start_date
        context.trading_day_index = -1
        context.show_debug_info = self._show_debug_info
        context.unfinished_sell_order_count = 0
        context.unfinished_buy_order_count = 0

        if self._initialize is not None:
            self._initialize(context)

    def _get_benchmark_symbol(self, symbol):
        if not isinstance(symbol, str):
            return symbol

        if "." not in symbol:
            if symbol.startswith("6"):
                return symbol[: symbol.index(".")] + ".SHA"
            else:
                return symbol[: symbol.index(".")] + ".SZA"

        if symbol.endswith(".INDX"):
            if symbol.startswith("0"):
                return symbol[: symbol.index(".")] + ".HIX"
            elif symbol.startswith("3"):
                return symbol[: symbol.index(".")] + ".ZIX"

        return symbol

    def _bq_handle_data(self, context, data):
        self._handle_data(context, data)
        BigQuantModule._record_positions_factor(context, data)

    def _bq_before_trading_start(self, context, data):
        BigQuantModule._clear_log(context)
        BigQuantModule._clear_transactions_factor(context)

        context.trading_day_index = context.trading_calendar.session_distance(
            pd.Timestamp(context.first_trading_date), pd.Timestamp(data.current_dt.strftime("%Y-%m-%d"))
        )

        if self._show_progress_interval > 0 and context.trading_day_index % self._show_progress_interval == 0:
            sys.stdout.write(".")

        # !optimize only for daily backtest
        if self._is_daily and self._auto_cancel_non_tradable_orders:
            BigQuantModule._auto_cancel_non_tradable_orders(context, data, True, True)

        if self._before_trading_start is not None:
            self._before_trading_start(context, data)

    @staticmethod
    def _clear_log(context):
        context.record(LOG=[])

    @staticmethod
    def _append_log(context, level, msg):
        rvs = context.recorded_vars
        if "LOG" in rvs:
            log_content = rvs["LOG"]
        else:
            log_content = []

        log_content.append({"level": level, "msg": msg})
        context.record(LOG=log_content)

    @staticmethod
    def _clear_transactions_factor(context):
        context.record(TRA_FAC={})

    @staticmethod
    def _append_transactions_factor(context, symbol, factor):
        rvs = context.recorded_vars
        if "TRA_FAC" in rvs:
            factors = rvs["TRA_FAC"]
        else:
            factors = {}
        factors[symbol] = factor
        context.record(TRA_FAC=factors)

    @staticmethod
    def _has_unfinished_sell_order(context, equity):
        orders = context.get_open_orders(equity)
        if not orders:
            return False

        for order in orders:
            if 0 >= order["filled"] > order["amount"] and order["amount"] < 0:
                if hasattr(context, "show_debug_info") and context.show_debug_info:
                    print("has unfinished sell order. equity symbol {}, filled {}, amount {}".format(equity.symbol, order["filled"], order["amount"]))
                context.unfinished_sell_order_count += 1
                return True

        return False

    @staticmethod
    def _has_unfinished_buy_order(context, equity):
        orders = context.get_open_orders(equity)
        if not orders:
            return False

        for order in orders:
            if 0 <= order["filled"] < order["amount"] and order["amount"] > 0:
                if hasattr(context, "show_debug_info") and context.show_debug_info:
                    print("has unfinished buy order. equity symbol {}, filled {}, amount {}".format(equity.symbol, order["filled"], order["amount"]))
                context.unfinished_buy_order_count += 1
            return True

        return False

    @staticmethod
    def _record_positions_factor(context, data):
        """记录持仓股票的除权值"""
        rvs = context.recorded_vars  # get last day record
        if "POS_FAC" in rvs:
            last_factors = rvs["POS_FAC"]
        else:
            last_factors = {}

        current_factors = {}
        current_positions = context.portfolio.positions
        for equity, position in current_positions.items():
            symbol = equity.symbol
            factor = data.current(equity, "adjust_factor")
            if np.isnan(factor):
                # mark here
                current_factors[symbol] = last_factors[symbol] if symbol in last_factors else 0.99999
            else:
                current_factors[symbol] = factor
        context.record(POS_FAC=current_factors)

    @staticmethod
    def auto_cancel_non_tradable_orders(context, data, cancel_for_suspended_stocks, cancel_for_price_limited_stocks):
        BigQuantModule._auto_cancel_non_tradable_orders(context, data, cancel_for_suspended_stocks, cancel_for_price_limited_stocks)

    @staticmethod
    def _auto_cancel_non_tradable_orders(context, data, cancel_for_suspended_stocks, cancel_for_price_limited_stocks):
        """处理不能成交的股票订单：停牌股票，一字张跌停股票。一般在 before_trading_start 里调用，撤销订单以释放资金占用。"""

        buy_moment = context.order_price_field_buy  # 买入时机
        sell_moment = context.order_price_field_sell  # 卖出时机

        PRICE_LIMIT_UPPER = 3
        PRICE_LIMIT_LOWER = 1

        # BIGQUANTC-385 Fix internal cancel order bacause zipline algorithm disable when price_field_sell is 'open'
        oldv = context._in_before_trading_start
        context._in_before_trading_start = False

        canceled_orders = []
        for open_orders in context.get_open_orders().values():
            for order in open_orders:
                sid = order.sid
                symbol = sid.symbol
                amount = order.amount
                current_values = data.current(sid, ["low", "high", "open", "close", "price_limit_status", "adjust_factor"])
                current_low = current_values["low"]
                current_high = current_values["high"]
                current_close = current_values["close"]
                current_price_limit_status = current_values["price_limit_status"]
                current_adjust_factor = current_values["adjust_factor"]

                # df = data.history(sid, ['close','high','low'], 2, '1d')
                # print('df price1:{0}, df_price2:{1}'.format(df.iloc[-1]['price'], df.iloc[-2]['price']))
                # is_price_increase = df.iloc[-1]['close'] > df.iloc[-2]['close']
                # is_price_decrease = df.iloc[-1]['close'] < df.iloc[-2]['close']

                # 停牌股票
                if cancel_for_suspended_stocks:
                    if np.isnan(current_low) or np.isnan(current_high) or np.isnan(current_close):
                        context.cancel_order(order)
                        canceled_orders.append(order)

                        if GlobalValue.get_is_stock():
                            BigQuantModule._append_log(context, "ERROR", "{}沒有市场数据，不能买卖".format(symbol))
                        else:
                            BigQuantModule._append_log(context, "ERROR", "{}没有市场数据或已退市，不能买卖".format(symbol))
                        continue

                # 若针对涨跌停股票不做处理, 则跳过
                if not cancel_for_price_limited_stocks:
                    continue

                # 早盘买入，并且当日一字涨停
                if amount > 0 and buy_moment == "open" and current_high == current_low and current_price_limit_status == PRICE_LIMIT_UPPER:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}一字涨停，不能买入".format(symbol))
                    continue

                # 尾盘买入，并且尾盘涨停
                if amount > 0 and buy_moment == "close" and current_price_limit_status == PRICE_LIMIT_UPPER:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}尾盘涨停，不能买入".format(symbol))
                    continue

                # 早盘卖出，并且当日一字跌停
                if amount < 0 and sell_moment == "open" and current_high == current_low and current_price_limit_status == PRICE_LIMIT_LOWER:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}一字跌停，不能卖出".format(symbol))
                    continue

                # 尾盘卖出，并且尾盘跌停
                if amount < 0 and sell_moment == "close" and current_price_limit_status == PRICE_LIMIT_LOWER:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}尾盘跌停，不能卖出".format(symbol))
                    continue

                # 记录除权
                BigQuantModule._append_transactions_factor(context, symbol, current_adjust_factor)
        context._in_before_trading_start = oldv
        return canceled_orders

    def _get_market_info(self, instruments):
        from zipline.data import load_cn, load_hk, load_us

        if len(instruments) == 0:
            return None

        if ".BIA" in instruments[0] or ".ZB" in instruments[0] or ".HBI" in instruments[0] or ".BFN" in instruments[0] or ".OKE" in instruments[0]:
            # add @20180605
            GlobalValue.set_round_num(6)
            return "DCC", "Asia/Shanghai", load_cn
        try:
            # markets = set([Market.TRADING_DAYS_MARKET_MAP[x.split('.')[1] + 'A'] for x in instruments])
            exchanges = [x.split(".")[1] for x in instruments]
            for i in range(len(exchanges)):
                if exchanges[i] == "SZ" or exchanges[i] == "SH":
                    exchanges[i] += "A"

            markets = set([Market.TRADING_DAYS_MARKET_MAP[x] for x in exchanges])
        except IndexError:
            log.error("输入股票列表有误,请检查确认!")
            return None
        except KeyError:
            # log.error('输入股票列表有误,请检查确认!')
            # return None
            # @20180806 允许任意市场数据输入
            markets = set()
            markets.add(exchanges[0])
            log.info("其它市场:{}".format(markets))

        if len(markets) > 1:
            log.error("含有来自不同交易日历市场的股票!")
            return None
        market = list(markets)[0]
        if market == Market.MG_CHINA.symbol:
            return "CN_Stock", "Asia/Shanghai", load_cn
        elif market == Market.MG_US.symbol:
            return "NYSE", "US/Eastern", load_us
        elif market == Market.MG_HK.symbol:
            return "HK", "Asia/Shanghai", load_hk
        else:
            # log.error('该交易日历下的市场尚未实现！')
            # return None
            # 允许任意市场数据后，默认返回中国A股市场的数据load
            return "CN_Stock", "Asian/Shanghai", load_cn

    def _get_holiday_list(self, market):
        # trading_days = D3.trading_days(market)
        trading_days = DataSource("trading_days").read(query="country_code=='CN'")[["date"]]
        trading_days = [x.date() for x in trading_days.date]
        start_date = min(trading_days)
        end_date = max(trading_days)
        holiday_list = sorted(set([x.date() for x in pd.date_range(start_date, end_date)]) - set(trading_days))
        return holiday_list

    def _to_date(self, dt):
        if not dt:
            return None
        dt = pd.to_datetime(dt)
        if self._data_frequency == constants.frequency_daily:
            dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0, 0, pytz.utc)
        else:
            dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 0, 0, pytz.utc)
        return dt

    def _get_product_type(self, instruments):
        if not isinstance(instruments, list):
            return "stock"
        for instrument in instruments:
            if "." not in instrument:
                continue
            _, exchange = instrument.split(".")

            if exchange in CN_SEC_MARKETS and exchange not in CN_OPT_MARKETS:
                return "stock"
            elif exchange in CN_FUT_MARKETS:
                return "future"
            elif exchange in CN_OPT_MARKETS:
                return "option"
            elif exchange in ["NYSE", "NASQ", "HKEX"]:
                return "stock"

        return "stock"

    def _handle_instruments(self, instruments):
        if isinstance(instruments, DataSource):
            data = instruments.read()
            if bigquant_has_panel and isinstance(data, pd.Panel):
                self._data_panel = data
                self._instruments = list(data.keys())
                # print('_instruments:', self._instruments)
        elif bigquant_has_panel and isinstance(instruments, pd.Panel):
            self._data_panel = instruments
            self._instruments = list(instruments.keys())
        else:
            self._instruments = instruments

        if bigquant_debug_log:
            log.info("{} instruments {} to {}".format(len(instruments), instruments[0], instruments[-1]))

        # @20180806 product type maybe from passed-in
        if self._product_type is None:
            self._product_type = self._get_product_type(instruments)
            log.info("product_type:{} by instrument0 {}".format(self._product_type, instruments[0]))
        else:
            log.info("product_type:{} by specified".format(self._product_type))
        GlobalValue.set_is_stock((self._product_type == "stock" or self._product_type == "dcc"))
        if self._product_type == "option":
            GlobalValue.set_is_option(True)

    def run_algo(self):
        from zipline.algorithm import TradingAlgorithm
        from zipline.finance.trading import TradingEnvironment
        from zipline.utils.calendars import TradingCalendar, calendar_utils

        log.info("biglearning backtest:{0}".format(BIGQUANT_Back_V8_Version))
        if "BIGQUANT_DEBUG_LOG" in os.environ:
            bigquant_debug_log = True  # noqa

        mock = ContextMock(instruments=self._instruments, start_date=self._start_date, end_date=self._end_date, options=self._options)
        if self.__prepare is not None:
            self.__prepare(mock)

        self._handle_instruments(mock.instruments)
        mock.instruments = self._instruments

        start_date = self._to_date(mock.start_date)
        end_date = self._to_date(mock.end_date)
        # TODO: byzhang ? what is this for? do we need this?
        # if ('instruments' not in mock.__dict__ is None and self._instruments is None) \
        #    or ('instruments' in mock.__dict__ and mock.instruments is None):
        #     print('!!WARNING: 为了对接模拟和实盘，平台最近做了一个重要更新，有可能导致部分策略不能运行，请使用最新的模板和生成器来生成代码')
        #     return None

        ret = self._get_market_info(self._instruments)
        if ret is None:
            return None
        market, exchange_tz, load = ret

        trading_calendar = self._trading_calendar
        if trading_calendar is None:
            if market == "HK":
                from zipline.utils.calendars import HKExchangeCalendar

                trading_calendar = HKExchangeCalendar(self._get_holiday_list(Market.MG_HK.symbol))
            else:
                trading_calendar = calendar_utils.get_calendar_by_instruments(self._instruments, self._data_frequency)
            self._trading_calendar = trading_calendar
        else:
            if isinstance(trading_calendar, DataSource):
                trading_calendar = trading_calendar.read()
            assert isinstance(trading_calendar, TradingCalendar), "trading_calendar instance must be type of 'TradingCalendar'"

        data_end_date = end_date
        if self._data_frequency == constants.frequency_daily:
            weeks = 26 if get_is_option() else 52
            data_start_date = start_date - timedelta(weeks=weeks)
        else:
            data_start_date = start_date - timedelta(days=90)
            if end_date.hour == 0 and end_date.minute == 0:
                data_end_date = end_date.replace(hour=16)

        use_m_cached = False if data_end_date.date() >= datetime.now().date() else True

        # try read benchmark data from data source firstly
        bm_symbol = None
        benchmark_df = None
        if isinstance(self._benchmark_data, DataSource):
            benchmark_df = self._benchmark_data.read()
        elif isinstance(self._benchmark_data, str) or self._benchmark_data is None:
            if self._benchmark_data is None:
                bm_symbol = "000300.HIX"
            else:
                bm_symbol = self._get_benchmark_symbol(self._benchmark_data)

            # @202005 new read
            benchmark_df = read_benchmark_df(bm_symbol, start=data_start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
        else:
            raise Exception("输入错误的benchmark数据")

        if len(benchmark_df) == 0:
            log.warn("未读取到benchmark数据")
        elif bm_symbol is None:
            bm_symbol = benchmark_df["instrument"].iloc[0]
        self._bm_symbol = bm_symbol
        self._market = market

        # comment out treasury_df since it is not actually used currently @20180505
        treasury_df = self._treasury_data
        try:
            # TODO yingzhi
            raise NotImplementedError()
        except Exception:
            treasury_df = None

        env = TradingEnvironment(
            load=load,
            exchange_tz=exchange_tz,
            trading_calendar=trading_calendar,
            asset_db_path=None,  # @20190516 不使用zipline默认的asset数据处理方式
            bm_symbol=bm_symbol,
            benchmark_df=benchmark_df,
            treasury_df=treasury_df,
        )
        algo = TradingAlgorithm(
            initialize=self._bq_initialize,
            handle_data=self._bq_handle_data,
            before_trading_start=self._bq_before_trading_start,
            volume_limit=self._volume_limit,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            capital_base=self._capital_base,
            data_frequency=self._data_frequency,
            price_type=self._price_type,
            start=start_date,
            end=end_date,
            env=env,
            product_type=self._product_type,
            trading_calendar=trading_calendar,
            perf_raw_object=self._perf_raw_object,
            show_debug_info=self._show_debug_info,
        )

        algo.outputs = Outputs()
        # algo.instruments = self._instruments
        # algo.start_date = self._start_date
        # algo.end_date = self._end_date
        # algo.options = self._options
        algo.__dict__.update(mock.__dict__)

        if algo.instruments is None:
            print("!!WARNING: 为了对接模拟和实盘，平台最近做了一个重要更新，有可能导致部分策略不能运行，请使用最新的模板和生成器来生成代码")
            return None

        if isinstance(self._history_data, DataSource):
            self._data_panel = self._history_data.read()
        elif use_m_cached:
            from learning.api import M

            cache_kwargs = {
                "instruments": algo.instruments,
                "start_date": data_start_date.strftime("%Y-%m-%d"),
                "end_date": data_end_date.strftime("%Y-%m-%d %H:%M:%S"),
                "price_type": self._price_type,
                "data_frequency": self._data_frequency,
                "price_field_buy": self._order_price_field_buy,
                "price_field_sell": self._order_price_field_sell,
            }
            self._history_data = M.cached.v2(run=create_panel_by_datasource, kwargs=cache_kwargs).data
            self._data_panel = self._history_data.read()
        else:
            created_data = create_panel_by_datasource(
                algo.instruments,
                data_start_date.strftime("%Y-%m-%d"),
                data_end_date.strftime("%Y-%m-%d %H:%M:%S"),
                self._price_type,
                data_frequency=self._data_frequency,
                is_write_ds=0,
                price_field_buy=self._order_price_field_buy,
                price_field_sell=self._order_price_field_sell,
            )
            self._data_panel = created_data.data

        if self._data_panel is None:
            raise Exception("创建回测需要的历史数据失败")
        if (bigquant_has_panel and isinstance(self._data_panel, pd.Panel) and len(self._data_panel.major_axis) <= 0) or (
            isinstance(self._data_panel, pd.DataFrame) and len(self._data_panel) == 0
        ):
            # return None
            raise Exception("未读取到回测需要的历史数据")

        if algo.show_debug_info:
            print(datetime.now(), "algo history_data={}".format(self._history_data))

        # 运行回测
        algo_result = algo.run(self._data_panel, overwrite_sim_params=False)

        """ @20190218 comment out, 由于现在不存在隔夜单，即一笔订单不会在多天完成
        if algo.unfinished_buy_order_count > 0:
            print("[注意] 有 {} 笔买入是在多天内完成的。当日买入股票超过了当日股票交易的{}%会出现这种情况。"
                  .format(algo.unfinished_buy_order_count, self._volume_limit * 100))
        if algo.unfinished_sell_order_count > 0:
            print("[注意] 有 {} 笔卖出是在多天内完成的。当日卖出股票超过了当日股票交易的{}%会出现这种情况。"
                  .format(algo.unfinished_sell_order_count, self._volume_limit * 100))
        """

        # 回测结果为一个DataFrame，索引为日期时间
        algo_result.sort_index(inplace=True)

        raw_perf = DataSource.write_df(algo_result)

        return Outputs(
            data=self._data_panel,
            algo=algo,
            raw_perf=raw_perf,
            show_debug_info=self._show_debug_info,
            data_frequency=self._data_frequency,
            product_type=self._product_type,
        )

    def run(self):
        run_algo_result = self.run_algo()
        if run_algo_result is None:
            print("!!WARNING: v8回测结果返回为空")
            return None

        if RUN_MODE == "STRATEGY_BACKTEST":
            self._save_backtest_to_sql(run_algo_result)
        
        if isinstance(self._data_panel, pd.DataFrame):
            data_panel = DataSource.write_df(self._data_panel)
        else:
            data_panel = DataSource.write_pickle(self._data_panel)

        return Outputs(
            raw_perf=run_algo_result.raw_perf,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            context_outputs=run_algo_result.algo.outputs,
            plot_charts=self._plot_charts,
            __start_date=self._start_date,
            __end_date=self._end_date,
            start_date=self._start_date,
            end_date=self._end_date,
            benchmark=self._bm_symbol,
            market=self._market,
            data_frequency=self._data_frequency,
            product_type=self._product_type,
            perf_raw_object=self._perf_raw_object,
            data_panel=data_panel,
            capital_base=self._capital_base,
            price_type=self._price_type,
            volume_limit=self._volume_limit,
        )

    def _get_stats(self, results):
        if self._product_type == "stock" or self._product_type == "dcc":
            res = get_stats_stock(results)
        else:
            res = get_stats_future(results)
        for k, v in  res.items():
            try:
                res[k] = float(res[k])
            except Exception:
                res[k] = -1.0
        return res 


    def _save_backtest_to_sql(self, algo_result):
        """save backtest to mysql"""
        import json
        backtest_id = os.getenv("BACKTEST_ID")
        credential = Credential.from_env()
        df: pd.DataFrame = algo_result.raw_perf.read()

        res = self._get_stats(df)

        df.reset_index(inplace=True)
        df.rename(columns={"index": "run_date"}, inplace=True)
        df_columns = df.columns
        data_list = [v for k, v in json.loads(df.T.to_json()).items()]
        daily_data = []
        for i in data_list:
            daily_data.append(BacktestDailyPerformanceSchema(**i))
    
        performance = CreateBacktestPerformanceRequest(
            run_date=datetime.now(),
            backtest_id=backtest_id,
            return_ratio=float(res["return_ratio"]),
            annual_return_ratio=float(res["annual_return_ratio"]),
            benchmark_ratio=float(res["benchmark_ratio"]),
            alpha=float(res["alpha"]),
            sharp=float(res["sharp_ratio"]),
            ir=float(res["ir"]),
            return_volatility=float(res["return_volatility"]),
            max_drawdown=float(res["max_drawdown"]),
            win_ratio=float(res["win_ratio"]),
            profit_loss_ratio=float(res["profit_loss_ratio"]),
            daily_data=daily_data,
        )
        StrategyClient.update_backtest_performance(params=performance, credential=credential)


def bigquant_postrun(outputs):
    from .postrun import brinson_analysis, display, factor_profit_analyze, pyfolio_full_tear_sheet, read_data_panel, read_raw_perf, risk_analyze

    extend_class_methods(outputs, display=display)

    extend_class_methods(outputs, read_raw_perf=read_raw_perf)

    extend_class_methods(outputs, read_data_panel=read_data_panel)

    # extend_class_methods(outputs, pyfolio_full_tear_sheet=pyfolio_full_tear_sheet)

    # extend_class_methods(outputs, risk_analyze=risk_analyze)

    # extend_class_methods(outputs, factor_profit_analyze=factor_profit_analyze)

    # extend_class_methods(outputs, brinson_analysis=brinson_analysis)

    if outputs.plot_charts:
        outputs.display()
    return outputs


def bigquant_cache_key(kwargs):
    kwargs["__internal_version"] = "8.0.7"
    return kwargs


if __name__ == "__main__":
    # test code here
    pass
