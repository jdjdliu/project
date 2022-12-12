# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime, timedelta
from warnings import warn

import numpy as np
import pandas as pd
from learning.module2.common.data import Outputs
from learning.settings import site
from sdk.datasource import DataSource
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

log = BigLogger("hfbacktest")

bigquant_cacheable = True
bigquant_public = False

"""
v1.4.2 Add replay_bdb param
v1.4.3 Support only pass-in minute data (auto resample daily data) to do backtest
v1.4.4 Optimize replay bdb minute data
v1.4.10 Optimize not read each data when replay bdb
v1.4.11 Fix order_price_field_buy/sell
v1.4.12 Fix auto read dominant data
v1.4.14 Fix not cache benchmark data if disable_cache
"""
bigquant_hftest_version = "V1.4.14"
bigquant_debug_log = False

g_trading_calendar = None
g_end_date = None


def read_history_data(start_date, end_date, instruments, product_type, frequency, adjust_type=None, is_write_ds=1, **kwargs):
    from bigtrader.constant import AdjustType
    from bigtrader.mdata.data_source import HistoryDataSource as HistDS

    HistDS.set_data_source(DataSource)

    if not adjust_type:
        adjust_type = AdjustType.NONE

    df = HistDS.read_history_data(
        start_date=start_date,
        end_date=end_date,
        instruments=instruments,
        product_type=product_type,
        frequency=frequency,
        adjust_type=adjust_type,
        **kwargs
    )
    if df is None:
        return Outputs(data=None)
    df.reset_index(inplace=True, drop=True)
    data = DataSource.write_df(df) if is_write_ds else df
    return Outputs(data=data)


def read_history_each_data(start_date, end_date, instruments, product_type, is_write_ds=1):
    from bigtrader.mdata.data_source import HistoryDataSource as HistDS

    HistDS.set_data_source(DataSource)

    df = HistDS.read_l2trade_data(instruments=instruments, start_date=start_date, end_date=end_date, product_type=product_type)
    if df is None:
        return Outputs(data=None)
    data = DataSource.write_df(df) if is_write_ds else df
    return Outputs(data=data)


class BigQuantModule:
    def __init__(
        self,
        start_date,
        end_date,
        instruments=None,
        prepare=None,
        initialize=None,
        after_trading=None,
        on_timer=None,
        on_message=None,
        before_trading_start=None,
        handle_data=None,
        handle_tick=None,
        handle_l2trade=None,
        handle_l2order=None,
        handle_trade=None,
        handle_order=None,
        capital_base=1e6,
        product_type=None,
        adjust_type=None,
        frequency="1m",
        benchmark="000300.HIX",
        plot_charts=True,
        options_data=None,
        **kwargs
    ):
        """
        回测模块
        :param instruments: 代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param initialize: 初始化函数，initialize(context)
        :param on_timer: 定时触发函数，一般不用于回测，on_timer(context, t)
        :param on_message: 外部消息通知触发函数，一般不用于回测，on_message(context, msg)
        :param after_trading: 策略运行结束处理函数，after_trading(context)
        :param before_trading_start: 在每个交易日开始前的处理函数，before_trading_start(context, data)
        :param handle_data: 每个bar更新时的处理函数， handle_data(context, data)
        :param handle_tick: 在每个Tick快照行情更新时的处理函数，handle_tick(context, tick)
        :param handle_l2trade: 在每个逐笔成交行情更新时的处理函数，handle_l2trade(context, l2trade)
        :param handle_l2order: 在每个逐笔委托行情更新时的处理函数，handle_l2order(context, l2order)
        :param handle_trade: 在成交回报更新时的处理函数，handle_trade(context, trade)
        :param handle_order: 在委托回报更新时的处理函数，handle_order(context, order)
        :param capital_base: 初始资金，默认为 1000000
        :param frequency: 回测频率，如 1d/1m/tick/tick2
        :param product_type:回测产品类型，如 stock/future/option等，一般不用指定，系统自动根据合约代码判断产品类型
        :param adjust_type: 回测复权类型，如 真实价格[real]，后复权[post]
        :param benchmark:回测基准数据, 可以是DataSource，DataFrame，或者股票/指数代码(如000300.HIX)
        :param options_data: 其他参数从这里传入，可以在 handle_data 等函数里使用 context.options['data']
        """
        # assert instruments is not None
        # from os import environ
        # environ["BT_LOG_MODULE"] = "logging"

        from bigtrader.constant import AdjustType, Frequency, Product, SymbolExchangeMode
        from bigtrader.mdata.data_source import HistoryDataSource as HistDS

        self._is_site_citics = site == "citics"

        self._strategy = {}
        if prepare:
            self._strategy["prepare"] = prepare
        if initialize:
            self._strategy["on_init"] = initialize
        if after_trading:
            self._strategy["on_stop"] = after_trading
        if on_timer:
            self._strategy["on_timer"] = on_timer
        if on_message:
            self._strategy["on_message"] = on_message
        if before_trading_start:
            self._strategy["on_start"] = before_trading_start
        if handle_data:
            self._strategy["handle_data"] = handle_data
        if kwargs.get("handle_bar"):
            self._strategy["handle_bar"] = kwargs.pop("handle_bar")
        if handle_tick:
            self._strategy["handle_tick"] = handle_tick
        if handle_l2trade:
            self._strategy["handle_l2trade"] = handle_l2trade
        if handle_l2order:
            self._strategy["handle_l2order"] = handle_l2order
        if handle_trade:
            self._strategy["handle_trade"] = handle_trade
        if handle_order:
            self._strategy["handle_order"] = handle_order
        if kwargs.get("handle_instrument_status"):
            self._strategy["handle_instrument_status"] = kwargs.pop("handle_instrument_status")
        if kwargs.get("handle_user_data"):
            self._strategy["handle_user_data"] = kwargs.pop("handle_user_data")

        if not len(self._strategy):
            raise Exception("no strategy callbacks!")

        self._start_date = start_date
        self._end_date = end_date
        self._instruments = instruments
        self._kwargs = kwargs
        self._positions = kwargs.pop("positions", None)
        self._strategy_setting = kwargs.pop("strategy_setting", None)
        self._disable_cache = kwargs.pop("disable_cache", 0)
        self._show_debug_info = kwargs.pop("show_debug_info", "BIGQUANT_DEBUG_LOG" in os.environ)
        self._before_start_days = max(kwargs.pop("before_start_days", 0), 0)
        self._exchange_mode = kwargs.pop("exchange_mode", SymbolExchangeMode.BQ)
        self._replay_bdb = kwargs.pop("replay_bdb", 0)

        global bigquant_debug_log
        bigquant_debug_log = self._show_debug_info

        assert start_date is not None, "hfbacktest: start_date param must not be None!"
        assert end_date is not None, "hfbacktest: end_date param must not be None!"

        self._order_price_field_buy = kwargs.pop("order_price_field_buy", "open")
        self._order_price_field_sell = kwargs.pop("order_price_field_sell", "open")
        self._capital_base = capital_base
        self._options_data = options_data
        self._product_type = product_type or HistDS.get_product_by_instruments(instruments)
        self._plot_charts = plot_charts
        self._bm_symbol = benchmark
        self._market = ""  # Just try do same as backtest.v8

        if not adjust_type:
            adjust_type = kwargs.pop("price_type", AdjustType.NONE)
        if adjust_type in ["后复权", "post", "post_right", "backward_adjusted", AdjustType.POST]:
            self._adjust_type = AdjustType.POST
        elif adjust_type in ["前复权", "pre", "pre_right", "forward_adjusted", AdjustType.PRE]:
            self._adjust_type = AdjustType.PRE
        else:
            self._adjust_type = AdjustType.NONE

        if frequency in ["1d", "daily"]:
            self._frequency = Frequency.DAILY
        else:
            self._frequency = frequency

        self._have_product_instrument = 0
        if instruments and self._product_type == Product.FUTURE:
            for instrument in instruments:
                if len(instrument) <= 2 or "888" in instrument:
                    self._have_product_instrument = 1
                    break

        self._daily_data_ds = kwargs.pop("daily_data_ds", None)
        self._minute_data_ds = kwargs.pop("minute_data_ds", None)
        self._tick_data_ds = kwargs.pop("tick_data_ds", None)
        self._each_data_ds = kwargs.pop("each_data_ds", None)
        self._dominant_data_ds = kwargs.pop("dominant_data_ds", None)
        self._benchmark_data_ds = kwargs.pop("benchmark_data_ds", None)
        self._trading_calendar_ds = kwargs.pop("trading_calendar", None)

        if self._show_debug_info:
            log.info("passed-in daily_data_ds:{}".format(self._daily_data_ds))
            log.info("passed-in minute_data_ds:{}".format(self._minute_data_ds))
            log.info("passed-in tick_data_ds:{}".format(self._tick_data_ds))
            log.info("passed-in each_data_ds:{}".format(self._each_data_ds))
            log.info("passed-in dominant_data_ds:{}".format(self._dominant_data_ds))
            log.info("passed-in benchmark_data_ds:{}".format(self._benchmark_data_ds))
            log.info("passed-in trading_calendar_ds:{}".format(self._trading_calendar_ds))

        self._daily_data = self._daily_data_ds.read() if self._daily_data_ds else None
        self._minute_data = self._minute_data_ds.read() if self._minute_data_ds else None
        self._tick_data = self._tick_data_ds.read() if self._tick_data_ds else None
        self._each_data = self._each_data_ds.read() if self._each_data_ds else None
        self._dominant_data = self._dominant_data_ds.read() if self._dominant_data_ds else None
        self._benchmark_data = self._benchmark_data_ds.read() if self._benchmark_data_ds else None
        self._dividend_data = None
        self._basic_info_data = None
        self._trading_calendar = self._trading_calendar_ds.read() if self._trading_calendar_ds else None

        self._cached_daily_ds = None
        self._cached_minute_ds = None
        self._cached_tick_ds = None
        self._cached_each_ds = None
        self._cached_dominant_ds = None
        self._cached_benchmark_ds = None

    def create_history_data(self):
        from bigtrader.constant import AdjustType, Frequency, Product
        from bigtrader.mdata.data_source import HistoryDataSource as HistDS
        from bigtrader.utils.bar_generator import BarGenerator
        from bigtrader.utils.datetime_tools import to_pydatetime
        from learning.api import M

        if (
            self._frequency not in [Frequency.TICK, Frequency.TICK2]
            and self._before_start_days > 0
            and self._product_type in [Product.EQUITY, Product.FUND]
        ):
            # Maybe need do pre-adjust history data, cannot use bdb
            self._replay_bdb = 0

        # if self._replay_bdb:
        #     # Check can use bdb or not in this environment
        #     from bigdatasource.api.utils.bdb import get_table_files  # TODO

        #     _files = get_table_files("holidays_CN")
        #     if len(_files) and _files[0].endswith(".bdb"):
        #         self._replay_bdb = 1
        #     else:
        #         self._replay_bdb = 0
        #         log.info("Not use replay bdb mode because running env.")

        data_start_date = self._to_date(self._start_date)
        benchmark_start_date = self._trading_calendar.get_prev_trading_day(data_start_date)
        daily_start_date = self._trading_calendar.get_start_date(246, end_date=data_start_date)
        minute_start_date = self._trading_calendar.get_start_date(self._before_start_days, end_date=data_start_date)
        dominant_start_date = self._trading_calendar.get_start_date(7, end_date=data_start_date)

        show_debug_info = self._show_debug_info

        # @202111 do not cache if today is end date
        disable_cache = self._disable_cache
        if not disable_cache and datetime.now().date() == to_pydatetime(self._end_date).date():
            disable_cache = 1

        if show_debug_info:
            log.info(
                "begin reading history data, {}~{}, disable_cache:{}, replay_bdb:{}".format(
                    data_start_date, self._end_date, disable_cache, self._replay_bdb
                )
            )

        if self._benchmark_data is None and self._bm_symbol:
            if show_debug_info:
                log.info("reading benchmark data {}~{}...".format(benchmark_start_date, self._end_date))
            kwargs0 = {
                "start_date": benchmark_start_date.strftime("%Y-%m-%d"),
                "end_date": pd.Timestamp(self._end_date).strftime("%Y-%m-%d"),
                "instruments": [self._bm_symbol],
                "frequency": Frequency.DAILY,
                "product_type": Product.NONE,  # not only index
                "adjust_type": AdjustType.POST,  # use post-adjusted
            }
            if disable_cache:
                self._benchmark_data_ds = None
                self._benchmark_data = read_history_data(is_write_ds=0, **kwargs0).data
            else:
                self._benchmark_data_ds = M.cached.v2(run=read_history_data, kwargs=kwargs0).data
                self._benchmark_data = self._benchmark_data_ds.read()

        # Do resample df is passed in minute/tick data
        if not self._daily_data_ds:
            if self._minute_data_ds and self._minute_data is not None:
                if show_debug_info:
                    log.info("resample daily data by minute data...")
                self._daily_data = BarGenerator.resample_df(self._minute_data, "1d")
                self._daily_data_ds = DataSource.write_df(self._daily_data)
            elif self._tick_data_ds and self._tick_data is not None:
                if show_debug_info:
                    log.info("resample daily data by tick data...")
                self._daily_data = BarGenerator.resample_df(self._tick_data, "1d")
                if "close" not in self._daily_data.columns:
                    self._daily_data.rename(columns={"price": "close"}, inplace=True)
                self._daily_data_ds = DataSource.write_df(self._daily_data)

        if self._daily_data is None:
            if show_debug_info:
                log.info("reading daily data {}~{}...".format(daily_start_date, self._end_date))
            kwargs = {
                "start_date": daily_start_date.strftime("%Y-%m-%d"),
                "end_date": self._end_date,
                "instruments": self._instruments,
                "frequency": Frequency.DAILY,
                "product_type": self._product_type if self._product_type == Product.OPTION else Product.NONE,
                "adjust_type": AdjustType.NONE,
            }
            if disable_cache:
                self._cached_daily_ds = None
                self._daily_data = read_history_data(is_write_ds=0, **kwargs).data
            else:
                self._cached_daily_ds = M.cached.v2(run=read_history_data, kwargs=kwargs).data
                self._daily_data = self._cached_daily_ds.read()

        # @202111 dynamic read history data when transform events
        need_read_history_data = 0
        if (
            not self._replay_bdb
            and self._frequency.endswith(("m", "minute"))
            and (self._product_type == Product.FUTURE or self._product_type == Product.OPTION)
        ):
            if len(self._instruments) <= 20:
                need_read_history_data = 1

        if (
            need_read_history_data
            and self._minute_data is None
            and (self._frequency.endswith(("m", "minute")) or self._kwargs.get("need_minute_data"))
        ):
            minute_start_date = self._trading_calendar.get_prev_trading_day(minute_start_date).replace(hour=20, minute=55)
            if show_debug_info:
                log.info("reading minute data {}~{}...".format(minute_start_date, self._end_date))
            kwargs = {
                "start_date": minute_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": self._end_date,
                "instruments": self._instruments,
                "frequency": self._frequency,
                "product_type": Product.NONE,
                "adjust_type": AdjustType.NONE,
            }
            if disable_cache:
                self._cached_minute_ds = None
                self._minute_data = read_history_data(is_write_ds=0, **kwargs).data
            else:
                self._cached_minute_ds = M.cached.v2(run=read_history_data, kwargs=kwargs).data
                self._minute_data = self._cached_minute_ds.read() if self._cached_minute_ds else None

        if need_read_history_data and self._tick_data is None and self._frequency in [Frequency.TICK, Frequency.TICK2]:
            if isinstance(self._start_date, str) and len(self._start_date) <= 10:
                tick_start_date = self._trading_calendar.get_prev_trading_day(self._start_date).replace(hour=20, minute=55)
                tick_start_date = tick_start_date.strftime("%Y-%m-%d %H:%M:%S")
            else:
                tick_start_date = self._start_date

            if show_debug_info:
                log.info("reading {} data {}~{}...".format(self._frequency, tick_start_date, self._end_date))
            kwargs = {
                "start_date": tick_start_date,
                "end_date": self._end_date,
                "instruments": self._instruments,
                "frequency": self._frequency,
                "product_type": self._product_type,  # FIXME: change as Product.NONE ?
            }
            if disable_cache:
                self._cached_tick_ds = None
                self._tick_data = read_history_data(is_write_ds=0, **kwargs).data
            else:
                self._cached_tick_ds = M.cached.v2(run=read_history_data, kwargs=kwargs).data
                self._tick_data = self._cached_tick_ds.read()

        if (
            need_read_history_data
            and self._each_data is None
            and self._frequency == Frequency.TICK2
            and (self._strategy.get("handle_l2trade") or self._strategy.get("handle_l2order"))
        ):
            if show_debug_info:
                log.info("reading each trade data {}~{}...".format(self._start_date, self._end_date))
            kwargs2 = {
                "start_date": self._start_date,
                "end_date": self._end_date,
                "instruments": self._instruments,
                "product_type": self._product_type,
            }
            if disable_cache:
                self._cached_each_ds = None
                kwargs2["is_write_ds"] = 0
                self._each_data = read_history_each_data(**kwargs2).data
            else:
                kwargs2["is_write_ds"] = 1
                self._cached_each_ds = M.cached.v2(run=read_history_each_data, kwargs=kwargs2).data
                self._each_data = self._cached_each_ds.read()

        if self._dominant_data is None and self._have_product_instrument:
            if show_debug_info:
                log.info("reading dominant data {}~{}...".format(dominant_start_date, self._end_date))

            def _create_dominant_data(instruments, start_date, end_date, is_write_ds=1):
                df = HistDS.read_future_dominant(instruments, start_date, end_date)
                data = DataSource.write_df(df) if is_write_ds else df
                return Outputs(data=data)

            kwargs3 = {"start_date": dominant_start_date.strftime("%Y-%m-%d"), "end_date": self._end_date, "instruments": self._instruments}
            if disable_cache:
                self._dominant_data_ds = None
                self._dominant_data = _create_dominant_data(is_write_ds=0, **kwargs3).data
            else:
                self._dominant_data_ds = M.cached.v2(run=_create_dominant_data, kwargs=kwargs3).data
                self._dominant_data = self._dominant_data_ds.read()

        # FIXME: no Product.FUND data
        # @202203 we temp not use dividend data
        if 0 and self._product_type in [Product.EQUITY] and not self._daily_data_ds:
            fields = ["ex_date", "bonus_conversed_sum", "cash_before_tax", "cash_after_tax"]
            self._dividend_data = HistDS.read_dividend_infos(self._instruments, daily_start_date, self._end_date, fields=fields)
        elif not self._is_site_citics and self._product_type == Product.FUTURE:
            self._basic_info_data = HistDS.read_future_basic_info(self._instruments)
        elif not self._is_site_citics and self._product_type == Product.OPTION:
            self._basic_info_data = HistDS.read_option_basic_info(self._instruments)

        if self._show_debug_info:
            log.info("cached_benchmark_ds:{}".format(self._benchmark_data_ds))
            log.info("cached_daily_ds:{}".format(self._cached_daily_ds))
            log.info("cached_minute_ds:{}".format(self._cached_minute_ds))
            log.info("cached_tick_ds:{}".format(self._cached_tick_ds))
            log.info("cached_each_ds:{}".format(self._cached_each_ds))
            log.info("dominant_data_ds:{}".format(self._dominant_data_ds))
            if self._dividend_data is not None:
                log.info("dividend_ds:{}".format(DataSource.write_df(self._dividend_data)))

    def run_algo(self):
        from bigtrader import RELEASE_DATE, RELEASE_VERSION
        from bigtrader.mdata.data_source import HistoryDataSource as HistDS
        from bigtrader.run_trading import run_backtest
        from bigtrader.utils.trading_calendar import BTCalendar

        log.info("biglearning {}".format(bigquant_hftest_version))
        log.info("bigtrader v{} {}".format(RELEASE_VERSION, RELEASE_DATE))

        HistDS.set_data_source(DataSource)

        if self._trading_calendar is None:
            # FIXME: cache the global trading_calendar for CN
            global g_trading_calendar
            global g_end_date
            if g_trading_calendar is None or (g_end_date and g_end_date != self._end_date):
                holidays = HistDS.read_holidays_cn()
                g_trading_calendar = BTCalendar("CN_Stock")
                g_trading_calendar.set_holidays_set(holidays)
                g_end_date = self._end_date
            self._trading_calendar = g_trading_calendar

        show_debug_info = self._show_debug_info
        if show_debug_info:
            log.info("strategy callbacks:{}".format(self._strategy))

        self.create_history_data()

        if show_debug_info:
            log.info("read history data done, call run_backtest()")

        algo = run_backtest(
            self._start_date,
            self._end_date,
            strategy=self._strategy,
            instruments=self._instruments,
            positions=self._positions,
            capital=self._capital_base,
            product_type=self._product_type,
            adjust_type=self._adjust_type,
            frequency=self._frequency,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            daily_df=self._daily_data,
            minute_df=self._minute_data,
            tick_df=self._tick_data,
            each_data_df=self._each_data,
            dominant_df=self._dominant_data,
            benchmark_df=self._benchmark_data,
            dividend_df=self._dividend_data,
            basic_info_df=self._basic_info_data,
            trading_calendar=self._trading_calendar,
            options_data=self._options_data,
            strategy_setting=self._strategy_setting,
            show_debug_info=self._show_debug_info,
            dbpath=None,
            save_md=False,
            before_start_days=self._before_start_days,
            exchange_mode=self._exchange_mode,
            replay_bdb=self._replay_bdb,
            **self._kwargs
        )

        # 回测结果大多为一个DataFrame，索引为日期时间
        algo_result = algo.get_perf_result()
        if not isinstance(algo_result, dict):
            algo_result.sort_index(inplace=True)
            raw_perf_ds = DataSource.write_df(algo_result)
        else:
            for result in algo_result.values():
                result.sort_index(inplace=True)
            raw_perf_ds = DataSource.write_pickle(algo_result)

        log.info("backtest done, raw_perf_ds:{}".format(raw_perf_ds))

        params = algo.get_backtest_params()
        try:
            params["start_date"] = params["start_date"].strftime("%Y-%m-%d %H:%M:%S")
            params["end_date"] = params["end_date"].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            params["start_date"] = params["start_date"]
            params["end_date"] = params["end_date"]
        params.pop("positions", None)
        params.pop("kwargs", None)
        if self._strategy_setting is None:
            params.pop("strategy_settings", None)
        params.pop("instruments", None)

        return Outputs(algo=algo, raw_perf=raw_perf_ds, params=params, data_frequency=self._frequency, product_type=self._product_type)

    def run(self):
        run_algo_result = self.run_algo()

        if run_algo_result is None:
            print("!!WARNING: hfbacketstv1回测结果返回为空")
            return None

        algo = run_algo_result.algo

        # @202104 Fix strategy run too long, and the ds was deleted
        if self._benchmark_data_ds:
            if self._benchmark_data_ds.read() is None:
                self._benchmark_data_ds = DataSource.write_df(self._benchmark_data)

        return Outputs(
            raw_perf=run_algo_result.raw_perf,
            params=run_algo_result.params,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            plot_charts=self._plot_charts,
            start_date=self._start_date,
            end_date=self._end_date,
            capital_base=self._capital_base,
            # instruments=self._instruments,
            data_frequency=self._frequency,
            price_type=self._adjust_type,
            benchmark=self._bm_symbol,
            market=self._market,
            product_type=self._product_type,
            volume_limit=0,
            daily_data=self._cached_daily_ds,
            minute_data=self._cached_minute_ds,
            tick_data=self._cached_tick_ds,
            each_data_data=self._cached_each_ds,
            benchmark_data=self._benchmark_data_ds,
            get_all_orders=algo.get_all_orders,
            get_all_trades=algo.get_all_trades,
            get_positions=algo.get_account_position_datas,
            show_debug_info=self._show_debug_info,
        )

    def _to_date(self, dt):
        if not dt:
            return None
        return pd.Timestamp(dt).to_pydatetime().replace(second=0, microsecond=0)


def bigquant_postrun(outputs):
    from .postrun import (
        analyze_pnl_per_day,
        analyze_pnl_per_trade,
        display,
        plot_curve_return,
        plot_return_curve_per_stock,
        plot_trade_points,
        read_one_raw_perf,
        read_raw_perf,
    )

    extend_class_methods(outputs, display=display)
    extend_class_methods(outputs, read_raw_perf=read_raw_perf)
    extend_class_methods(outputs, read_one_raw_perf=read_one_raw_perf)

    extend_class_methods(outputs, analyze_pnl_per_trade=analyze_pnl_per_trade)
    extend_class_methods(outputs, analyze_pnl_per_day=analyze_pnl_per_day)
    extend_class_methods(outputs, plot_return_curve_per_stock=plot_return_curve_per_stock)
    extend_class_methods(outputs, plot_curve_return=plot_curve_return)
    extend_class_methods(outputs, plot_trade_points=plot_trade_points)

    if outputs.plot_charts:
        outputs.display()

    return outputs


def bigquant_cache_key(kwargs):
    kwargs["__internal_version"] = bigquant_hftest_version
    return kwargs


if __name__ == "__main__":
    # test code here
    pass
