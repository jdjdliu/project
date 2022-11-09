# -*- coding: utf-8 -*-
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz
from learning.module2.common.data import DataSource, Outputs
from sdk.datasource import DataReaderV2
from sdk.datasource.bigdata import constants
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

from .perf_render import render

D2 = DataReaderV2()
log = BigLogger("backtest")

bigquant_cacheable = True
bigquant_public = False


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
        benchmark="000300.SHA",
        auto_cancel_non_tradable_orders=True,
        show_progress_interval=0,
        show_debug_info=False,
        price_type=constants.price_type_post_right,
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
        :param benchmark: 基准指数，可以用股票或者指数代码
        :param auto_cancel_non_tradable_orders: 是否自动取消不能成交的订单（停牌、张跌停），默认为 True
        :param price_type:回测价格类型，有pre_right(前复权)/post_right(后复权)/real(真实价格)，默认为post_right(后复权)
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
        self._volume_limit = volume_limit
        self._order_price_field_buy = order_price_field_buy
        self._order_price_field_sell = order_price_field_sell
        self._capital_base = capital_base
        self._benchmark = benchmark
        self._auto_cancel_non_tradable_orders = auto_cancel_non_tradable_orders
        self._show_progress_interval = show_progress_interval
        self._options = options
        self._price_type = price_type
        self._show_debug_info = show_debug_info

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

        if self._auto_cancel_non_tradable_orders:
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

                # 停牌股票
                if cancel_for_suspended_stocks:
                    if np.isnan(current_low) or np.isnan(current_high) or np.isnan(current_close):
                        context.cancel_order(order)
                        canceled_orders.append(order)
                        BigQuantModule._append_log(context, "ERROR", "{}停牌，不能买卖".format(symbol))
                        continue

                # 若针对涨跌停股票不做处理, 则跳过
                if not cancel_for_price_limited_stocks:
                    continue

                # tushare数据，并且一字行情
                if np.isnan(current_price_limit_status) and current_high == current_low:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    continue

                # 早盘买入，并且当日一字涨停
                if amount > 0 and buy_moment == "open" and current_high == current_low and current_price_limit_status == 3:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}一字涨停，不能买入".format(symbol))
                    continue

                # 尾盘买入，并且尾盘涨停
                if amount > 0 and buy_moment == "close" and current_price_limit_status == 3:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}尾盘涨停，不能买入".format(symbol))
                    continue

                # 早盘卖出，并且当日一字跌停
                if amount < 0 and sell_moment == "open" and current_high == current_low and current_price_limit_status == 1:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}一字跌停，不能卖出".format(symbol))
                    continue

                # 尾盘卖出，并且尾盘跌停
                if amount < 0 and sell_moment == "close" and current_price_limit_status == 1:
                    context.cancel_order(order)
                    canceled_orders.append(order)
                    BigQuantModule._append_log(context, "ERROR", "{}尾盘跌停，不能卖出".format(symbol))
                    continue

                # 记录除权
                BigQuantModule._append_transactions_factor(context, symbol, current_adjust_factor)
        return canceled_orders

    def run_algo(self):
        from zipline.algorithm import TradingAlgorithm
        from zipline.finance.trading import TradingEnvironment
        from zipline.utils.calendars import calendar_utils

        start = datetime.strptime(self._start_date, "%Y-%m-%d")
        end = datetime.strptime(self._end_date, "%Y-%m-%d")
        start = datetime(start.year, start.month, start.day, 0, 0, 0, 0, pytz.utc)
        end = datetime(end.year, end.month, end.day, 0, 0, 0, 0, pytz.utc)

        # Create and run the algorithm.  TODO: better way to detect market
        if self._benchmark and self._benchmark.endswith(".NYSE"):
            market = "NYSE"
            exchange_tz = "US/Eastern"
            from zipline.data import load_us

            load = load_us
        else:
            market = "CN_Stock"
            exchange_tz = "Asia/Shanghai"
            from zipline.data import load_cn

            load = load_cn
        trading_calendar = calendar_utils.get_calendar(market)

        # Add @2018-06-29 since zipline needs df now
        data_start_date = start - timedelta(weeks=52)
        benchmark_df = D2.history_data(
            [self._benchmark], start_date=data_start_date, end_date=end, fields=["date", "instrument", "open", "high", "low", "close", "volume"]
        )

        env = TradingEnvironment(
            load=load, exchange_tz=exchange_tz, trading_calendar=trading_calendar, bm_symbol=self._benchmark, benchmark_df=benchmark_df
        )
        algo = TradingAlgorithm(
            initialize=self._bq_initialize,
            handle_data=self._bq_handle_data,
            before_trading_start=self._bq_before_trading_start,
            volume_limit=self._volume_limit,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            capital_base=self._capital_base,
            data_frequency=constants.frequency_daily,
            price_type=self._price_type,
            start=start,
            end=end,
            env=env,
            trading_calendar=trading_calendar,
        )
        # TODO: refactor code
        algo.outputs = Outputs()
        algo.instruments = self._instruments
        algo.start_date = self._start_date
        algo.end_date = self._end_date
        algo.options = self._options
        if self.__prepare is not None:
            self.__prepare(algo)

        if algo.instruments is None:
            print("!!WARNING: 为了对接模拟和实盘，平台最近做了一个重要更新，有可能导致部分策略不能运行，请使用最新的模板和生成器来生成代码")
            return None
        if isinstance(algo.instruments, pd.Panel):
            data = algo.instruments
        else:
            data_start_date = start - timedelta(weeks=52)
            data = D2.fast_history_data_for_backtest(algo.instruments, data_start_date, end, price_type=self._price_type)
        if len(data.major_axis) <= 0:
            return None

        algo_result = algo.run(data, overwrite_sim_params=False)

        if algo.unfinished_buy_order_count > 0:
            print("[注意] 有 {} 笔买入是在多天内完成的。当日买入股票超过了当日股票交易的{}%会出现这种情况。".format(algo.unfinished_buy_order_count, self._volume_limit * 100))
        if algo.unfinished_sell_order_count > 0:
            print("[注意] 有 {} 笔卖出是在多天内完成的。当日卖出股票超过了当日股票交易的{}%会出现这种情况。".format(algo.unfinished_sell_order_count, self._volume_limit * 100))

        algo_result.sort_index(inplace=True)
        raw_perf = DataSource.write_df(algo_result)

        return Outputs(data=data, algo=algo, raw_perf=raw_perf, show_debug_info=self._show_debug_info)

    def run(self):
        run_algo_result = self.run_algo()

        if run_algo_result is None:
            print("!!WARNING: 回测结果返回为空")
            return None

        return Outputs(
            raw_perf=run_algo_result.raw_perf,
            order_price_field_buy=self._order_price_field_buy,
            order_price_field_sell=self._order_price_field_sell,
            context_outputs=run_algo_result.algo.outputs,
        )


def bigquant_postrun(outputs):
    algo_result = outputs.raw_perf.read_df()
    render(algo_result, outputs.order_price_field_buy, outputs.order_price_field_sell)

    def read_raw_perf(self):
        return outputs.raw_perf.read_df()

    extend_class_methods(outputs, read_raw_perf=read_raw_perf)

    def pyfolio_full_tear_sheet(self):
        import pyfolio as pf

        perf = self.read_raw_perf()
        returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(perf)
        pf.tears.create_full_tear_sheet(returns, positions, transactions, gross_lev=gross_lev, benchmark_rets=perf.benchmark_period_return)

    extend_class_methods(outputs, pyfolio_full_tear_sheet=pyfolio_full_tear_sheet)

    def risk_analyze(self):
        result_df = self.read_raw_perf()
        from .risk_analyze import factor_analyze, industry_analyze

        factor_analyze(result_df)
        industry_analyze(result_df)

    extend_class_methods(outputs, risk_analyze=risk_analyze)

    return outputs


def bigquant_cache_key(kwargs):
    kwargs["__internal_version"] = "0.0.1"
    return kwargs


if __name__ == "__main__":
    # test code here
    pass
