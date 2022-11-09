# -TODO: reuse backtest code
# -*- coding:utf-8 -*-
import copy
import json
import math
import uuid
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

# from biglearning.module.common.wechat import WeChatClient
from learning.api import M
from learning.module2.common.data import DataSource, Outputs

# from biglearning.module2.modules.backtest.v7 import create_panel_from_df
from learning.module2.modules.backtest.v8 import CN_FUT_MARKETS, CN_OPT_MARKETS, CN_SEC_MARKETS
from learning.module2.modules.backtest.v8 import BigQuantModule as Btm
from learning.module2.modules.backtest.v8 import bigquant_has_panel, create_panel_by_datasource, get_is_stock, set_is_stock
from sdk.auth import Credential
from sdk.datasource import DataReaderV2, constants
from sdk.datasource.extensions.bigshared.utils import extend_class_methods
from sdk.strategy import StrategyClient
from sdk.strategy.schemas import StrategyDailySchema, StrategyPerformanceSchema, CreateStrategyDailySchema
from sdk.utils import BigLogger

from .paper_test_helper import PaperTestHelper
from .record_datas import RecordDatas, get_ago_portfolio_dict, get_win_ratio

D2 = DataReaderV2()
# log = logbook.Logger('forward_test')
log = BigLogger("forward_test")


epoch = datetime.utcfromtimestamp(0)
bigquant_cacheable = False
bigquant_public = False

BIGQUANT_Forward_V5_Version = "V5.5.0"
RUN_MODE = os.getenv("RUN_MODE", "")

class BigQuantModule:
    def __init__(
        self,
        prepare,
        initialize,
        handle_data,
        run_date,
        first_trading_date,  # 这个参数无效
        algo_id,
        instruments=None,
        before_trading_start=None,
        order_price_field_buy="open",
        order_price_field_sell="close",
        auto_cancel_non_tradable_orders=True,
        data_frequency=constants.frequency_daily,
        price_type=constants.price_type_post_right,
        product_type="stock",  # 暂未具体用到
        volume_limit=0.025,
        options=None,
        email_to=None,
        wechat_to=None,
        history_data=None,
        benchmark_data=None,
        benchmark_symbol=None,
        treasury_data=None,
        trading_calendar=None,
    ):
        self._user_name = os.environ["JPY_USER"]
        self._prepare = prepare
        self._initialize = initialize
        self._handle_data = handle_data
        self._before_trading_start = before_trading_start
        self._order_price_field_buy = order_price_field_buy
        self._order_price_field_sell = order_price_field_sell
        self._algo_id = algo_id
        self._notebook_id = ""
        self._run_date = run_date
        self._first_trading_date = first_trading_date if isinstance(first_trading_date, str) else first_trading_date.strftime("%Y-%m-%d")
        self._algo_name = ""
        self._algo_desc = ""
        self._email_to = email_to
        self._wechat_to = wechat_to
        self._extra_message = ""
        self._auto_cancel_non_tradable_orders = auto_cancel_non_tradable_orders
        self._instruments = instruments if instruments else []
        self._options = options
        self._data_frequency = data_frequency
        self._price_type = price_type
        self._volume_limit = volume_limit
        self._product_type = self._get_product_type(self._instruments)
        self._credential = Credential.from_env()
        set_is_stock(self._product_type == "stock")

        # Fix BIGQUANTC-225
        if self._order_price_field_buy == "low":
            self._order_price_field_buy = "open"
        if self._order_price_field_sell == "high":
            self._order_price_field_sell = "close"

        try:
            log.info("init options data:{}".format(options["data"]))
        except:
            pass

        log.info(
            "init username:{0}, algo_id:{1}, first_date:{2},\
            frequency:{3}, price_type:{4}, instruments:{5}".format(
                self._user_name, self._algo_id, self._first_trading_date, self._data_frequency, self._price_type, self._instruments[:32]
            )
        )
        log.info("init benchmark_symbol={}".format(benchmark_symbol))
        assert self._user_name is not None
        assert self._run_date is not None
        assert self._algo_id is not None

        self._history_data = history_data
        self._benchmark_data = benchmark_data
        self._benchmark_symbol = benchmark_symbol if benchmark_symbol else "000300.HIX"
        self._treasury_data = treasury_data
        self._trading_calendar = trading_calendar

        # 20190318 是否使用第三方撮合
        self._is_third_match = 0
        self._ohlc_data = None

        self.paper_test_helper = PaperTestHelper(
            self._price_type, get_is_stock(), self._run_date, self._instruments, self._first_trading_date, self._benchmark_symbol
        )

    def _get_product_type(self, instruments):
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

        return "stock"

    def run(self):
        from zipline import TradingAlgorithm
        from zipline.data import load_cn
        from zipline.finance.commission import PerOrder
        from zipline.finance.execution import MarketOrder
        from zipline.finance.performance import DIRECTION_LONG, DIRECTION_SHORT, Position_Future, Position_Stock, PositionInfo
        from zipline.finance.performance.position import positiondict
        from zipline.finance.trading import TradingEnvironment
        from zipline.protocol import Positions
        from zipline.utils.calendars import TradingCalendar, calendar_utils

        log.info("biglearning forward test:{0}".format(BIGQUANT_Forward_V5_Version))
        try:
            is_stock = get_is_stock()

            equity_algo = StrategyClient.get_strategy_by_id(strategy_id=self._algo_id, credential=self._credential)
            try:
                performance = StrategyClient.get_performance_by_strategy_id(strategy_id=self._algo_id, credential=self._credential)
                performance_id = performance.id
            except Exception:
                performance = None
                performance_id = uuid.uuid4()
            # TODO: check attributes
            self._notebook_id = equity_algo.task_id
            self._algo_name = equity_algo.name
            self._algo_desc = equity_algo.description
            self._is_third_match = equity_algo.parameter.is_third_match
            log.info(f"run_date: {self._run_date}, strategy_name:{self._algo_name}, is_third_match:{self._is_third_match}")

            record_datas = RecordDatas(self._algo_id, self._run_date)
            self.paper_test_helper.set_record_datas(record_datas)

            # get cash & positions & orders & extensions & first_trading_date
            original_capital_base = equity_algo.parameter.capital_base  # 初始本金
            cash = original_capital_base
            max_pv = original_capital_base  # 最大资产,默认为初始资金
            drawdown = 0.0  # 最大回撤
            positions = []
            last_positions = []
            last_portfolio = {}
            orders = []
            extension = {}
            is_sync = 0

            if self._is_third_match:
                # 使用第三方模拟撮合的策略：资金、持仓均使用当日的
                if record_datas.today_record:
                    log.info("third_match has today_record!")

                    # !mark as 1 since will verify it nextly!
                    record_datas.today_record.is_sync = 1
                else:
                    log.warn("third_match no today_record, maybe not upload live data yet!")

                if record_datas.last_record:
                    last_positions = record_datas.last_record.positions
                    last_portfolio = record_datas.last_record.portfolio
                    orders_date = record_datas.last_record.run_date
                    max_pv = last_portfolio.get("max_pv", cash)
                    drawdown = last_portfolio.get("drawdown", 0)
                    log.info("third_match last_record last_positions:{}, last_portfolio:{}".format(len(last_positions), last_portfolio))
                else:
                    log.info("third_match no last_record")
                    orders_date = self._run_date

            if record_datas.today_record:  # today is synced
                cash = record_datas.today_record.cash
                positions = record_datas.today_record.positions
                if not is_stock:
                    cash += sum((pos["margin_used"] - (pos["holding_pnl"] if ("holding_pnl" in pos) else 0)) for pos in positions)
                is_sync = 1

                log.info("today_record.is_sync cash:{}".format(cash))
                for _pos in positions:
                    log.info("position: {}".format(_pos))
                # F-IXME: maybe need loads today orders
            elif record_datas.last_record:
                cash = record_datas.last_record.cash
                positions = record_datas.last_record.positions
                # print('-----poses:', positions)
                if not is_stock:
                    cash += sum((pos["margin_used"] - (pos["holding_pnl"] if ("holding_pnl" in pos) else 0)) for pos in positions)
                last_positions = positions
                last_portfolio = record_datas.last_record.portfolio
                orders = [] if not record_datas.last_record.orders else record_datas.last_record.orders
                orders_date = record_datas.last_record.run_date
                # 获取至上一交易日为止的最大资产，以便与当日资产进行比较
                max_pv = last_portfolio["max_pv"]
                # 获取至上一交易日为止的最大回撤，以便与当日最大回撤进行比较
                drawdown = last_portfolio["drawdown"]

                if record_datas.last_record.extension:
                    extension = record_datas.last_record.extension
                else:
                    log.warn("last_record's extension is None")

                log.info("last_record is_sync cash:{}, last_portfolio:{}".format(cash, last_portfolio))
                log.info("last_record is_sync extension:{}".format(extension))

            if record_datas.first_record:
                self._first_trading_date = record_datas.first_record.run_date.strftime("%Y-%m-%d")
            else:
                self._first_trading_date = self._run_date
            if datetime.strptime(self._run_date, "%Y-%m-%d") < datetime.strptime(self._first_trading_date, "%Y-%m-%d"):
                self._first_trading_date = self._run_date
                log.warning("run_date < first_trading_date in history")

            def c_initialize(context):
                # default commission
                context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
                context.first_trading_date = self._first_trading_date
                context.trading_day_index = context.trading_calendar.session_distance(
                    pd.Timestamp(self._first_trading_date), pd.Timestamp(self._run_date)
                )
                context.has_unfinished_sell_order = lambda e: Btm._has_unfinished_sell_order(context, e)
                context.has_unfinished_buy_order = lambda e: Btm._has_unfinished_buy_order(context, e)
                context.unfinished_sell_order_count = 0
                context.unfinished_buy_order_count = 0
                log.info("c_initialize() first_trading_date:{}, trading_day_index:{}".format(context.first_trading_date, context.trading_day_index))
                self._initialize(context)

                # initialize positions
                total_margin = 0.0
                starting_value = 0.0
                if len(positions) > 0:
                    if self._is_third_match:
                        self.paper_test_helper.init_live_positions(positions, self._ohlc_data)

                    pf_positions = Positions(context)
                    pt_positions = positiondict()
                    for _p in positions:
                        if is_stock:
                            asset = context.asset_finder.lookup_symbol(_p["sid"], None)
                            pos = Position_Stock(asset=asset)
                            pos.amount = _p["amount"]

                            # fix bug BIGQUANT-656, we allow short position for stocks
                            if pos.amount == 0:
                                continue
                        else:
                            asset = context.asset_finder.lookup_future_symbol(_p["sid"])
                            pos = Position_Future(asset=asset)
                            pos.amount = _p["amount"]
                            pos.long_position = PositionInfo(DIRECTION_LONG)
                            pos.short_position = PositionInfo(DIRECTION_SHORT)
                            pos.long_position.amount = _p["amount_long"]
                            pos.short_position.amount = _p["amount_short"]

                            if pos.long_position.amount == 0 and pos.short_position.amount == 0:
                                log.debug("c_initialize() no future position, ignore it {0}".format(pos.log_str()))
                                continue
                            # log.info('c_initialize() future position:{0}'.format(pos.log_str()))
                            pos.long_position.cost_basis = _p["cost_basis_long"]
                            pos.short_position.cost_basis = _p["cost_basis_short"]
                            pos.long_position.cost_basis_yesterday = _p["cost_basis_long"]
                            pos.short_position.cost_basis_yesterday = _p["cost_basis_short"]
                            total_margin += _p["margin_used"]

                        pos.cost_basis = _p["cost_basis"]
                        pos.last_sale_price = _p["last_sale_price"]
                        pos.last_sale_date = pd.to_datetime(_p["last_sale_date"], utc=True) if "last_sale_date" in _p else None

                        # log.info(
                        #     'c_initialize() init position:{0}'.format(pos.log_str()))

                        pf_positions[asset] = pos
                        pt_positions[asset] = pos
                        starting_value += pos.amount * pos.last_sale_price
                    context.portfolio.positions = pf_positions
                    context.perf_tracker.position_tracker.update_positions(pt_positions)
                    context.perf_tracker.cumulative_performance.starting_value = starting_value
                log.info("c_initialize() positions len:{0}, value:{1}".format(len(positions), starting_value))

                # initialize cash
                context.capital_base = cash
                context.portfolio.starting_cash = cash
                context.portfolio.cash = cash
                context.perf_tracker.cumulative_performance.starting_cash = cash

                # initialize orders
                log.info("c_initialize() orders len:{0}".format(len(orders)))
                if len(orders) > 0 and not self._is_third_match:
                    session_between = context.trading_calendar.session_distance(pd.Timestamp(orders_date), pd.Timestamp(self._run_date))
                    if session_between == 1:
                        for _o in orders:
                            if get_is_stock():
                                asset = context.asset_finder.lookup_symbol(_o["sid"], None)
                            else:
                                asset = context.asset_finder.lookup_future_symbol(_o["sid"])
                            s_amount = _o["amount"]
                            position_effect = None
                            if "position_effect" in _o:
                                position_effect = _o["position_effect"]

                            # log.info(
                            #     'c_initialize() order: {0}'.format(_o))
                            context.blotter.order(asset, s_amount, position_effect, MarketOrder(), is_stored_order=True)

                # fix BIGQUANT-658, handle splits only when use real price
                if self._price_type in [constants.price_type_real, constants.price_type_original] and not self._is_third_match:
                    context.handle_splits_for_paper_trading()

            def c_before_trading_start(context, data):
                if self._auto_cancel_non_tradable_orders:
                    Btm.auto_cancel_non_tradable_orders(context, data, True, True)  # TO-DO: check later
                if self._before_trading_start:
                    self._before_trading_start(context, data)

            def c_handle_data(context, data):
                self._handle_data(context, data)
                self._extra_message = context.extra_notification_message if hasattr(context, "extra_notification_message") else ""

            # Set the simulation start and end dates. _run_date: e.g. '2016-10-12'
            terms = self._run_date.split("-")
            run_date = datetime(int(terms[0]), int(terms[1]), int(terms[2]), 0, 0, 0, 0, pytz.utc)

            # create and run the algorithm. TODO: support other markets
            trading_calendar = self._trading_calendar
            if trading_calendar is None:
                trading_calendar = calendar_utils.get_calendar_by_instruments(self._instruments, self._data_frequency)
            else:
                assert isinstance(trading_calendar, TradingCalendar), "trading_calendar instance must be type of 'TradingCalendar'"
            log.info("forward test trading_calendar done!")

            data_start_date = run_date - timedelta(weeks=52)
            # bm_symbol = '000300.SHA'

            # try read benchmark dataframe from data source
            benchmark_df = None
            if isinstance(self._benchmark_data, DataSource):
                benchmark_df = self._benchmark_data.read()
                log.info("forward test read benchmark_df {} from _benchmark_data ds done!".format(True if benchmark_df is not None else False))
            else:
                # benchmark_df = D2.history_data([self._benchmark_symbol], start_date=data_start_date.replace(tzinfo=None), end_date=run_date.replace(tzinfo=None))
                # F-IXME: only support bar1d_index
                benchmark_symbol = self._benchmark_symbol
                if benchmark_symbol.startswith("00") and benchmark_symbol.endswith("SHA"):
                    benchmark_symbol = benchmark_symbol.replace("SHA", "HIX")
                if benchmark_symbol.startswith("39") and benchmark_symbol.endswith("SZA"):
                    benchmark_symbol = benchmark_symbol.replace("SZA", "ZIX")
                benchmark_fields = ["date", "instrument", "open", "high", "low", "close", "volume"]
                benchmark_df = DataSource("bar1d_index_CN_STOCK_A").read(
                    [benchmark_symbol],
                    start_date=data_start_date.replace(tzinfo=None),
                    end_date=run_date.replace(tzinfo=None),
                    fields=benchmark_fields,
                )
                if benchmark_df is None or benchmark_df.empty:
                    benchmark_df = DataSource("bar1d_CN_STOCK_A").read(
                        [benchmark_symbol],
                        start_date=data_start_date.replace(tzinfo=None),
                        end_date=run_date.replace(tzinfo=None),
                        fields=benchmark_fields,
                    )
                log.info("forward test read benchmark_df {} from DataSource done!".format(True if benchmark_df is not None else False))
            if benchmark_df is not None:
                log.info("benchmark_df:\n{}".format(benchmark_df.tail()))
            else:
                log.warn("benchmark_df is None by symbol={}".format(self._benchmark_symbol))

            # treasury_df = self._treasury_data
            treasury_df = None

            try:
                raise NotImplementedError()
            except Exception:
                treasury_df = None

            # 2018-05-23 add by yzhzheng for algo output debug info when paper trading
            algo_show_debug = True

            env = TradingEnvironment(
                load=load_cn,
                exchange_tz="Asia/Shanghai",
                trading_calendar=trading_calendar,
                bm_symbol=benchmark_symbol,
                benchmark_df=benchmark_df,
                treasury_df=treasury_df,
            )
            algo = TradingAlgorithm(
                initialize=c_initialize,
                handle_data=c_handle_data,
                before_trading_start=c_before_trading_start,
                order_price_field_buy=self._order_price_field_buy,
                order_price_field_sell=self._order_price_field_sell,
                capital_base=cash,
                start=run_date,
                end=run_date,
                env=env,
                trading_calendar=trading_calendar,
                data_frequency=self._data_frequency,
                price_type=self._price_type,
                product_type=self._product_type,
                show_debug_info=algo_show_debug,
                volume_limit=self._volume_limit,
                extension=extension,
                papertrading=True,
            )

            # -TODO: refactor code
            algo.outputs = Outputs()
            algo.instruments = self._instruments
            algo.start_date = self._run_date
            algo.end_date = self._run_date
            algo.options = self._options
            if self._prepare is not None:
                self._prepare(algo)

            # setup equity data
            if algo.instruments is not None and bigquant_has_panel and isinstance(algo.instruments, pd.Panel):
                ohlc_data = algo.instruments
            else:
                instruments = copy.deepcopy(self._instruments) if self._instruments else []
                log.info("instruments type {0}, {1}, {2}".format(type(self._instruments), type(instruments), type(algo.instruments)))
                if isinstance(algo.instruments, np.ndarray):
                    algo.instruments = list(algo.instruments)
                instruments += algo.instruments if algo.instruments is not None else []
                instruments += [p["sid"] for p in positions]
                instruments += [o["sid"] for o in orders]
                instruments = list(set(instruments))

                ohlc_data = None
                log.info("start create history data")
                sorted_instruments = sorted(instruments)
                if isinstance(self._history_data, DataSource):
                    log.info("_history_data is {}".format(self._history_data))
                    ohlc_data = self._history_data.read()
                elif os.environ.get("ENABLE_CACHE", "True") in ["True", "TRUE", "1"]:
                    cache_kwargs = {
                        "instruments": sorted_instruments,
                        "start_date": data_start_date.strftime("%Y-%m-%d"),
                        "end_date": run_date.strftime("%Y-%m-%d"),
                        "price_type": self._price_type,
                        "price_field_buy": self._order_price_field_buy,
                        "price_field_sell": self._order_price_field_sell,
                    }
                    ohlc_data = M.cached.v2(run=create_panel_by_datasource, kwargs=cache_kwargs)
                    log.info("create history data ENABLE_CACHE is True, ohlc_data_ds:{}".format(ohlc_data.data))
                    ohlc_data = ohlc_data.data.read()
                else:
                    # @20200628 run paper trading using vwap/twap price
                    ohlc_data = create_panel_by_datasource(
                        instruments=sorted_instruments,
                        start_date=data_start_date.strftime("%Y-%m-%d"),
                        end_date=run_date.strftime("%Y-%m-%d"),
                        price_type=self._price_type,
                        data_frequency=self._data_frequency,
                        price_field_buy=self._order_price_field_buy,
                        price_field_sell=self._order_price_field_sell,
                    )
                    log.info("create history data ENABLE_CACHE is False, directly reading ohlc_data_ds:{}".format(ohlc_data.data))
                    ohlc_data = ohlc_data.data.read()
                log.info("end create history data type={}.".format(type(ohlc_data)))
                if isinstance(ohlc_data, pd.DataFrame):
                    log.info("ohlc_data=\n{}".format(ohlc_data.tail()))
                else:
                    log.error("ohlc_data is error:{}".format(ohlc_data))
                # print(ohlc_data)
            # if len(ohlc_data.major_axis) <= 0:
            if (bigquant_has_panel and isinstance(ohlc_data, pd.Panel) and len(ohlc_data.major_axis) <= 0) or (
                isinstance(ohlc_data, pd.DataFrame) and len(ohlc_data) == 0
            ):
                log.error("no OHLC data available for date %s" % self._run_date)
                return None

            self._ohlc_data = ohlc_data

            log.info("forward test get_adjust_factor_map...")
            self.paper_test_helper.get_adjust_factor_map(last_positions, ohlc_data)

            # init equity names by 'instruments' after invoking prepare function
            if self._product_type == "stock":
                log.info("forward test init_equity_name_map...")
                self.paper_test_helper.init_equity_name_map(instruments, run_date, run_date)

            # run zipline TradingAlgorithm
            algo_result = algo.run(ohlc_data, overwrite_sim_params=False)
            algo_result.sort_index(inplace=True)
            result_last_row = algo_result.tail(1)

            # only extract some columns to log
            result_last_row_1 = result_last_row[
                [
                    "algorithm_period_return",
                    "alpha",
                    "benchmark_period_return",
                    "beta",
                    "capital_used",
                    "ending_cash",
                    "ending_value",
                    "long_value",
                    "longs_count",
                    "max_drawdown",
                ]
            ]
            result_last_row_2 = result_last_row[
                [
                    "period_close",
                    "period_label",
                    "period_open",
                    "pnl",
                    "portfolio_value",
                    "returns",
                    "short_value",
                    "shorts_count",
                    "sortino",
                    "starting_cash",
                    "starting_value",
                    "trading_days",
                ]
            ]
            log.info("get last row of algo result_1:\n{}".format(result_last_row_1))
            log.info("get last row of algo result_2:\n{}".format(result_last_row_2))
            if len(result_last_row) <= 0:
                log.error("no algo result for trade date {}".format(run_date))
                return None
            # cash
            new_cash = result_last_row["ending_cash"].values[0]
            # print('---------new_cash:',new_cash)

            # Fix process futures
            if "adjust_factor" not in self._ohlc_data.columns:
                self._ohlc_data["adjust_factor"] = 1

            # orders
            new_orders_df, new_orders, json_orders = self.paper_test_helper.get_orders(result_last_row, ohlc_data, self._run_date)
            # print('---------new_orders:',new_orders)
            # print('---------json_orders:',json_orders)
            # positions
            if not self._is_third_match:
                last_sale_date_dict = self.paper_test_helper.get_last_sale_date_dict(positions, algo_result)
                new_positions, json_positions = self.paper_test_helper.get_positions(result_last_row, last_sale_date_dict, last_positions)
            else:
                # !attention, here new_positions' type is different from result_last_row!
                new_positions, json_positions = positions, json.dumps(positions)
                for pos in new_positions:
                    log.info("#3rd pos:{}".format(pos))
                log.info("#position_value:{0}".format(self.paper_test_helper.position_value))
            # print('---------new_positions:',new_positions)
            # print('--------json_positions:',json_positions)
            # portfolio
            trading_day_index = trading_calendar.session_distance(pd.Timestamp(self._first_trading_date), pd.Timestamp(self._run_date))
            portfolio_value = result_last_row["portfolio_value"].values[0]
            json_portfolio = self.paper_test_helper.get_portfolio(
                portfolio_value, original_capital_base, last_positions, max_pv, drawdown, trading_day_index
            )
            # print('---------json_portfolio:',json_portfolio)
            # transactions
            json_transactions = self.paper_test_helper.get_transactions(result_last_row)
            # print('---------json_transactions:',json_transactions)
            # logs
            json_logs = self.paper_test_helper.get_json_logs(result_last_row)
            # print("---------json_logs:", json_logs)

            # benchmark 沪深300
            # run_date - timedelta(days=15)
            benchmark_start_date = self._first_trading_date
            print("reading benchmark data...", self._benchmark_symbol, benchmark_start_date, run_date)
            # benchmark = D2.history_data([self._benchmark_symbol], benchmark_start_date, run_date.replace(tzinfo=None), fields=['close'])
            benchmark_symbol = self._benchmark_symbol
            if benchmark_symbol.startswith("00") and benchmark_symbol.endswith("SHA"):
                benchmark_symbol = benchmark_symbol.replace("SHA", "HIX")
            if benchmark_symbol.startswith("39") and benchmark_symbol.endswith("SZA"):
                benchmark_symbol = benchmark_symbol.replace("SZA", "ZIX")
            benchmark_df = DataSource("bar1d_index_CN_STOCK_A").read(
                [benchmark_symbol], start_date=benchmark_start_date, end_date=run_date, fields=benchmark_fields
            )
            if benchmark_df is None or benchmark_df.empty:
                benchmark_df = DataSource("bar1d_CN_STOCK_A").read(
                    [benchmark_symbol], start_date=benchmark_start_date, end_date=run_date, fields=benchmark_fields
                )

            print(benchmark_df.tail())
            benchmark_len = len(benchmark_df)
            cum_benchmark = benchmark_df.iloc[benchmark_len - 1]["close"] / benchmark_df.iloc[0]["close"] - 1.0
            today_benchmark_return = benchmark_df.iloc[benchmark_len - 1]["close"] / benchmark_df.iloc[benchmark_len - 2]["close"] - 1.0
            json_cum_benchmark = {self._benchmark_symbol: today_benchmark_return, self._benchmark_symbol + ".CUM": cum_benchmark}
            json_cum_benchmark = json.dumps(json_cum_benchmark)

            # risk_indicator
            json_risk_indicator = self.paper_test_helper.get_risk_indicators(today_benchmark_return)
            # extension
            if not extension:
                json_extension = self.paper_test_helper.get_json_extension(result_last_row, self._order_price_field_buy, self._order_price_field_sell)
            else:
                if "order_price_field_buy" not in extension:
                    extension["order_price_field_buy"] = self._order_price_field_buy
                    extension["order_price_field_sell"] = self._order_price_field_sell
                    extension["is_stock"] = str(self.paper_test_helper.is_stock)
                    extension["need_settle"] = str(result_last_row["need_settle"].values[0])
                for k, v in extension.items():
                    if isinstance(v, np.int64):
                        extension[k] = int(np.int64)
                json_extension = json.dumps(algo.extension)
            log.info("result json_extension:{}".format(json_extension))

            print("processing to update_database...")
            update_database = os.environ.get("UPDATE_DATABASE", "True")
            # delete today daily record
            if record_datas.today_record and update_database != "False":
                StrategyClient.delete_strategy_daily_by_rundate(strategy_id=self._algo_id, run_date=self._run_date, credential=self._credential)
            grade = 0
            # 计算策略评分
            # 计算模拟实盘策略距离首次运行的天数
            time_delta_days = (
                datetime.strptime(self._run_date, "%Y-%m-%d").date() - datetime.strptime(self._first_trading_date, "%Y-%m-%d").date()
            ).days
            grade = (
                self.paper_test_helper.cum_return * 1000
                + self.paper_test_helper.current_sharpe * 3
                + 6 * self.paper_test_helper.annual_return / (1 + self.paper_test_helper.max_drawdown)
            )
            if time_delta_days <= 10:
                grade = int(grade * 0.0000000001)
            elif time_delta_days <= 20:
                grade = int(grade * 0.0000001)
            elif time_delta_days < 30:
                grade = int(grade * 0.0001)
            if math.isnan(grade):
                grade = 0
            if update_database != "False":
                if RUN_MODE == "STRATEGY_STRATEGY":
                    strategy_daily = CreateStrategyDailySchema(
                        performance_id=performance_id,
                        strategy_id=self._algo_id,
                        cash=float(portfolio_value - self.paper_test_helper.position_value),
                        positions=json_positions,
                        orders=json_orders,
                        transactions=json_transactions,
                        benchmark=json_cum_benchmark,
                        portfolio=json_portfolio,
                        risk_indicator=json_risk_indicator,
                        trading_days=self.paper_test_helper.new_trading_days,
                        extension=json_extension,  # extension 存储股票的买卖时间
                        logs=json_logs,
                        # is_sync=is_sync,
                        # grade=grade,
                        run_date=datetime.strptime(self._run_date, "%Y-%m-%d"),
                    )
                    StrategyClient.create_strategy_daily(params=strategy_daily, credential=self._credential)
            new_win_ratio, new_win_loss_count = (None, None)
            new_all_previous_records = copy.deepcopy(record_datas.all_previous_records)
            new_last_record = copy.deepcopy(record_datas.last_record)
            if new_all_previous_records and new_last_record and len(new_all_previous_records) > 0:
                new_last_record.strategy_id = self._algo_id
                new_last_record.cash = float(portfolio_value - self.paper_test_helper.position_value)
                new_last_record.positions = json_positions
                new_last_record.orders = json_orders
                new_last_record.transactions = json.loads(json_transactions)
                new_last_record.benchmark = json_cum_benchmark
                new_last_record.portfolio = json_portfolio
                new_last_record.risk_indicator = json_risk_indicator
                new_last_record.trading_days = self.paper_test_helper.new_trading_days
                new_last_record.extension = json_extension  # extension 存储股票的买卖时间
                new_last_record.logs = json_logs
                # new_last_record.is_sync = is_sync
                # new_last_record.grade = grade
                new_last_record.run_date = datetime.strptime(self._run_date, "%Y-%m-%d")
                new_all_previous_records.append(new_last_record)

            if performance and abs(performance.win_ratio)<1:
                new_win_ratio, new_win_loss_count = get_win_ratio(
                    new_all_previous_records, performance.run_date, performance.win_loss_count, performance.win_ratio
                )
            elif performance and record_datas.last_record:
                new_win_ratio, new_win_loss_count = get_win_ratio(
                    new_all_previous_records, record_datas.last_record, None, None
                )
            try:
                sharpe = json.loads(json_risk_indicator.replace("'", '"')).get("sharpe")
            except Exception as e:
                log.error("forward_test  json_risk_indicator Exception, e {}".format(e))
                sharpe = None
            # 更新策略表中的cum_return等数据
            # 近10日收益
            ten_days_return = -1
            ten_days_ago_portfolio_dict = get_ago_portfolio_dict("ten_days", self.paper_test_helper.record_datas)
            if ten_days_ago_portfolio_dict.get("cum_return") is not None:
                ten_days_return = (self.paper_test_helper.cum_return - ten_days_ago_portfolio_dict["cum_return"]) / (
                    1 + ten_days_ago_portfolio_dict["cum_return"]
                )

            # 近1周收益
            week_return = -1
            week_ago_portfolio_dict = get_ago_portfolio_dict("week", self.paper_test_helper.record_datas)
            if week_ago_portfolio_dict.get("cum_return") is not None:
                week_return = (self.paper_test_helper.cum_return - week_ago_portfolio_dict["cum_return"]) / (
                    1 + week_ago_portfolio_dict["cum_return"]
                )
            # 近1月收益
            month_return = -1
            month_ago_portfolio_dict = get_ago_portfolio_dict("month", self.paper_test_helper.record_datas)
            if month_ago_portfolio_dict.get("cum_return") is not None:
                month_return = (self.paper_test_helper.cum_return - month_ago_portfolio_dict["cum_return"]) / (
                    1 + month_ago_portfolio_dict["cum_return"]
                )
            # 近3月收益
            three_month_return = -1
            three_month_ago_portfolio_dict = get_ago_portfolio_dict("three_month", self.paper_test_helper.record_datas)
            if three_month_ago_portfolio_dict.get("cum_return") is not None:
                three_month_return = (self.paper_test_helper.cum_return - three_month_ago_portfolio_dict["cum_return"]) / (
                    1 + three_month_ago_portfolio_dict["cum_return"]
                )
            # 近6月收益
            six_month_return = -1
            six_month_ago_portfolio_dict = get_ago_portfolio_dict("six_month", self.paper_test_helper.record_datas)
            if six_month_ago_portfolio_dict.get("cum_return") is not None:
                six_month_return = (self.paper_test_helper.cum_return - six_month_ago_portfolio_dict["cum_return"]) / (
                    1 + six_month_ago_portfolio_dict["cum_return"]
                )
            # 近1年收益
            year_return = -1
            year_ago_portfolio_dict = get_ago_portfolio_dict("year", self.paper_test_helper.record_datas)
            if year_ago_portfolio_dict.get("cum_return") is not None:
                year_return = (self.paper_test_helper.cum_return - year_ago_portfolio_dict["cum_return"]) / (
                    1 + year_ago_portfolio_dict["cum_return"]
                )

            # 净值曲线-------------------------------------------------start----------------------------------
            cum_return_plot = []
            before_shared_cum_return_plot = []
            after_shared_cum_return_plot = []
            benchmark_cum_return_plot = []
            hold_percent_plot = []
            if math.isclose(self.paper_test_helper.current_pv, 0):
                today_hold_percent = 0
            else:
                # 算出持仓占比，保留4位小数, 前端会乘以100
                today_hold_percent = round(self.paper_test_helper.position_value / self.paper_test_helper.current_pv, 4)
            epoch_time = datetime(1970, 1, 1) + timedelta(hours=8)
            today_run_date_time = datetime.strptime(self._run_date, "%Y-%m-%d")
            today_plot_dt = (today_run_date_time - epoch_time).total_seconds() * 1000
            index_list = []
            before_timestamps = None
            try:
                before_timestamps = performance.cum_return_plot[-1][0]
                before_date_str = datetime.datetime.fromtimestamp(before_timestamps / 1000).date().strftime("%Y-%m-%d")
                # from bigdata.api.datareader import D
                # td_df = D.trading_days()
                td_df = DataSource("trading_days").read(query="country_code=='CN'")[["date"]]
                index_list = td_df[(td_df.date == self._run_date) | (td_df.date == before_date_str)].index.tolist()
            except Exception:
                pass
            if (
                performance
                and performance.cum_return_plot 
                and performance.hold_percent_plot
                and performance.benchmark_cum_return_plot
                and not record_datas.today_record
                and before_timestamps
                and before_timestamps < today_plot_dt
                and len(index_list) == 2
                and abs(index_list[0] - index_list[1]) == 1
            ):
                cum_return_plot = copy.deepcopy(performance.cum_return_plot)
                benchmark_cum_return_plot = copy.deepcopy(performance.benchmark_cum_return_plot)
                hold_percent_plot = copy.deepcopy(performance.hold_percent_plot)
                # 策略累计收益
                cum_return_plot.append([today_plot_dt, self.paper_test_helper.cum_return])
                # 基准指数收益
                benchmark_cum_return_plot.append([today_plot_dt, cum_benchmark])
                # 持仓比例曲线
                hold_percent_plot.append([today_plot_dt, today_hold_percent])
            else:
                # 首次计算净值曲线
                temp_cum_benchmark = 0.0
                for previous_record in record_datas.all_previous_records:
                    # 当天的累计收益
                    temp_portfolio = previous_record.portfolio
                    # 当天的运行日期
                    temp_run_date_time = datetime.combine(previous_record.run_date, datetime.min.time())
                    cum_return_plot.append([(temp_run_date_time - epoch_time).total_seconds() * 1000, temp_portfolio["cum_return"]])
                    # 基准累计收益
                    benchmark_symbol = self._benchmark_symbol
                    json_benchmark = previous_record.benchmark
                    # log.info("json_benchmark={}".format(json_benchmark))
                    try:
                        temp_cum_benchmark = (1 + json_benchmark[benchmark_symbol]) * (temp_cum_benchmark + 1) - 1
                        if json_benchmark.get(benchmark_symbol + ".CUM", None):
                            benchmark_cum_return_plot.append(
                                [(temp_run_date_time - epoch_time).total_seconds() * 1000, json_benchmark[benchmark_symbol + ".CUM"]]
                            )
                        else:
                            benchmark_cum_return_plot.append([(temp_run_date_time - epoch_time).total_seconds() * 1000, temp_cum_benchmark])
                    except KeyError:
                        benchmark_symbol = benchmark_symbol.replace("HIX", "SHA").replace("ZIX", "SZA")
                        temp_cum_benchmark = (1 + json_benchmark[benchmark_symbol]) * (temp_cum_benchmark + 1) - 1
                        if json_benchmark.get(benchmark_symbol + ".CUM", None):
                            benchmark_cum_return_plot.append(
                                [(temp_run_date_time - epoch_time).total_seconds() * 1000, json_benchmark[benchmark_symbol + ".CUM"]]
                            )
                        else:
                            benchmark_cum_return_plot.append([(temp_run_date_time - epoch_time).total_seconds() * 1000, temp_cum_benchmark])
                    # 当前总资产
                    temp_total_assets = previous_record.cash + temp_portfolio["pv"]
                    if math.isclose(temp_total_assets, 0):
                        hold_percent_plot.append([(temp_run_date_time - epoch_time).total_seconds() * 1000, 0])
                    else:
                        # 算出持仓占比，保留4位小数，前端会乘以100
                        hold_percent_plot.append(
                            [(temp_run_date_time - epoch_time).total_seconds() * 1000, round(temp_portfolio["pv"] / temp_total_assets, 4)]
                        )
                # 添加今日的曲线点
                # 策略累计收益
                cum_return_plot.append([today_plot_dt, self.paper_test_helper.cum_return])
                # 基准指数收益
                benchmark_cum_return_plot.append([today_plot_dt, cum_benchmark])
                # 持仓比例曲线
                hold_percent_plot.append([today_plot_dt, today_hold_percent])
                # 相对累计收益曲线
                relative_cum_return_plot = self._get_relative_cum_return_plot(
                    cum_return_plot=cum_return_plot, benchmark_cum_return_plot=benchmark_cum_return_plot
                )

            # if equity_algo.shared_date:
            #     shared_date_time = (datetime.combine(equity_algo.shared_date, datetime.min.time()) - epoch_time).total_seconds() * 1000
            #     for plot_point in cum_return_plot:
            #         if plot_point[0] <= shared_date_time:
            #             before_shared_cum_return_plot.append(plot_point)
            #         else:
            #             after_shared_cum_return_plot.append(plot_point)
            print(
                "cum_return_plot: {}, before_shared_cum_return_plot: {}, \
                after_shared_cum_return_plot: {}, benchmark_cum_return_plot: {}, hold_percent_plot: {}".format(
                    len(cum_return_plot),
                    len(before_shared_cum_return_plot),
                    len(after_shared_cum_return_plot),
                    len(benchmark_cum_return_plot),
                    len(hold_percent_plot),
                )
            )

            if update_database != "False":
                if RUN_MODE == "STRATEGY_STRATEGY":
                    performance = StrategyPerformanceSchema(
                        id=performance_id,
                        strategy_id=self._algo_id,
                        sharpe=float(sharpe),
                        run_date=self._run_date,
                        # 最大回撤
                        max_drawdown=float(self.paper_test_helper.max_drawdown),
                        # 累计收益
                        cum_return=float(self.paper_test_helper.cum_return),
                        # 年化收益
                        annual_return=float(self.paper_test_helper.annual_return),
                        # 今日收益
                        today_return=float(self.paper_test_helper.today_return),
                        ten_days_return=float(ten_days_return),
                        # 最新运行日期
                        week_return=float(week_return),
                        month_return=float(month_return),
                        three_month_return=float(three_month_return),
                        six_month_return=float(six_month_return),
                        year_return=float(year_return),
                        cum_return_plot=cum_return_plot,
                        benchmark_cum_return_plot=benchmark_cum_return_plot,
                        hold_percent_plot=hold_percent_plot,
                        relative_cum_return_plot=relative_cum_return_plot,
                        # before_shared_cum_return_plot=before_shared_cum_return_plot,
                        # after_shared_cum_return_plot=after_shared_cum_return_plot,
                        # grade=grade,
                        # win_ratio=new_win_ratio or -1.0,
                        win_ratio= new_win_ratio or -1,

                        win_loss_count=new_win_loss_count,
                    )
                    StrategyClient.create_or_update_strategy_performance(params=performance, credential=self._credential)
            # 净值曲线-------------------------------------------------end-----------------------------------------

            # 已经跑完一个实盘此时设置环境变量
            os.environ.setdefault("FIRST_TRADE_SUCCESS", "True")

            # send notify of orders
            # new_orders = self._append_price_info_to_orders(ohlc_data, new_orders)

            log.info("forward test result, new_cash {}, extension {}".format(new_cash, json_extension))

            # 20180630 add 'raw_perf' param to keep same behavior between backtest & forwardtest
            raw_perf = None
            try:
                raw_perf = DataSource.write_df(algo_result)
            except Exception:
                pass
            outputs = Outputs(
                algo_result=algo_result,
                new_orders=new_orders,
                raw_perf=raw_perf,
                positions=pd.DataFrame(new_positions),
                ohlc_data=ohlc_data,
                context_outputs=algo.outputs,
            )

            # 20180707 add some fake extend methods to outputs
            def read_raw_perf(self):
                return outputs.raw_perf.read() if outputs.raw_perf else None

            extend_class_methods(outputs, read_raw_perf=read_raw_perf)

            def pyfolio_full_tear_sheet(self):
                pass

            extend_class_methods(outputs, pyfolio_full_tear_sheet=pyfolio_full_tear_sheet)

            def risk_analyze(self):
                pass

            extend_class_methods(outputs, risk_analyze=risk_analyze)

            def factor_profit_analyze(self):
                pass

            extend_class_methods(outputs, factor_profit_analyze=factor_profit_analyze)

            return outputs
        except Exception as e:
            log.exception("run hit Exception, e {}".format(e))
            raise e

    def _render_template(self, template, data):
        for k, v in data.items():
            template = template.replace(k, str(v))
        return template

    def _append_price_info_to_orders(self, history_data_panel, orders):
        orders = pd.DataFrame(orders)
        if len(orders) > 0:
            # orders['created'] = orders.created.apply(lambda x: pd.to_datetime(x.date()))
            orders["created"] = orders.created.apply(lambda x: pd.to_datetime(self._run_date))
            orders["instrument"] = orders.sid.apply(lambda x: x.symbol)
            instrument_list = sorted(set(orders.instrument) & set(history_data_panel.keys()))
            data_df = pd.concat(
                [df.assign(instrument=instrument) for instrument, df in history_data_panel[instrument_list].iteritems()]
            ).reset_index()
            data_df["created"] = data_df.date.apply(lambda x: pd.to_datetime(x.date()))

            orders = orders.merge(data_df[["created", "instrument", "close", "adjust_factor"]], on=["created", "instrument"], how="left")
            orders["last_sale_price"] = orders["close"]

            log.info("_append_price_info_to_orders:\n{0}".format(orders))
        return orders

    def _get_relative_cum_return_plot(self, cum_return_plot, benchmark_cum_return_plot):
        try:
            if not cum_return_plot or not benchmark_cum_return_plot:
                return []
            df1 = pd.DataFrame(cum_return_plot, columns=["index", "algorithm_period_return"])
            df2 = pd.DataFrame(benchmark_cum_return_plot, columns=["index2", "benchmark_period_return"])
            results = df1.merge(df2[["benchmark_period_return"]], left_index=True, right_index=True, how="outer")
            """ 计算相对收益率 """
            results["a_delta"] = results["algorithm_period_return"].shift(1)
            results["a_delta"].iloc[0] = 0
            results["a_delta"] = (results["algorithm_period_return"] + 1.0) / (results["a_delta"] + 1.0) - 1.0
            results["b_delta"] = results["benchmark_period_return"].shift(1)
            results["b_delta"].iloc[0] = 0
            results["b_delta"] = (results["benchmark_period_return"] + 1.0) / (results["b_delta"] + 1.0) - 1.0
            results["r_delta"] = results["a_delta"] - results["b_delta"]
            var_set = {"last": 0.0}

            def cum_func(x):
                cur = (var_set["last"] + 1.0) * (1.0 + x) - 1.0
                var_set["last"] = cur
                return cur

            results["relative_ratio"] = results["r_delta"].map(cum_func)
            relative_cum_return_plot = results[["index", "relative_ratio"]].values.tolist()
            return relative_cum_return_plot
        except Exception:
            return []

    def _format_list(self, li):
        try:
            _li = li[0]
            res = [[float(i),j] for i,j in _li]
            return [res]
        except Exception:
            return li


if __name__ == "__main__":
    os.environ["JPY_USER"] = "bigquant"
    my_instruments = ["600519.SHA"]

    def initialize(context):
        pass

    def handle_data(context, data):
        for instrument in my_instruments:
            instrument = context.symbol(instrument)
            context.order_value(instrument, 10000)

    m = BigQuantModule(
        initialize=initialize,
        handle_data=handle_data,
        prepare=None,
        run_date="2017-08-04",
        first_trading_date="2017-08-03",
        algo_id=299,
        instruments=my_instruments,
        email_to=["rydeng@bigquant.com"],
    )
    m.run()
