# from datetime import datetime, timedelta
from datetime import datetime
from pandas import DataFrame, Timestamp, Timedelta
from warnings import warn

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
from learning.module2.common.utils import smart_list
from learning.toolimpl.userenv import get_user_env
from learning.settings import site


# 是否自动缓存结果，默认为True。一般对于需要很长计算时间的（超过1分钟），启用缓存(True)；否则禁用缓存(False)
bigquant_cacheable = False  # TODO: fix this

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True


DEFAULT_INITIALIZE = """# 交易引擎：初始化函数，只执行一次
def bigquant_run(context):
    # 加载预测数据
    pass
"""

DEFAULT_BEFORE_TRADING_START = """# 交易引擎：每个单位时间开盘前调用一次。
def bigquant_run(context, data):
    # 盘前处理，订阅行情等
    pass
"""

DEFAULT_HANDLE_TICK = """# 交易引擎：tick数据处理函数，每个tick执行一次
def bigquant_run(context, tick):
    pass
"""

DEFAULT_HANDLE_DATA = """# 交易引擎：bar数据处理函数，每个时间单位执行一次
def bigquant_run(context, data):
    pass
"""

DEFAULT_HANDLE_TRADE = """# 交易引擎：成交回报处理函数，每个成交发生时执行一次
def bigquant_run(context, trade):
    pass
"""

DEFAULT_HANDLE_ORDER = """# 交易引擎：委托回报处理函数，每个委托变化时执行一次
def bigquant_run(context, order):
    pass
"""

DEFAULT_AFTER_TRADING = """# 交易引擎：盘后处理函数，每日盘后执行一次
def bigquant_run(context, data):
    pass
"""


# 模块接口定义
bigquant_category = "回测与交易"
bigquant_friendly_name = "HFTrade (高频 回测/模拟/实盘)"
bigquant_doc_url = "https://bigquant.com/wiki"

order_price_fields = ["open", "close"]


def bigquant_run(
    start_date: I.str("开始日期，设定值只在回测模式有效，在实盘模式下为当前日期，示例：2021-06-01。一般不需要指定，使用 代码列表 里的开始日期") = "",
    end_date: I.str("结束日期，设定值只在回测模式有效，在实盘模式下为当前日期，示例：2021-06-01。一般不需要指定，使用 代码列表 里的结束日期") = "",
    instruments: I.port("代码列表", optional=True, specific_type_name="列表|DataSource") = None,
    initialize: I.code(
        "初始化函数，[回调函数] 初始化函数，整个回测中只在最开始时调用一次，用于初始化一些账户状态信息和策略基本参数，context也可以理解为一个全局变量，在回测中存放当前账户信息和策略基本参数便于会话。",
        I.code_python,
        DEFAULT_INITIALIZE,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    before_trading_start: I.code(
        "盘前处理函数，[回调函数]  选择实现的函数，每个单位时间开始前调用一次，即每日开盘前调用一次。你的算法可以在该函数中进行一些数据处理计算，比如确定当天有交易信号的股票池。",
        I.code_python,
        DEFAULT_BEFORE_TRADING_START,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    handle_tick: I.code(
        "Tick处理函数，[回调函数] 选择实现的函数，该函数每个Tick会调用一次,。一般策略的交易逻辑和订单生成体现在该函数中。",
        I.code_python,
        DEFAULT_HANDLE_TICK,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    handle_data: I.code(
        "K线处理函数，[回调函数] 选择实现的函数，该函数每个单位时间会调用一次, 如果按分钟,则每分钟调用一次。在交易中，可以通过对象data获取单只股票或多只股票的时间窗口价格数据。一般策略的交易逻辑和订单生成体现在该函数中。",
        I.code_python,
        DEFAULT_HANDLE_DATA,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    handle_trade: I.code(
        "成交回报处理函数，[回调函数] 选择实现的函数，该函数在每个订单成交发生时会调用一次。", I.code_python, DEFAULT_HANDLE_TRADE, specific_type_name="函数", auto_complete_type="python"
    ) = None,
    handle_order: I.code(
        "委托回报处理函数，[回调函数] 选择实现的函数，该函数在每个订单状态变化时会调用一次。", I.code_python, DEFAULT_HANDLE_ORDER, specific_type_name="函数", auto_complete_type="python"
    ) = None,
    after_trading: I.code(
        "盘后处理函数，[回调函数] 选择实现的函数，在当日交易结束后调用一次。", I.code_python, DEFAULT_AFTER_TRADING, specific_type_name="函数", auto_complete_type="python"
    ) = None,
    options_data: I.port(
        "其他输入数据：回测中用到的其他数据，比如预测数据、训练模型等。如果设定，在回测中通过 context.options['data'] 使用", optional=True, specific_type_name="DataSource"
    ) = None,
    history_ds: I.port("回测历史数据", optional=True, specific_type_name="DataSource") = None,
    benchmark_ds: I.port("基准数据，不影响回测结果", optional=True, specific_type_name="DataSource") = None,
    capital_base: I.float("初始资金", min=0) = 1.0e6,
    frequency: I.choice("数据频率：日线 (daily)，分钟线 (minute)，快照（tick），逐笔（tick2）", ["daily", "minute", "tick", "tick2"]) = "minute",
    price_type: I.choice("价格类型：前复权(forward_adjusted)，真实价格(original)，后复权(backward_adjusted)", ["真实价格", "前复权", "后复权"]) = "真实价格",
    product_type: I.choice("产品类型：股票(stock), 期货(future), 期权(option), 股票(fund), 可转债(conbond), 自动(none)", ["股票", "期货", "期权", "基金", "可转债", "自动"]) = "股票",
    before_start_days: I.str("历史数据向前取的天数，默认为0") = "0",
    order_price_field_buy: I.choice("买入点：open=开盘买入，close=收盘买入", order_price_fields) = "open",
    order_price_field_sell: I.choice("卖出点：open=开盘卖出，close=收盘卖出", order_price_fields) = "close",
    benchmark: I.str("基准代码，不影响回测结果") = "000300.HIX",
    plot_charts: I.bool("显示回测结果图表") = True,
    disable_cache: I.bool("是否禁用回测缓存数据") = False,
    replay_bdb: I.bool("是否快速回放行情模式") = False,
    show_debug_info: I.bool("是否输出调试信息") = False,
    backtest_only: I.bool("只在回测模式下运行：默认情况下，Trade会在回测和实盘模拟模式下都运行。不需要模拟或实盘时为 backtest_only=True 即可") = False,
    m_meta_kwargs={},
) -> [I.port("回测详细数据", "raw_perf"),]:
    """
    高频交易模块
    :param start_date: 开始日期，在实盘的时候，这个日期会被重写为交易日的日期
    :param end_date: 结束日期，在实盘的时候，这个日期会被重写为交易日的日期
    :param instruments: 代码列表，可选，也可以在通过在 initialize 里通过 context.instruments 设置
    :param initialize: 初始化函数，initialize(context)
    :param before_trading_start: 在每个交易日开始前的处理函数，before_trading_start(context, data: BarDatas)
    :param handle_tick:  每个Tick快照行情处理函数， handle_tick(context, tick: TickData)
    :param handle_data:  每个Bar行情的处理函数， handle_data(context, data: BarDatas)
    :param handle_order: 每笔委托回报的处理函数， handle_order(context, data: OrderData)
    :param handle_trade: 每笔成交回报的处理函数， handle_trade(context, data: TradeData)
    :param after_trading:  每天交易盘后处理函数， after_trading(context, data: BarDatas)
    :param capital_base: 回测初始资金，默认为 1000000
    :param frequency: 数据频率，默认为 minute，也可以是 daily, tick 等
    :param price_type: 复权类型 adjust_type，如 真实价格[real/none]，前复权[pre]，后复权[post]
    :param product_type: 产品类型，如 stock/future/option等，一般不用指定或为Product.NONE，系统自动根据合约代码判断产品类型
    :param benchmark: 基准指数，可以用股票代码
    :param before_start_days: 历史数据向前取的天数，默认为0
    :param plot_charts: 是否输出绩效图表
    :param replay_bdb: 是否快速回放行情模式
    :param disable_cache: 是否禁用回测缓存历史数据
    :param options: 其他参数从这里传入，可以在 handle_data 等函数里使用
    """
    from learning.api import M
    from bigtrader.constant import Frequency, Product, AdjustType, SymbolExchangeMode

    # TODO: where is bigtrader.constant
    try:
        before_start_days = int(before_start_days)
    except ValueError:
        before_start_days = 0

    PRICE_TYPE_DICT = {
        "前复权": AdjustType.PRE,
        "真实价格": AdjustType.NONE,
        "后复权": AdjustType.POST,
        "forward_adjusted": AdjustType.PRE,
        "original": AdjustType.NONE,
        "backward_adjusted": AdjustType.POST,
        "pre": AdjustType.PRE,
        "none": AdjustType.NONE,
        "real": AdjustType.NONE,
        "post": AdjustType.POST,
    }
    PRODUCT_TYPE_DICT = {
        "股票": Product.EQUITY,
        "期货": Product.FUTURE,
        "期权": Product.OPTION,
        "可转债": Product.CON_BOND,
        "债券": Product.BOND,
        "基金": Product.FUND,
        "场内基金": Product.FUND,
        "场外基金": Product.MUT_FUND,
        "fund": Product.FUND,
        "mutfund": Product.MUT_FUND,
        "stock": Product.EQUITY,
        "future": Product.FUTURE,
        "option": Product.OPTION,
        "conbond": Product.CON_BOND,
        "none": Product.NONE,
        "自动": Product.NONE,
    }

    adjust_type = PRICE_TYPE_DICT.get(price_type, price_type)
    product_type = PRODUCT_TYPE_DICT.get(product_type, product_type)
    instruments = smart_list(instruments)
    if isinstance(instruments, dict):
        start_date = start_date or instruments["start_date"]
        end_date = end_date or instruments["end_date"]
        instruments = instruments["instruments"]

    data_frequency = frequency
    if data_frequency == "daily":
        data_frequency = Frequency.DAILY
    elif data_frequency == "minute":
        data_frequency = Frequency.MINUTE

    # FIXME: for using new siteconf
    env = get_user_env()
    is_site_citics = site == "citics"
    exchange_mode = SymbolExchangeMode.CITICS if is_site_citics else SymbolExchangeMode.BQ

    daily_data_ds = history_ds if data_frequency == Frequency.DAILY else None
    minute_data_ds = history_ds if data_frequency == Frequency.MINUTE else None
    tick_data_ds = history_ds if data_frequency in [Frequency.TICK, Frequency.TICK2] else None

    def cached_read_history_data(instruments, start_date, end_date, data_frequency, price_type):
        from zxtrader.trade_module.history_data import read_history_data_df2

        df = read_history_data_df2(instruments, start_date, end_date, data_frequency=data_frequency, price_type=adjust_type)
        if df is None or df.empty:
            return Outputs(data=None)

        dt_key = "datetime" if "datetime" in df.columns else "date"
        if data_frequency == "bar1m" and "trading_day" not in df.columns and dt_key in df.columns:
            df["trading_day"] = df[dt_key].apply(lambda x: x.year * 10000 + x.month * 100 + x.day)

        return Outputs(data=DataSource.write_df(df))

    if is_site_citics and not env.is_paper_trading() and not env.is_live_trading():
        from zxtrader.trade_module.history_data import read_history_data_df2

        data_start_date = (Timestamp(start_date) - Timedelta(weeks=52)).strftime("%Y-%m-%d")
        if daily_data_ds is None:
            kwargs = {
                "instruments": instruments,
                "start_date": data_start_date,
                "end_date": end_date,
                "data_frequency": "bar1d",
                "price_type": adjust_type,
            }
            if disable_cache:
                daily_data_ds = cached_read_history_data(**kwargs).data
            else:
                daily_data_ds = M.cached.v2(run=cached_read_history_data, kwargs=kwargs).data
            if daily_data_ds is None:
                raise Exception("Read daily history data empty!")
            if show_debug_info:
                print("read daily_data_ds={}".format(daily_data_ds))

        if minute_data_ds is None and data_frequency.endswith("m"):
            kwargs = {
                "instruments": instruments,
                "start_date": start_date,
                "end_date": end_date,
                "data_frequency": "bar1m",
                "price_type": adjust_type,
            }
            if disable_cache:
                minute_data_ds = cached_read_history_data(**kwargs).data
            else:
                minute_data_ds = M.cached.v2(run=cached_read_history_data, kwargs=kwargs).data
            if minute_data_ds is None:
                raise Exception("Read minute history data empty!")
            if show_debug_info:
                print("read minute_data_ds={}".format(minute_data_ds))

        if benchmark_ds is None:
            kwargs = {
                "instruments": [benchmark or "000300.SH"],
                "start_date": data_start_date,
                "end_date": end_date,
                "data_frequency": "bar1d",
                "price_type": adjust_type,
            }
            if disable_cache:
                benchmark_ds = cached_read_history_data(**kwargs).data
            else:
                benchmark_ds = M.cached.v2(run=cached_read_history_data, kwargs=kwargs).data
            if benchmark_ds is None:
                raise Exception("Read benchmark history data empty!")
            if show_debug_info:
                print("read benchmark_ds={}".format(benchmark_ds))

    result = None

    def do_backtest_run(env):
        """运行高频回测"""
        # print("====run hfbacktest now m_meta_kwargs={}".format(m_meta_kwargs))
        result = M.hfbacktest.v1(
            start_date=start_date,
            end_date=end_date,
            instruments=instruments,
            initialize=initialize,
            before_trading_start=before_trading_start,
            handle_tick=handle_tick,
            handle_data=handle_data,
            handle_trade=handle_trade,
            handle_order=handle_order,
            after_trading=after_trading,
            daily_data_ds=daily_data_ds,
            minute_data_ds=minute_data_ds,
            tick_data_ds=tick_data_ds,
            benchmark_data_ds=benchmark_ds,
            capital_base=capital_base,
            product_type=product_type,
            frequency=data_frequency,
            adjust_type=adjust_type,
            benchmark=benchmark,
            before_start_days=before_start_days,
            order_price_field_buy=order_price_field_buy,
            order_price_field_sell=order_price_field_sell,
            plot_charts=plot_charts,
            disable_cache=disable_cache,
            show_debug_info=show_debug_info,
            options_data=options_data,
            replay_bdb=replay_bdb,
            exchange_mode=exchange_mode,
            **m_meta_kwargs
        )
        return result

    def do_paper_run(env):
        """运行模拟交易"""
        from learning.api import M

        lm_1 = M.forward_register.v2(
            algo_name=env.strategy_display_name,
            market_type=env.market_type,
            capital_base=capital_base,
            first_date=env.trading_date,
            description="",
            unique_id=env.paper_trading_account,
            benchmark_symbol=benchmark,
            price_type=price_type,
            product_type=product_type,
            data_frequency=data_frequency,
            **m_meta_kwargs
        )
        print("do_paper_run lm_1={}, options={}".format(lm_1, options_data))

        lm = M.hfpapertrading.v1(
            instruments=instruments,
            prepare=None,
            initialize=initialize,
            before_trading_start=before_trading_start,
            handle_tick=handle_tick,
            handle_data=handle_data,
            handle_trade=handle_trade,
            handle_order=handle_order,
            after_trading=after_trading,
            run_date=env.trading_date,
            first_trading_date=lm_1.first_date,
            algo_id=lm_1.algo_id,
            order_price_field_buy=order_price_field_buy,
            order_price_field_sell=order_price_field_sell,
            options_data=options_data,
            email_to=env.notify_email,
            wechat_to=env.notify_wechat,
            frequency=lm_1.data_frequency,
            adjust_type=lm_1.price_type,
            product_type=lm_1.product_type,
            history_data=None,
            benchmark_data=None,
            benchmark_symbol=lm_1.benchmark_symbol,
            trading_calendar=None,
            replay_bdb=replay_bdb,
            **m_meta_kwargs
        )
        return lm

    def do_live_run(env):
        """运行高频实盘交易"""
        result = M.hflivetrading.v1(
            start_date=start_date,
            end_date=end_date,
            instruments=instruments,
            initialize=initialize,
            before_trading_start=before_trading_start,
            handle_tick=handle_tick,
            handle_data=handle_data,
            handle_trade=handle_trade,
            handle_order=handle_order,
            after_trading=after_trading,
            daily_data_ds=daily_data_ds,
            minute_data_ds=minute_data_ds,
            tick_data_ds=tick_data_ds,
            benchmark_data_ds=benchmark_ds,
            capital_base=capital_base,
            product_type=product_type,
            frequency=data_frequency,
            adjust_type=adjust_type,
            benchmark=benchmark,
            before_start_days=before_start_days,
            plot_charts=plot_charts,
            show_debug_info=show_debug_info,
            options_data=options_data,
            dbpath=None,
            exchange_mode=exchange_mode,
            **m_meta_kwargs
        )
        return result

    if backtest_only:
        print("HFTrade backtest_only mode!")

    # print("is_paper_trading=", env.is_paper_trading(), ", is_live_trading=", env.is_live_trading())
    if env.is_paper_trading() and is_site_citics and data_frequency == Frequency.DAILY:
        from zxtrader.trade_module.trade_module import trademodule_run

        algo_result = trademodule_run(
            start_date,
            end_date,
            instruments,
            env.strategy_display_name,
            capital_base=capital_base,
            data_frequency=data_frequency,
            price_type=price_type,
            product_type=product_type,
            volume_limit=0.025,
            order_price_field_buy=order_price_field_buy,
            order_price_field_sell=order_price_field_sell,
            show_debug_info=True,  # default True when papertrading
            standard_mode=True,  # default True from SaaS
            initialize=initialize,
            before_trading_start=before_trading_start,
            handle_data=handle_data,
            handle_order=handle_order,
            handle_trade=handle_trade,
            history_data=None,
            benchmark_data=None,
            trading_calendar=None,
            engine_type="bt",
            exchange_mode=exchange_mode,
            options={"data": options_data},
        )
        if isinstance(algo_result, DataFrame):
            raw_perf = DataSource.write_df(algo_result)
        else:
            raw_perf = DataSource.write_pickle(algo_result)
        outputs = Outputs(raw_perf=raw_perf, start_date=start_date, end_date=end_date)
        return outputs

    if env.is_paper_trading():
        if is_site_citics and data_frequency in ["1m", "minute"]:
            result = do_live_run(env)
        else:
            result = do_paper_run(env) if not backtest_only else None
    elif env.is_live_trading():
        result = do_live_run(env) if not backtest_only else None
    else:
        run_begin_time = datetime.now()
        result = do_backtest_run(env)

        if is_site_citics:
            from zxtrader.trade_module.trade_module import trade_module_push_result

            trade_module_push_result(result, run_begin_time)

    return result


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
