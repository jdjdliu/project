from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
from learning.module2.common.utils import smart_list
from learning.toolimpl.userenv import get_user_env

# from learning.settings import site


# 是否自动缓存结果，默认为True。一般对于需要很长计算时间的（超过1分钟），启用缓存(True)；否则禁用缓存(False)
bigquant_cacheable = False  # TODO: fix this
# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"
""" v1日后不维护 """

bigquant_public = True


DEFAULT_INITIALIZE = """# 交易引擎：初始化函数，只执行一次
def bigquant_run(context):
    # 加载预测数据
    pass
"""

DEFAULT_BEFORE_TRADING_START = """# 交易引擎：每个单位时间开盘前调用一次。
def bigquant_run(context, data):
    pass
"""

DEFAULT_HANDLE_TICK = """# 交易引擎：tick数据处理函数，每个tick执行一次
def bigquant_run(context, data):
    pass
"""

DEFAULT_HANDLE_DATA = """# 交易引擎：bar数据处理函数，每个时间单位执行一次
def bigquant_run(context, data):
    pass
"""

DEFAULT_HANDLE_TRADE = """# 交易引擎：成交回报处理函数，每个成交发生时执行一次
def bigquant_run(context, data):
    pass
"""

DEFAULT_HANDLE_ORDER = """# 交易引擎：委托回报处理函数，每个委托变化时执行一次
def bigquant_run(context, data):
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
    history_ds: I.port("回测历史数据", optional=True, specific_type_name="DataSource") = None,
    benchmark_ds: I.port("基准数据，不影响回测结果", optional=True, specific_type_name="DataSource") = None,
    capital_base: I.float("初始资金", min=0) = 1.0e6,
    frequency: I.choice("数据频率：日线 (daily)，分钟线 (minute)，快照（tick），逐笔（tick2）", ["daily", "minute", "tick", "tick2"]) = "minute",
    price_type: I.choice("价格类型：前复权(forward_adjusted)，真实价格(original)，后复权(backward_adjusted)", ["真实价格", "前复权", "后复权"]) = "真实价格",
    product_type: I.choice("产品类型：股票(stock), 期货(future), 期权(option)", ["股票", "期货", "期权"]) = "股票",
    before_start_days: I.str("历史数据向前取的天数，默认为0") = "0",
    benchmark: I.str("基准代码，不影响回测结果") = "000300.HIX",
    plot_charts: I.bool("显示回测结果图表") = True,
    disable_cache: I.bool("是否禁用回测缓存数据") = False,
    show_debug_info: I.bool("是否输出调试信息") = False,
    backtest_only: I.bool("只在回测模式下运行：默认情况下，Trade会在回测和实盘模拟模式下都运行。不需要模拟或实盘时为 backtest_only=True 即可") = False,
    options_data: I.port(
        "其他输入数据：回测中用到的其他数据，比如预测数据、训练模型等。如果设定，在回测中通过 context.options['data'] 使用", optional=True, specific_type_name="DataSource"
    ) = None,
    options: I.doc("用户自定义数据，在回调函数中要用到的变量，需要从这里传入，并通过 context.options 使用", specific_type_name="字典") = None,
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
    :param disable_cache: 是否禁用回测缓存历史数据
    :param options: 其他参数从这里传入，可以在 handle_data 等函数里使用
    """
    from learning.api import M
    from bigtrader.constant import Frequency, Product, AdjustType

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
        "post": AdjustType.POST,
    }
    PRODUCT_TYPE_DICT = {
        "股票": Product.EQUITY,
        "期货": Product.FUTURE,
        "期权": Product.OPTION,
        "stock": Product.EQUITY,
        "future": Product.FUTURE,
        "option": Product.OPTION,
    }

    price_type = PRICE_TYPE_DICT.get(price_type, price_type)
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

    daily_data_ds = history_ds if data_frequency == Frequency.DAILY else None
    minute_data_ds = history_ds if data_frequency == Frequency.MINUTE else None
    tick_data_ds = history_ds if data_frequency in [Frequency.TICK, Frequency.TICK2] else None

    result = None
    env = get_user_env()

    def do_backtest_run(env):
        """运行高频回测"""
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
            on_stop=after_trading,
            daily_data_ds=daily_data_ds,
            minute_data_ds=minute_data_ds,
            tick_data_ds=tick_data_ds,
            benchmark_data_ds=benchmark_ds,
            capital_base=capital_base,
            product_type=product_type,
            frequency=data_frequency,
            price_type=price_type,
            benchmark=benchmark,
            before_start_days=before_start_days,
            plot_charts=plot_charts,
            disable_cache=disable_cache,
            show_debug_info=show_debug_info,
            options=options,
            options_data=options_data,
            **m_meta_kwargs
        )
        return result

    def do_paper_run(env):
        """运行高频模拟交易"""
        # TODO:
        raise Exception("Not implement yet!")

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
            on_stop=after_trading,
            daily_data_ds=daily_data_ds,
            minute_data_ds=minute_data_ds,
            tick_data_ds=tick_data_ds,
            benchmark_data_ds=benchmark_ds,
            capital_base=capital_base,
            product_type=product_type,
            frequency=data_frequency,
            price_type=price_type,
            benchmark=benchmark,
            before_start_days=before_start_days,
            plot_charts=plot_charts,
            show_debug_info=show_debug_info,
            options=options,
            options_data=options_data,
            dbpath=None,
            **m_meta_kwargs
        )
        return result

    if backtest_only:
        print("HFTrade backtest_only mode!")

    if env.is_paper_trading() and not backtest_only:
        result = do_paper_run(env)
    elif env.is_live_trading() and not backtest_only:
        result = do_live_run(env)
    else:
        result = do_backtest_run(env)
    return result


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
