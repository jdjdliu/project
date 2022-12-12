import os

import learning.module2.common.interface as I  # noqa
import pandas as pd
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
from learning.settings import site
from learning.toolimpl.userenv import get_user_env
from sdk.auth import Credential
from sdk.strategy.client import StrategyClient
from sdk.strategy.constants import StrategyBuildType
from sdk.strategy.schemas import UpdateBacktestParamter, UpdateBacktestParamterRequest

# 是否自动缓存结果，默认为True。一般对于需要很长计算时间的（超过1分钟），启用缓存(True)；否则禁用缓存(False)
bigquant_cacheable = False  # TODO: fix this
# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
bigquant_deprecated = None

bigquant_public = True


DEFAULT_PREPARE = """# 回测引擎：准备数据，只执行一次
def bigquant_run(context):
    pass
"""

DEFAULT_INITIALIZE = """# 回测引擎：初始化函数，只执行一次
def bigquant_run(context):
    # 加载预测数据
    context.ranker_prediction = context.options['data'].read_df()

    # 系统已经设置了默认的交易手续费和滑点，要修改手续费可使用如下函数
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    # 预测数据，通过options传入进来，使用 read_df 函数，加载到内存 (DataFrame)
    # 设置买入的股票数量，这里买入预测股票列表排名靠前的5只
    stock_count = 5
    # 每只的股票的权重，如下的权重分配会使得靠前的股票分配多一点的资金，[0.339160, 0.213986, 0.169580, ..]
    context.stock_weights = T.norm([1 / math.log(i + 2) for i in range(0, stock_count)])
    # 设置每只股票占用的最大资金比例
    context.max_cash_per_instrument = 0.2
    context.hold_days = 5
"""


DEFAULT_BEFORE_TRADING_START = """# 回测引擎：每个单位时间开始前调用一次，即每日开盘前调用一次。
def bigquant_run(context, data):
    pass
"""


DEFAULT_HANDLE_DATA = """# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    # 按日期过滤得到今日的预测数据
    ranker_prediction = context.ranker_prediction[
        context.ranker_prediction.date == data.current_dt.strftime('%Y-%m-%d')]

    # 1. 资金分配
    # 平均持仓时间是hold_days，每日都将买入股票，每日预期使用 1/hold_days 的资金
    # 实际操作中，会存在一定的买入误差，所以在前hold_days天，等量使用资金；之后，尽量使用剩余资金（这里设置最多用等量的1.5倍）
    is_staging = context.trading_day_index < context.hold_days # 是否在建仓期间（前 hold_days 天）
    cash_avg = context.portfolio.portfolio_value / context.hold_days
    cash_for_buy = min(context.portfolio.cash, (1 if is_staging else 1.5) * cash_avg)
    cash_for_sell = cash_avg - (context.portfolio.cash - cash_for_buy)
    positions = {e.symbol: p.amount * p.last_sale_price
                 for e, p in context.perf_tracker.position_tracker.positions.items()}

    # 2. 生成卖出订单：hold_days天之后才开始卖出；对持仓的股票，按StockRanker预测的排序末位淘汰
    if not is_staging and cash_for_sell > 0:
        equities = {e.symbol: e for e, p in context.perf_tracker.position_tracker.positions.items()}
        instruments = list(reversed(list(ranker_prediction.instrument[ranker_prediction.instrument.apply(
                lambda x: x in equities and not context.has_unfinished_sell_order(equities[x]))])))
        # print('rank order for sell %s' % instruments)
        for instrument in instruments:
            context.order_target(context.symbol(instrument), 0)
            cash_for_sell -= positions[instrument]
            if cash_for_sell <= 0:
                break

    # 3. 生成买入订单：按StockRanker预测的排序，买入前面的stock_count只股票
    buy_cash_weights = context.stock_weights
    buy_instruments = list(ranker_prediction.instrument[:len(buy_cash_weights)])
    max_cash_per_instrument = context.portfolio.portfolio_value * context.max_cash_per_instrument
    for i, instrument in enumerate(buy_instruments):
        cash = cash_for_buy * buy_cash_weights[i]
        if cash > max_cash_per_instrument - positions.get(instrument, 0):
            # 确保股票持仓量不会超过每次股票最大的占用资金量
            cash = max_cash_per_instrument - positions.get(instrument, 0)
        if cash > 0:
            context.order_value(context.symbol(instrument), cash)
"""


# 模块接口定义
bigquant_category = "回测与交易"
bigquant_friendly_name = "Trade (回测/模拟)"
bigquant_doc_url = "https://bigquant.com/docs/"

PRICE_TYPE_DICT = {
    "前复权": "pre_right",
    "真实价格": "real",
    "后复权": "post_right",
    "forward_adjusted": "pre_right",
    "original": "real",
    "backward_adjusted": "post_right",
}

PRODUCT_TYPE_DICT = {"股票": "stock", "期货": "future", "期权": "option", "数字货币": "dcc"}

order_price_fields = [
    "open",
    "close",
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


def read_holidays(market="CN", as_list=True):
    """读取holidays表"""
    holidays_df = DataSource("holidays_CN").read()
    if as_list:
        holidays = holidays_df.date.apply(lambda x: x.date()).tolist()
        return holidays
    else:
        return holidays_df


def live_mock_backtest_method(*args, **kwargs):
    pass


def bigquant_run(
    start_date: I.str("开始日期，设定值只在回测模式有效，在模拟实盘模式下为当前日期，示例：2017-06-01。一般不需要指定，使用 代码列表 里的开始日期") = "",
    end_date: I.str("结束日期，设定值只在回测模式有效，在模拟实盘模式下为当前日期，示例：2017-06-01。一般不需要指定，使用 代码列表 里的结束日期") = "",
    initialize: I.code(
        "初始化函数，[回调函数] 初始化函数，整个回测中只在最开始时调用一次，用于初始化一些账户状态信息和策略基本参数，context也可以理解为一个全局变量，在回测中存放当前账户信息和策略基本参数便于会话。",
        I.code_python,
        DEFAULT_INITIALIZE,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    handle_data: I.code(
        "主函数，[回调函数] 必须实现的函数，该函数每个单位时间会调用一次, 如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次,由于我们现在数据只有日K，所以是按天回调。在回测中，可以通过对象data获取单只股票或多只股票的时间窗口价格数据。如果算法中没有schedule_function函数，那么该函数为必选函数。一般策略的交易逻辑和订单生成体现在该函数中。",
        I.code_python,
        DEFAULT_HANDLE_DATA,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    instruments: I.port("代码列表，如果提供了 `prepare`_ 函数，可以在 `prepare`_ 中覆盖此参数提供的值", optional=True, specific_type_name="列表|DataSource") = None,
    prepare: I.code(
        "数据准备函数，[回调函数] 准备数据函数，运行过程中只调用一次，在 initialize 前调用，准备交易中需要用到数据。目前支持设置交易中用到的股票列表，设置到 context.instruments。[更多](https://bigquant.com/docs/develop/modules/trade/usage.html)",
        I.code_python,
        DEFAULT_PREPARE,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    before_trading_start: I.code(
        "盘前处理函数，[回调函数] 每个单位时间开始前调用一次，即每日开盘前调用一次，该函数是可选函数。你的算法可以在该函数中进行一些数据处理计算，比如确定当天有交易信号的股票池。",
        I.code_python,
        DEFAULT_BEFORE_TRADING_START,
        specific_type_name="函数",
        auto_complete_type="python",
    ) = None,
    volume_limit: I.float("成交率限制：执行下单时控制成交量参数，默认值2.5%，若设置为０时，不进行成交量检查", 0, 1) = 0.025,
    order_price_field_buy: I.choice("买入点：open=开盘买入，close=收盘买入", order_price_fields) = "open",
    order_price_field_sell: I.choice("卖出点：open=开盘卖出，close=收盘卖出", order_price_fields) = "close",
    capital_base: I.float("初始资金", min=0) = 1.0e6,
    auto_cancel_non_tradable_orders: I.bool("自动取消无法成交订单：是否自动取消因为停牌等原因不能成交的订单") = True,
    data_frequency: I.choice("回测数据频率：日线 (daily)，分钟线 (minute)", ["daily", "minute"]) = "daily",
    price_type: I.choice("回测价格类型：前复权(forward_adjusted)，真实价格(original)，后复权(backward_adjusted)", ["真实价格", "后复权"]) = "真实价格",
    product_type: I.choice("回测产品类型：股票(stock), 期货(future), 期权(option), 数字货币(dcc)", ["股票", "期货", "期权"]) = "股票",
    plot_charts: I.bool("显示回测结果图表") = True,
    backtest_only: I.bool(
        "只在回测模式下运行：默认情况下，Trade会在回测和实盘模拟模式下都运行。如果策略中有多个M.trade，在实盘模拟模式下，只能有一个设置为运行，其他的需要设置为 backtest_only=True，否则将会有未定义的行为错误"
    ) = False,
    options_data: I.port(
        "其他输入数据：回测中用到的其他数据，比如预测数据、训练模型等。如果设定，在回测中通过 context.options['data'] 使用", optional=True, specific_type_name="DataSource"
    ) = None,
    options: I.doc("用户自定义数据，在回调函数中要用到的变量，需要从这里传入，并通过 context.options 使用", specific_type_name="字典") = None,
    history_ds: I.port("回测历史数据", optional=True, specific_type_name="DataSource") = None,
    benchmark_ds: I.port("基准数据，不影响回测结果", optional=True, specific_type_name="DataSource") = None,
    benchmark: I.str("基准代码，不影响回测结果") = "000300.HIX",
    trading_calendar: I.port("交易日历", optional=True) = None,
    amount_integer=None,  # noqa
    m_meta_kwargs={},
) -> [I.port("回测详细数据", "raw_perf"),]:
    """
    量化交易引擎。支持回测和模拟实盘交易
    """

    RUN_MODE = os.getenv("RUN_MODE", None)
    credential = Credential.from_env()
    instruments = smart_list(instruments)
    price_type = PRICE_TYPE_DICT.get(price_type, price_type)
    product_type = PRODUCT_TYPE_DICT.get(product_type, product_type)

    # @20180703 前复权模式暂时按真实价格模式
    if price_type == "pre_right":
        print("!trade.v4 module price_type is", price_type)
        price_type = "real"

    if isinstance(instruments, dict):
        start_date = start_date or instruments["start_date"]
        end_date = end_date or instruments["end_date"]
        instruments = instruments["instruments"]

    if not start_date and history_ds is not None:
        if isinstance(history_ds, DataSource):
            df = history_ds.read()
        else:
            df = history_ds
        start_date = df["date"].min().strftime("%Y-%m-%d")
        end_date = df["date"].max().strftime("%Y-%m-%d")
        instruments = list(set(df["instrument"]))
        del df

    if instruments is None:
        instruments = options_data
    options = options or {}
    if options_data is not None:
        options["data"] = options_data

    history_data = history_ds
    if benchmark_ds is not None:
        benchmark_data = benchmark_ds
    elif benchmark:
        benchmark_data = benchmark
    else:
        benchmark_data = None

    treasury_data = None  # not used currently

    # @202002 生成交易日历
    if trading_calendar is None:
        from zipline.utils.calendars import calendar_utils

        holidays = read_holidays(as_list=True)
        trading_calendar = calendar_utils.gen_trading_calendar_with_holidays(instruments, holidays, data_frequency=data_frequency, use_cache=True)

    # 201901 for supporting SaaS papertrading on site citics
    env = get_user_env()

    def do_live_run(env):
        from learning.api import M

        strategy_id = os.getenv("STRATEGY_ID", "")
        assert strategy_id is not None

        lm_1 = M.forward_register.v2(
            algo_name=env.strategy_display_name,
            market_type=env.market_type,
            capital_base=capital_base,
            first_date=env.trading_date,
            description="",
            unique_id=strategy_id,
            benchmark_symbol=benchmark,
            price_type=price_type,
            product_type=product_type,
            data_frequency=data_frequency,
            **m_meta_kwargs
        )

        lm_3 = M.forward_test.v5(
            instruments=instruments,
            prepare=prepare,
            initialize=initialize,
            handle_data=handle_data,
            run_date=env.trading_date,
            first_trading_date=lm_1.first_date,
            algo_id=lm_1.algo_id,
            before_trading_start=before_trading_start,
            order_price_field_buy=order_price_field_buy,
            order_price_field_sell=order_price_field_sell,
            auto_cancel_non_tradable_orders=auto_cancel_non_tradable_orders,
            options=options,
            data_frequency=lm_1.data_frequency,
            price_type=lm_1.price_type,
            product_type=lm_1.product_type,
            volume_limit=volume_limit,
            history_data=history_data,
            benchmark_data=None,  # 20180810 currently pass-in none
            benchmark_symbol=lm_1.benchmark_symbol,
            treasury_data=None,
            trading_calendar=trading_calendar,  # @202002 also pass-in
            **m_meta_kwargs
        )
        with open("/var/tmp/.status", "w") as f:
            f.write("done")
        return lm_3

    if RUN_MODE == "STRATEGY_STRATEGY":
        env = get_user_env()
        return do_live_run(env)
    else:
        from learning.api import M

        m_backtest = M.backtest.v8(
            instruments=instruments,
            start_date=start_date or None,
            end_date=end_date or None,
            prepare=prepare,
            handle_data=handle_data,
            initialize=initialize,
            before_trading_start=before_trading_start,
            volume_limit=volume_limit,
            order_price_field_buy=order_price_field_buy,
            order_price_field_sell=order_price_field_sell,
            capital_base=capital_base,
            auto_cancel_non_tradable_orders=auto_cancel_non_tradable_orders,
            data_frequency=data_frequency,
            plot_charts=plot_charts,
            options=options,
            price_type=price_type,
            product_type=product_type,
            history_data=history_data,
            benchmark_data=benchmark_data,
            treasury_data=treasury_data,
            trading_calendar=trading_calendar,
            **m_meta_kwargs
        )
        if os.getenv("BUILD_TYPE") == StrategyBuildType.CODED:
            # update paramter
            backtest_id = os.getenv("BACKTEST_ID")
            parameter = UpdateBacktestParamter(capital_base=capital_base)
            query = UpdateBacktestParamterRequest(backtest_id=backtest_id, paramter=parameter)
            StrategyClient.update_params_to_backtest(query, credential=credential)
        with open("/var/tmp/.status", "w") as f:
            f.write("done")
        return m_backtest

    return None


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
