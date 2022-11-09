import os
from collections import OrderedDict
from datetime import datetime

import learning.api.tools as T
import numpy as np
import pandas as pd
from bigcharts.datatale import plot as plot_datatable
from bigcharts.tabs import plot as plot_tabs
from bigtrader.constant import offset_desc_dict
from jinja2 import Template

stats_template = """
<div class='kpicontainer'>
    <ul class='kpi'>
        <li><span class='title'>收益率</span><span class='value'>{{ stats.return_ratio }}%</span></li>
        <li><span class='title'>年化收益率</span><span class='value'>{{ stats.annual_return_ratio }}%</span></li>
        <li><span class='title'>基准收益率</span><span class='value'>{{ stats.benchmark_ratio }}%</span></li>
        <li><span class='title'>阿尔法</span><span class='value'>{{ stats.alpha }}</span></li>
        <li><span class='title'>贝塔</span><span class='value'>{{ stats.beta }}</span></li>
        <li><span class='title'>夏普比率</span><span class='value'>{{ stats.sharp_ratio }}</span></li>
        <li><span class='title'>胜率</span><span class='value'>{{ stats.win_ratio }}</span></li>
        <li><span class='title'>盈亏比</span><span class='value'>{{ stats.profit_loss_ratio }}</span></li>
        <li><span class='title'>收益波动率</span><span class='value'>{{ stats.return_volatility }}%</span></li>
        <li><span class='title'>信息比率</span><span class='value'>{{ stats.ir }}</span></li>
        <li><span class='title'>最大回撤</span><span class='value'>{{ stats.max_drawdown }}%</span></li>
    </ul>
</div>
{{ plot_data }}
"""

# mainly amount round number
perf_round_num = 3
perf_frequency = "daily"
risk_free = 0.0001173


def render(
    outputs,
    buy_moment,
    sell_moment,
    output="display",
    round_num=3,
    data_frequency="daily",
    plot_series_compare=None,
    return_script=None,
    acct_type=None,
):

    global perf_round_num
    perf_round_num = round_num
    global perf_frequency
    perf_frequency = data_frequency

    show_debug_info = outputs.show_debug_info
    if show_debug_info:
        print("{} perf_render raw_perf={}, benchmark_data={}, process stats...".format(datetime.now(), outputs.raw_perf, outputs.benchmark_data))

    stats = get_stats(outputs, acct_type=acct_type)
    general_html = (
        stats
        if output == "object"
        else Template(stats_template).render(
            stats=stats, plot_data=plot_backtest_result(outputs, plot_series_compare=plot_series_compare, acct_type=acct_type)
        )
    )

    transactions_stk, has_whole_data_transactions_stk = get_stk_transactions(
        outputs.read_one_raw_perf(acct_type[0]), buy_moment, sell_moment, data_frequency
    )
    transactions_subacct = get_future_transactions(outputs.read_one_raw_perf(acct_type[1]), buy_moment, sell_moment, data_frequency)

    transactions_html_stk = (
        transactions_stk if output == "object" else plot_datatable(transactions_stk, output="script", has_whole_data=has_whole_data_transactions_stk)
    )

    transactions_html_subacct = transactions_subacct if output == "object" else plot_datatable(transactions_subacct, output="script")

    odict_all = OrderedDict(
        [
            ("收益概况", general_html),
        ]
    )

    odict_market = OrderedDict(
        [
            ("股票交易详情", transactions_html_stk),
            ("期货交易详情", transactions_html_subacct),
        ]
    )

    if show_debug_info:
        print("{} perf_render process plot...".format(datetime.now()))

    if output == "object":
        return odict_all, odict_market
    if return_script:
        return plot_tabs(odict_all, output="script"), plot_tabs(odict_market, output="script")
    plot_tabs(odict_all, title="HFTrade(回测/模拟)")
    plot_tabs(odict_market)


def get_stats(outputs, acct_type=None):
    from empyrical import alpha_beta_aligned, annual_volatility, max_drawdown, sharpe_ratio
    from empyrical.utils import nanmean, nanstd

    # @202106 fix reference information_ratio because new empyrical removed it
    try:
        from empyrical import information_ratio
    except ImportError:
        from empyrical.utils import nanmean, nanstd

        def _adjust_returns(returns, adjustment_factor):
            if isinstance(adjustment_factor, (float, int)) and adjustment_factor == 0:
                return returns
            return returns - adjustment_factor

        def information_ratio(returns, factor_returns):
            if len(returns) < 2:
                return np.nan

            active_return = _adjust_returns(returns, factor_returns)
            tracking_error = nanstd(active_return, ddof=1)
            if np.isnan(tracking_error):
                return 0.0
            if tracking_error == 0:
                return np.nan
            return nanmean(active_return) / tracking_error

    results_stk = outputs.read_one_raw_perf(acct_type[0]).copy()
    results_subacct = outputs.read_one_raw_perf(acct_type[1]).copy()

    if outputs.benchmark_data:
        benchmark_data = outputs.benchmark_data.read().copy()
        results_stk = calculate_benchmark_returns(results_stk, benchmark_data)
        results_subacct = calculate_benchmark_returns(results_subacct, benchmark_data)
    else:
        results_stk["benchmark_return"] = 0.0
        results_stk["benchmark_period_return"] = 0.0
        results_subacct["benchmark_return"] = 0.0
        results_subacct["benchmark_period_return"] = 0.0

    # return_value = results["algorithm_period_return"].tail(1).values[0]
    return_value = (results_stk.portfolio_value.iloc[-1] + results_subacct.portfolio_value.iloc[-1]) / (
        results_stk.portfolio_value.iloc[0] + results_subacct.portfolio_value.iloc[0]
    ) - 1

    return_ratio = round(return_value * 100, 2)

    num_trading_days = results_stk["trading_days"].tail(1).values[0]

    if num_trading_days > 30:
        annual_return_ratio = round((pow(1 + return_value, 242.0 / num_trading_days) - 1) * 100, 2)
    else:
        annual_return_ratio = "nan"

    benchmark_ratio = round(results_stk["benchmark_period_return"].tail(1).values[0] * 100, 2)

    results_stk["weight"] = (
        (results_stk["portfolio_value"] / (results_stk["portfolio_value"] + results_subacct["portfolio_value"])).shift(1).fillna(0)
    )
    results_subacct["weight"] = (
        (results_subacct["portfolio_value"] / (results_stk["portfolio_value"] + results_subacct["portfolio_value"])).shift(1).fillna(0)
    )
    weighted_returns = results_stk["returns"] * results_stk["weight"] + results_subacct["returns"] * results_subacct["weight"]

    alpha, beta = alpha_beta_aligned(weighted_returns.values, results_stk["benchmark_return"].values, risk_free=risk_free)
    alpha, beta = round(alpha, 2), round(beta, 2)

    _sharpe_ratio = round(sharpe_ratio(weighted_returns.values, risk_free=risk_free), 2)
    _sharpe_ratio = "n/a" if _sharpe_ratio < -1.0e6 else _sharpe_ratio

    results_stk["trade_times_no_open"] = results_stk["trade_times"]
    results_subacct["trade_times_no_open"] = results_subacct["trade_times"]
    results_stk.loc[(results_stk["profit_count"] == 0) & (results_stk["loss_count"] == 0), "trade_times_no_open"] = 0
    results_subacct.loc[(results_subacct["profit_count"] == 0) & (results_subacct["loss_count"] == 0), "trade_times_no_open"] = 0
    winning_ratio = (results_stk.profit_count.sum() + results_subacct.profit_count.sum()) / (
        results_stk.trade_times_no_open.sum() + results_subacct.trade_times_no_open.sum()
    )

    ir = round(information_ratio(weighted_returns.values, results_stk["benchmark_return"].values), 2)

    return_volatility = round(annual_volatility(weighted_returns.values) * 100, 2)

    _max_drawdown = round(abs(max_drawdown(weighted_returns.values) * 100), 2)

    profit_loss_ratio = get_pnl_ratio(results_stk, results_subacct)

    stats = {
        "return_ratio": return_ratio,
        "annual_return_ratio": annual_return_ratio,
        "benchmark_ratio": benchmark_ratio,
        "beta": beta,
        "alpha": alpha,
        "sharp_ratio": _sharpe_ratio,
        "ir": ir,
        "return_volatility": return_volatility,
        "max_drawdown": _max_drawdown,
        "win_ratio": round(winning_ratio, 2),
        "profit_loss_ratio": abs(round(profit_loss_ratio, 2)),
    }
    return stats


relative_cum_last = 0.0


def plot_backtest_result(outputs, plot_series_compare=None, acct_type=None):
    from empyrical import cum_returns

    results_stk = outputs.read_one_raw_perf(acct_type[0]).copy()
    results_subacct = outputs.read_one_raw_perf(acct_type[1]).copy()

    if outputs.benchmark_data:
        benchmark_data = outputs.benchmark_data.read().copy()
        results_stk = calculate_benchmark_returns(results_stk, benchmark_data)
        results_subacct = calculate_benchmark_returns(results_subacct, benchmark_data)
    else:
        results_stk["benchmark_return"] = 0.0
        results_stk["benchmark_period_return"] = 0.0
        results_subacct["benchmark_return"] = 0.0
        results_subacct["benchmark_period_return"] = 0.0

    # get recorded columns
    ##TODO add extra risk factors columns
    system_columns = {
        "LOG",
        "POS_FAC",
        "TRA_FAC",
        "algorithm_period_return",
        "benchmark_return",
        "benchmark_period_return",
        "cancel_times",
        "capital_changed",
        "capital_used",
        "commission",
        "date",
        "ending_cash",
        "ending_value",
        "gross_leverage",
        "long_margin",
        "longs_count",
        "loss_count",
        "max_leverage",
        "net_leverage",
        "orders",
        "pnl_ratio",
        "portfolio_value",
        "positions",
        "positions_pnl",
        "profit_count",
        "realized_pnl",
        "returns",
        "short_margin",
        "shorts_count",
        "starting_cash",
        "starting_value",
        "today_buy_balance",
        "today_sell_balance",
        "trade_times",
        "trading_days",
        "transactions",
        "win_percent",
    }
    results_stk["weight"] = (
        (results_stk["portfolio_value"] / (results_stk["portfolio_value"] + results_subacct["portfolio_value"])).shift(1).fillna(0)
    )
    results_subacct["weight"] = (
        (results_subacct["portfolio_value"] / (results_stk["portfolio_value"] + results_subacct["portfolio_value"])).shift(1).fillna(0)
    )
    weighted_returns = results_stk["returns"] * results_stk["weight"] + results_subacct["returns"] * results_subacct["weight"]

    results = pd.DataFrame()
    results["algorithm_period_return"] = cum_returns(weighted_returns)
    results["benchmark_period_return"] = results_stk["benchmark_period_return"]
    results["portfolio_ratio"] = (results["algorithm_period_return"]) * 100
    results["benchmark_ratio"] = (results["benchmark_period_return"]) * 100

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

    results["relative_period_return"] = results["r_delta"].map(cum_func)
    results["relative_ratio"] = results["relative_period_return"] * 100

    # results["gross_leverage_percent"] = results["gross_leverage"] * 100
    columns = ["portfolio_ratio", "benchmark_ratio", "relative_ratio"]
    results = results[columns]
    # dt = results.index.to_datetime()
    # results.index = dt  # - 3600 * 1000000000 * dt.hour

    options = {
        "legend": {
            "enabled": True,
            "symbolHeight": 8,
            "symbolWidth": 8,
            "symbolPadding": 8,
            "symbolRadius": 0,
        },
        "plotOptions": {
            "series": {
                "showInNavigator": True,
            },
            "area": {
                "showInNavigator": True,
            },
        },
        "rangeSelector": {
            "enabled": True,
        },
        "navigator": {
            "enabled": True,
        },
        "series": [
            {
                "name": "策略收益率",
                "color": "#ff0000",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
                "type": "area",
                "fillColor": {
                    "linearGradient": [0, 0, 0, 1],
                    "stops": [
                        [0, "rgba(238,149,148,0.5)"],  # equals `Highstock.Color('#EE9594').setOpacity(0.5).get('rgba')`
                        [1, "rgba(238,149,148,0.5)"],
                    ],
                },
                # 'zoneAxis': 'x', # 最大回撤区间
                # 'zones': [
                #     { 'value': maxDrawdownStamp[0][0], color: '#FF0000' },
                #     { 'value': maxDrawdownStamp[1][0], color: '#279CB6' },
                #     { 'color': '#FF0000' },
                # ],
            },
            {
                "name": "基准收益率",
                "color": "#2776B6",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
            },
            {
                "name": "相对收益率",
                "color": "#8f4e4f",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
                "visible": False,
            },
            # {
            #     "name": "持仓占比",
            #     "color": "#66ff99",
            #     "tooltip": {
            #         "valueDecimals": 2,
            #         "valueSuffix": " %",
            #     },
            # },
        ],
        "yAxis": [
            {
                "title": {
                    "text": "净值曲线",
                },
                "labels": {"format": "{value}%"},
                "gridLineColor": "#cfcfcf",
                "gridLineWidth": 1,
            },
            # {
            #     "title": {
            #         "text": "持仓占比",
            #     },
            #     "labels": {"format": "{value}%"},
            #     "gridLineColor": "#cfcfcf",
            #     "gridLineWidth": 1,
            # },
        ],
        "chart": {
            # 'height': 500,
        },
    }

    # if len(record_columns) > 0:
    #     options["yAxis"].append(
    #         {
    #             "title": {
    #                 "text": "自定义数据",
    #             },
    #             "gridLineColor": "#cfcfcf",
    #             "gridLineWidth": 1,
    #         }
    #     )

    # # hide user data pane when there's no data
    # if not record_columns:
    #     options["yAxis"] = options["yAxis"][:2]

    if plot_series_compare:
        options["plotOptions"] = {
            "series": {"compare": plot_series_compare, "showInNavigator": True},
            "area": {
                "showInNavigator": True,
            },
        }
    return T.plot(
        results[columns],
        panes=[["portfolio_ratio", "benchmark_ratio", "relative_ratio", "100%"]],
        options=options,
        output="script",
    )


def get_stk_transactions(results, buy_moment, sell_moment, data_frequency="daily"):

    # results = outputs.read_one_raw_perf(acct_type)
    # results_stk = outputs.read_one_raw_perf(acct_type[0])
    # results_subacct = outputs.read_one_raw_perf(acct_type[1])

    if data_frequency in ["tick", "tick2"]:
        time_format = "%H:%M:%S.%f"
    else:
        time_format = "%H:%M:%S"
    row_count_limit = 10000
    has_whole_data = True

    # is_daily = 1 if data_frequency == 'daily' else 0

    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        if row_count_limit <= 0 and os.environ.get("displayAllData", "False") != "True":
            has_whole_data = False
            break

        transactions = row["transactions"]
        if len(transactions) <= 0:
            continue

        #         commission_map = {}
        #         for od in row["orders"]:
        #             commission_map[od["id"]] = od["commission"]

        period_open = row["date"]
        # tra_factors = row["TRA_FAC"]

        transaction_num = 0
        total_buy = 0.0
        total_sell = 0.0
        output_trs = []

        for t in transactions:
            symbol = t.get("symbol") or t["sid"].symbol
            # amount = abs(round(t["amount"] * tra_factors[symbol], 2))
            amount = abs(round(t["amount"], perf_round_num))
            # price = round(t["price"] / tra_factors[symbol], 2)
            price = round(t["price"] + 1e-6, 3)
            total = round(t["amount"] * t["price"], 2)
            commission = t["commission"]
            #             if t["order_id"]:
            #                 commission = round(commission_map[t["order_id"]], 2)
            #             else:
            #                 commission = 0.0

            dt = t["dt"]
            # @2018-05-24 fix BIGQUANT-666 由于V2修改时，为支持分钟数据，zipline返回的时间不再是UTC时间，而是自然交易时间
            # time = pd.Timestamp(dt.value, tz='Asia/Shanghai').strftime("%Y-%m-%d %H:%M")
            time = dt.strftime(time_format)

            if t["amount"] > 0:
                direction = "买入"
                total_buy += total
            else:
                direction = "卖出"
                total_sell += total
            transaction_num += 1
            # @20190629 show symbol name
            symbol_name = t.get("name", symbol)
            output_row = ["", time, symbol, symbol_name, direction, amount, price, total, commission]
            output_trs.insert(0, output_row)

        output_trs[0][0] = period_open.strftime("%Y-%m-%d")
        for tr in output_trs:
            output_all.append(tr)
            row_count_limit -= 1

        # 增加汇总这一行
        stats_number = "（{} 笔交易".format(transaction_num)
        stats_total_buy = "买入 ￥{}".format(round(total_buy, perf_round_num))
        stats_total_sell = "卖出 ￥{}）".format(round(total_sell, perf_round_num))
        # sum_output_row = ["", "", "", "", "", stats_number, stats_total_buy, stats_total_sell]
        sum_output_row = ["", "", "", "", "", "", stats_number, stats_total_buy, stats_total_sell]
        output_all.append(sum_output_row)
        row_count_limit -= 1

    # columns = ["日期", "时间", "股票代码", "买/卖", "数量", "成交价", "总成本", "交易佣金"]
    columns = ["日期", "时间", "股票代码", "股票名称", "买/卖", "数量", "成交价", "总成本", "交易佣金"]

    return pd.DataFrame(data=output_all, columns=columns), has_whole_data


def get_future_transactions(results, buy_moment, sell_moment, data_frequency, round_num=2):
    from bigtrader.constant import ConstantHelper

    if data_frequency in ["tick", "tick2"]:
        time_format = "%Y-%m-%d %H:%M:%S.%f"
    else:
        time_format = "%Y-%m-%d %H:%M:%S"
    row_count_limit = 10000

    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        if row_count_limit <= 0:
            break

        transactions = row["transactions"]
        if len(transactions) <= 0:
            continue

        #         commission_map = {}
        #         for od in row["orders"]:
        #             commission_map[od["id"]] = od["commission"]

        # period_close = row["period_close"]
        # tra_factors = row["TRA_FAC"]

        transaction_num = 0
        total_buy = 0.0
        total_sell = 0.0
        total_commission = 0.0
        output_trs = []
        for t in transactions:
            asset = t["sid"]
            symbol = t.get("symbol") or asset.symbol
            # amount = abs(round(t["amount"] * tra_factors[symbol], 2))
            amount = abs(round(t["amount"], 2))
            # price = round(t["price"] / tra_factors[symbol], 2)
            price = round(t["price"], round_num)
            total = round(t["amount"] * t["price"] * asset.multiplier, 2)
            offset_display = ConstantHelper.offset_desc(t["offset"])
            commission = round(t["commission"], 2)
            total_commission += commission
            dt = t["dt"]
            # time = pd.Timestamp(dt.value, tz='Asia/Shanghai').strftime("%Y-%m-%d %H:%M")
            time = dt.strftime(time_format)
            if t["amount"] > 0:
                direction = "买入"
                total_buy += total
            else:
                direction = "卖出"
                total_sell += total

            asset_name = t["name"] if t.get("name") else asset.name
            transaction_num += 1
            output_row = [
                time,
                symbol,
                asset_name,
                direction,
                offset_display,
                amount,
                price,
                total,
                commission,
            ]
            output_trs.insert(0, output_row)

        # output_trs[0][0] = period_close.strftime("%Y-%m-%d")
        for tr in output_trs:
            output_all.append(tr)
            row_count_limit -= 1

        # 增加汇总这一行
        stats_number = "（{} 笔交易".format(transaction_num)
        stats_total_buy = "买入 ￥{}".format(round(total_buy, 2))
        stats_total_sell = "卖出 ￥{}）".format(round(total_sell, 2))
        stats_total_commission = "总佣金￥{}）".format(round(total_commission, 2))
        output_all.append(["", "", "", "", "", stats_number, stats_total_buy, stats_total_sell, stats_total_commission])
        row_count_limit -= 1

    return pd.DataFrame(data=output_all, columns=["成交时间", "合约代码", "合约名称", "买/卖", "开/平", "数量", "成交价", "交易金额", "交易佣金"])


def calculate_alpha_sharp_ir(results):
    beta = results["beta"].tail(1)[0]
    risk_free = results["treasury_period_return"].tail(1)[0]
    cum_algo_return = results["algorithm_period_return"].tail(1)[0]
    cum_benchmark_return = results["benchmark_period_return"].tail(1)[0]
    num_trading_days = results["trading_days"].tail(1)[0]
    annualized_algo_return = (cum_algo_return + 1.0) ** (242.0 / num_trading_days) - 1.0
    annualized_benchmark_return = (cum_benchmark_return + 1.0) ** (242.0 / num_trading_days) - 1.0
    alpha = annualized_algo_return - (risk_free + beta * (annualized_benchmark_return - risk_free))
    alpha = round(alpha, 2)

    sharpe = results["sharpe"].tail(1)[0]
    if isinstance(sharpe, float):
        risk_free = results["treasury_period_return"].tail(1)[0]
        algo_volatility = results["algo_volatility"].tail(1)[0]
        if cum_algo_return <= -1.0:
            sharp_ratio = "n/a"
        else:
            sharp_ratio = ((cum_algo_return + 1) ** (252.0 / num_trading_days) - 1 - risk_free) / algo_volatility
            sharp_ratio = round(sharp_ratio, 2)
    else:
        sharp_ratio = "n/a"

    ir_raw = results["information"].tail(1)[0]
    if ir_raw:
        algo_returns = np.full(num_trading_days, np.nan)
        benchmark_returns = algo_returns.copy()
        cum_algo_returns = results["algorithm_period_return"]
        cum_benchmark_returns = results["benchmark_period_return"]
        for i in range(num_trading_days):
            if i == 0:
                algo_returns[i] = cum_algo_returns.iloc[i]
                benchmark_returns[i] = cum_benchmark_returns.iloc[i]
            else:
                algo_returns[i] = (1.0 + cum_algo_returns.iloc[i]) / (1.0 + cum_algo_returns.iloc[i - 1]) - 1.0
                benchmark_returns[i] = (1.0 + cum_benchmark_returns.iloc[i]) / (1.0 + cum_benchmark_returns.iloc[i - 1]) - 1.0
        relative_returns = benchmark_returns - algo_returns
        ir_base = np.nanstd(relative_returns) * (252 ** (1.0 / 2.0))
        if cum_algo_return <= -1.0:
            ir = "n/a"
        else:
            ir = ((cum_algo_return + 1) ** (252.0 / num_trading_days) - (cum_benchmark_return + 1) ** (252.0 / num_trading_days)) / ir_base
            ir = round(ir, 2)
    else:
        ir = "n/a"

    return alpha, sharp_ratio, ir


def calculate_relative_return(results):
    reset_results = results.reset_index()

    reset_results["date_ts"] = reset_results["index"].astype(int) / 1000000
    base_portfolio = results["starting_cash"][0]

    def portfolio_func(a):
        return [a[0].value / 1000000, a[1] / base_portfolio * 100 - 100]

    portfolio_return_data = reset_results[["index", "portfolio_value"]].values
    portfolio_plot_data = np.apply_along_axis(portfolio_func, 1, portfolio_return_data)

    benchmark_period_return = reset_results[["index", "benchmark_period_return"]].values

    relative_return_plot_data = []
    last_relative_return = 0
    algorithm_period_return = reset_results[["index", "algorithm_period_return"]].values
    for i in range(0, len(algorithm_period_return)):
        if i > 0:
            p = (1.0 + algorithm_period_return[i][1]) / (1.0 + algorithm_period_return[i - 1][1]) - 1.0
            b = (1.0 + benchmark_period_return[i][1]) / (1.0 + benchmark_period_return[i - 1][1]) - 1.0
            last_relative_return = (last_relative_return + 1.0) * (1.0 + p - b) - 1
        elif i == 0:
            p = algorithm_period_return[i][1]
            b = benchmark_period_return[i][1]
            last_relative_return = p - b
        # HACK: use portfolio_plot_data date index value
        d = portfolio_plot_data[i][0]
        relative_return_plot_data.append([d, last_relative_return * 100])
    return relative_return_plot_data


def calculate_benchmark_returns(raw_perf, benchmark_data):
    from empyrical import cum_returns

    has_period_return = "benchmark_period_return" in raw_perf.columns
    benchmark_data["benchmark_return"] = benchmark_data["close"].pct_change().fillna(0)
    if not has_period_return:
        benchmark_data["benchmark_period_return"] = cum_returns(benchmark_data["benchmark_return"]).fillna(0)
        raw_perf = raw_perf.merge(benchmark_data[["date", "benchmark_return", "benchmark_period_return"]], on="date", how="left")
    else:
        raw_perf = raw_perf.merge(benchmark_data[["date", "benchmark_return"]], on="date", how="left")
    raw_perf[["date", "benchmark_return", "benchmark_period_return"]] = raw_perf[["date", "benchmark_return", "benchmark_period_return"]].fillna(0)
    raw_perf = raw_perf.set_index("date")
    return raw_perf


def get_pnl_ratio(results_stk, results_subacct):
    profit = []
    loss = []
    p1, l1, p2, l2 = float(0.0), float(0.0), float(0.0), float(0.0)
    for i in range(results_stk.shape[0]):
        if results_stk["transactions"].iloc[i] == [] or (results_stk["profit_count"].iloc[i] == 0 and results_stk["loss_count"].iloc[i] == 0):
            p1 += float(0.0)
            l1 += float(0.0)
        elif results_subacct["transactions"].iloc[i] == [] or (
            results_subacct["profit_count"].iloc[i] == 0 and results_subacct["loss_count"].iloc[i] == 0
        ):
            p2 += float(0.0)
            l2 += float(0.0)
        else:
            for j in range(len(results_stk["transactions"].iloc[i])):
                if results_stk["transactions"].iloc[i][j]["offset"] == 1:
                    pnl = results_stk["transactions"].iloc[i][j]["realized_pnl"]
                    if pnl > 0:
                        p1 += pnl
                    elif pnl < 0:
                        l1 += pnl
                    else:
                        p1 += 0
                        l1 += 0
            for j in range(results_subacct.shape[0]):
                pnl = results_subacct["realized_pnl"].iloc[j]
                if pnl > 0:
                    p2 += pnl
                elif pnl < 0:
                    l2 += pnl
                else:
                    p2 += 0
                    l2 += 0
        profit.append(p1 + p2)
        loss.append(l1 + l2)
    profit = np.array(profit)
    loss = np.array(loss)
    pnl_ratio = -(profit / loss)
    pnl_ratio[np.isnan(pnl_ratio)] = 0
    pnl_ratio[np.isinf(pnl_ratio)] = 0
    return pnl_ratio[-1]
