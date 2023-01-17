import os
from collections import OrderedDict
from datetime import datetime

import numpy as np
import pandas as pd
from bigcharts.datatale import plot as plot_datatable
from bigcharts.tabs import plot as plot_tabs
from jinja2 import Template

import learning.api.tools as T

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
    acct_type=None
):

    global perf_round_num
    perf_round_num = round_num
    global perf_frequency
    perf_frequency = data_frequency

    show_debug_info = outputs.show_debug_info
    if show_debug_info:
        print("{} perf_render raw_perf={}, benchmark_data={}, process stats..." \
              .format(datetime.now(), outputs.raw_perf, outputs.benchmark_data))

    stats = get_stats(outputs, acct_type=acct_type)
    general_html = (
        stats
        if output == "object"
        else Template(stats_template).render(
            stats=stats, plot_data=plot_backtest_result(outputs, plot_series_compare=plot_series_compare, acct_type=acct_type)
        )
    )

    odict = OrderedDict()
    odict["收益概况"] = general_html

    plot_charts = 2 | 4 | 8 if outputs.plot_charts == 1 else outputs.plot_charts

    if plot_charts & 2:
        if show_debug_info:
            print("{} perf_render process transactions...".format(datetime.now()))

        transactions, has_whole_data_transactions = get_transactions(outputs, buy_moment, sell_moment, data_frequency, acct_type=acct_type)
        transactions_html = (
            transactions
            if output == "object"
            else plot_datatable(transactions, output="script", has_whole_data=has_whole_data_transactions)
        )
        odict["交易详情"] = transactions_html

    if plot_charts & 4:
        if show_debug_info:
            print("{} perf_render process positions...".format(datetime.now()))

        positions, has_whole_data_positions = get_positions(outputs, acct_type=acct_type)
        positions_html = (
            positions
            if output == "object"
            else plot_datatable(positions, output="script", has_whole_data=has_whole_data_positions)
        )
        odict["每日持仓和收益"] = positions_html

    if plot_charts & 8:
        if show_debug_info:
            print("{} perf_render process logs...".format(datetime.now()))

        logs = get_logs(outputs, acct_type=acct_type)
        logs_html = logs if output == "object" else plot_datatable(get_logs(outputs, acct_type=acct_type), output="script")
        odict["输出日志"] = logs_html

    if show_debug_info:
        print("{} perf_render process plot...".format(datetime.now()))

    if output == "object":
        return odict
    if return_script:
        return plot_tabs(odict, output="script")
    plot_tabs(odict, title="HFTrade(回测/模拟)")


def get_stats(outputs, acct_type=None):
    from empyrical import (alpha_beta_aligned, annual_volatility, max_drawdown,
                           sharpe_ratio)

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

    results = outputs.read_one_raw_perf(acct_type).copy()

    if outputs.benchmark_data:
        benchmark_data = outputs.benchmark_data.read().copy()
        results = calculate_benchmark_returns(results, benchmark_data)
    else:
        results["benchmark_return"] = 0.0
        results["benchmark_period_return"] = 0.0

    return_value = results["algorithm_period_return"].tail(1).values[0]

    return_ratio = round(return_value * 100, 2)

    num_trading_days = results["trading_days"].tail(1).values[0]

    if num_trading_days > 30:
        annual_return_ratio = round((pow(1 + return_value, 242.0 / num_trading_days) - 1) * 100, 2)
    else:
        annual_return_ratio = "nan"

    benchmark_ratio = round(results["benchmark_period_return"].tail(1).values[0] * 100, 2)

    alpha, beta = alpha_beta_aligned(results["returns"].values, results["benchmark_return"].values, risk_free=risk_free)
    alpha, beta = round(alpha, 2), round(beta, 2)

    _sharpe_ratio = round(sharpe_ratio(results["returns"].values, risk_free=risk_free), 2)
    _sharpe_ratio = "n/a" if _sharpe_ratio < -1.0e6 else _sharpe_ratio

    ir = round(information_ratio(results["returns"].values, results["benchmark_return"].values), 2)

    return_volatility = round(annual_volatility(results["returns"].values) * 100, 2)

    _max_drawdown = round(abs(max_drawdown(results["returns"].values) * 100), 2)

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
        "win_ratio": round(results["win_percent"].tail(1).values[0], 2),
        "profit_loss_ratio": abs(round(results["pnl_ratio"].tail(1).values[0], 2)),
    }
    return stats


relative_cum_last = 0.0


def plot_backtest_result(outputs, plot_series_compare=None, acct_type=None):
    results = outputs.read_one_raw_perf(acct_type).copy()

    if outputs.benchmark_data:
        benchmark_data = outputs.benchmark_data.read().copy()
        results = calculate_benchmark_returns(results, benchmark_data)
    else:
        results["benchmark_return"] = 0.0
        results["benchmark_period_return"] = 0.0

    # get recorded columns
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
        "sortino",
        "beta",
        "pnl",
        "algo_volatility",
        "information_ratio",
        "short_value",
        "benchmark_returns",
        "alpha",
        "max_drawdown",
        "sharpe",
        "long_value",
        "benchmark_volatility"
    }
    record_columns = list(set(results) - system_columns)
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

    results["gross_leverage_percent"] = results["gross_leverage"] * 100
    columns = ["portfolio_ratio", "benchmark_ratio", "relative_ratio", "gross_leverage_percent"] + record_columns
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
            {
                "name": "持仓占比",
                "color": "#66ff99",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
            },
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
            {
                "title": {
                    "text": "持仓占比",
                },
                "labels": {"format": "{value}%"},
                "gridLineColor": "#cfcfcf",
                "gridLineWidth": 1,
            },
        ],
        "chart": {
            # 'height': 500,
        },
    }

    if len(record_columns) > 0:
        options["yAxis"].append(
            {
                "title": {
                    "text": "自定义数据",
                },
                "gridLineColor": "#cfcfcf",
                "gridLineWidth": 1,
            }
        )

    # hide user data pane when there's no data
    if not record_columns:
        options["yAxis"] = options["yAxis"][:2]

    if plot_series_compare:
        options["plotOptions"] = {
            "series": {"compare": plot_series_compare, "showInNavigator": True},
            "area": {
                "showInNavigator": True,
            },
        }

    return T.plot(
        results[columns],
        panes=[["portfolio_ratio", "benchmark_ratio", "relative_ratio", "80%"], ["gross_leverage_percent", "20%"]],
        options=options,
        output="script",
    )


def get_transactions(outputs, buy_moment, sell_moment, data_frequency="daily", acct_type=None):

    results = outputs.read_one_raw_perf(acct_type)

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


def get_positions(outputs, acct_type=None):
    results = outputs.read_one_raw_perf(acct_type)

    row_count_limit = 10000
    has_whole_data = True

    last_period_date = None
    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        overall_commission = 0.0
        if row_count_limit <= 0 and os.environ.get("displayAllData", "False") != "True":
            has_whole_data = False
            break

        positions = row["positions"]
        period_open = row["date"]
        # pos_factors = row["POS_FAC"]

        for t in row["transactions"]:
            overall_commission += t["commission"]

        date = period_open.strftime("%Y-%m-%d")
        overall_total = 0.0
        overall_return = 0.0
        if len(positions) <= 0:
            # @20180608 when minutes test, we don't append empty record for each minute
            if perf_frequency in ["1d", "daily"] or last_period_date is None:
                output_all.append([date, "--", "--", "--", "--", "--", "--", "--"])
                row_count_limit -= 1
            elif last_period_date == date:
                continue
            else:
                output_all.append([date, "--", "--", "--", "--", "--", "--", "--"])
                row_count_limit -= 1

        last_period_date = date

        for pos in positions:
            symbol = pos["symbol"]
            # close = round(pos["last_sale_price"] / pos_factors[symbol], 2)
            close = pos["last_sale_price"]
            # amount = round(pos["amount"] * pos_factors[symbol], 2)
            amount = round(pos["amount"], perf_round_num)
            carry_cost = round(pos["cost_basis"], 3)
            total = round(close * amount, perf_round_num)
            rt = round((pos["last_sale_price"] - pos["cost_basis"]) * pos["amount"], perf_round_num)
            overall_total += total
            overall_return += rt
            output_all.append([date, symbol, pos.get("name", ""), carry_cost, "{0:.3f}".format(close), amount, total, rt])
            row_count_limit -= 1

        stats_portfolio_value = "（总资产￥{}".format(round(row["portfolio_value"], 3))
        stats_ending_cash = "剩余金额￥{}".format(round(row["ending_cash"], 3))
        stats_overall_commission = "当日交易佣金￥{}）".format(round(overall_commission, 2))
        stats_overall_total = "￥{}".format(round(overall_total, 3))
        stats_overall_return = "￥{}".format(round(overall_return, 3))
        output_all.append(
            [
                "",
                "",
                stats_portfolio_value,
                stats_ending_cash,
                stats_overall_commission,
                stats_overall_total,
                stats_overall_return,
            ]
        )
        row_count_limit -= 1

    return pd.DataFrame(data=output_all, columns=["日期", "标的代码", "标的名称", "持仓均价", "收盘价", "数量", "持仓价值", "收益"]), has_whole_data


def get_logs(outputs, acct_type=None):
    results = outputs.read_one_raw_perf(acct_type)

    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        if "LOG" not in row:
            # print('LOG key is not in row when v7 perf_render get_logs')
            continue
        logs = row["LOG"]
        if len(logs) <= 0:
            continue

        period_open = row["date"].strftime("%Y-%m-%d %H:%M:%S")
        for log in logs:
            # if (log['level'] == 'ERROR'):
            try:
                output_all.append([log["dt"], log["level"], log["msg"]])
            except KeyError:
                output_all.append([period_open, log["level"], log["msg"]])

    return pd.DataFrame(data=output_all, columns=["时间", "级别", "内容"])


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
                benchmark_returns[i] = (1.0 + cum_benchmark_returns.iloc[i]) / (
                    1.0 + cum_benchmark_returns.iloc[i - 1]
                ) - 1.0
        relative_returns = benchmark_returns - algo_returns
        ir_base = np.nanstd(relative_returns) * (252 ** (1.0 / 2.0))
        if cum_algo_return <= -1.0:
            ir = "n/a"
        else:
            ir = (
                (cum_algo_return + 1) ** (252.0 / num_trading_days)
                - (cum_benchmark_return + 1) ** (252.0 / num_trading_days)
            ) / ir_base
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
        raw_perf = raw_perf.merge(
            benchmark_data[["date", "benchmark_return", "benchmark_period_return"]], on="date", how="left"
        )
    else:
        raw_perf = raw_perf.merge(benchmark_data[["date", "benchmark_return"]], on="date", how="left")
    raw_perf[["date", "benchmark_return", "benchmark_period_return"]] = raw_perf[
        ["date", "benchmark_return", "benchmark_period_return"]
    ].fillna(0)
    raw_perf = raw_perf.set_index("date")
    return raw_perf
