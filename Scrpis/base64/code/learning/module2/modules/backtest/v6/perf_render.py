from collections import OrderedDict

import learning.api.tools as T
import numpy as np
import pandas as pd
from bigcharts.datatale import plot as plot_datatable
from bigcharts.tabs import plot as plot_tabs
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


def render(results, buy_moment, sell_moment, output="display"):
    stats = get_stats(results)
    general_html = stats if output == "object" else Template(stats_template).render(stats=stats, plot_data=plot_backtest_result(results))
    transactions = get_transactions(results, buy_moment, sell_moment)
    transactions_html = transactions if output == "object" else plot_datatable(get_transactions(results, buy_moment, sell_moment), output="script")
    positions = get_positions(results)
    positions_html = positions if output == "object" else plot_datatable(get_positions(results), output="script")
    logs = get_logs(results)
    logs_html = logs if output == "object" else plot_datatable(get_logs(results), output="script")

    odict = OrderedDict(
        [
            ("收益概况", general_html),
            ("交易详情", transactions_html),
            ("每日持仓和收益", positions_html),
            ("输出日志", logs_html),
        ]
    )

    if output == "object":
        return odict

    plot_tabs(odict, title="Trade(回测/模拟)")


def get_stats(results):
    return_value = results["algorithm_period_return"].tail(1)[0]
    return_ratio = round(return_value * 100, 2)
    num_trading_days = results["trading_days"].tail(1)[0]
    annual_return_ratio = round((pow(1 + return_value, 252.0 / num_trading_days) - 1) * 100, 2)
    benchmark_ratio = round(results["benchmark_period_return"].tail(1)[0] * 100, 2)
    beta = round(results["beta"].tail(1)[0], 2)
    alpha, sharp_ratio, ir = calculate_alpha_sharp_ir(results)

    return_volatility = round(results["algo_volatility"].tail(1)[0] * 100, 2)
    max_drawdown = round(abs(results["max_drawdown"].tail(1)[0] * 100), 2)
    win_ratio, profit_loss_ratio = calculate_win_loss_rate(results)
    return {
        "return_ratio": return_ratio,
        "annual_return_ratio": annual_return_ratio,
        "benchmark_ratio": benchmark_ratio,
        "beta": beta,
        "alpha": alpha,
        "sharp_ratio": sharp_ratio,
        "ir": ir,
        "return_volatility": return_volatility,
        "max_drawdown": max_drawdown,
        "win_ratio": win_ratio,
        "profit_loss_ratio": profit_loss_ratio,
    }


relative_cum_last = 0.0


def plot_backtest_result(results):
    # get recorded columns
    system_columns = {
        "index",
        "algo_volatility",
        "algorithm_period_return",
        "alpha",
        "benchmark_period_return",
        "benchmark_volatility",
        "beta",
        "buy_volume",
        "capital_used",
        "ending_cash",
        "ending_exposure",
        "ending_value",
        "excess_return",
        "gross_leverage",
        "information",
        "long_exposure",
        "long_value",
        "longs_count",
        "max_drawdown",
        "max_leverage",
        "net_leverage",
        "orders",
        "period_close",
        "period_label",
        "period_open",
        "pnl",
        "portfolio_value",
        "positions",
        "returns",
        "sharpe",
        "short_exposure",
        "short_value",
        "shorts_count",
        "sortino",
        "starting_cash",
        "starting_exposure",
        "starting_value",
        "trading_days",
        "transactions",
        "treasury_period_return",
        "LOG",
        "TRA_FAC",
        "POS_FAC",
    }
    record_columns = list(set(results) - system_columns)
    results["portfolio_ratio"] = (results["algorithm_period_return"]) * 100
    results["benchmark_ratio"] = (results["benchmark_period_return"]) * 100

    """ 计算相对收益率 """
    results["a_delta"] = results["algorithm_period_return"].shift(1)
    results["a_delta"].ix[0] = 0
    results["a_delta"] = (results["algorithm_period_return"] + 1.0) / (results["a_delta"] + 1.0) - 1.0
    results["b_delta"] = results["benchmark_period_return"].shift(1)
    results["b_delta"].ix[0] = 0
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
    dt = results.index.to_datetime()
    results.index = dt  # - 3600 * 1000000000 * dt.hour

    options = {
        "series": [
            {
                "name": "策略收益率",
                "color": "#ff9900",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
            },
            {
                "name": "基准收益率",
                "color": "#6699ff",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
            },
            {
                "name": "相对收益率",
                "color": "#994a4d",
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

    return T.plot(
        results[columns],
        panes=[["portfolio_ratio", "benchmark_ratio", "relative_ratio", "80%"], ["gross_leverage_percent", "20%"]],
        options=options,
        output="script",
    )


def get_transactions(results, buy_moment, sell_moment):
    buy_time = "09:30:00" if buy_moment == "open" else "15:00:00"
    sell_time = "09:30:00" if sell_moment == "open" else "15:00:00"
    row_count_limit = 10000

    output_all = []
    for index, row in results.iterrows():
        if row_count_limit <= 0:
            break

        transactions = row["transactions"]
        if len(transactions) <= 0:
            continue

        commission_map = {}
        for od in row["orders"]:
            commission_map[od["id"]] = od["commission"]

        period_open = row["period_open"]
        # tra_factors = row["TRA_FAC"]

        transaction_num = 0
        total_buy = 0.0
        total_sell = 0.0
        output_trs = []
        for t in transactions:
            symbol = t["sid"].symbol
            # amount = abs(round(t["amount"] * tra_factors[symbol], 2))
            amount = abs(round(t["amount"], 2))
            # price = round(t["price"] / tra_factors[symbol], 2)
            price = round(t["price"], 2)
            total = round(t["amount"] * t["price"], 2)
            if t["order_id"]:
                commission = round(commission_map[t["order_id"]], 2)
            else:
                commission = 0.0

            if t["amount"] > 0:
                time = buy_time
                direction = "买入"
                total_buy += total
            else:
                time = sell_time
                direction = "卖出"
                total_sell += total
            transaction_num += 1
            output_row = ["", time, symbol, direction, amount, price, total, commission]
            # 最终买入和卖出合并到一起
            if t["amount"] > 0:
                output_trs.insert(0, output_row)
            else:
                output_trs.append(output_row)

        output_trs[0][0] = period_open.strftime("%Y-%m-%d")
        for tr in output_trs:
            output_all.append(tr)
            row_count_limit -= 1

        # 增加汇总这一行
        stats_number = "（{} 笔交易".format(transaction_num)
        stats_total_buy = "买入 ￥{}".format(round(total_buy, 2))
        stats_total_sell = "卖出 ￥{}）".format(round(total_sell, 2))
        output_all.append(["", "", "", "", "", stats_number, stats_total_buy, stats_total_sell])
        row_count_limit -= 1
    return pd.DataFrame(data=output_all, columns=["日期", "时间", "股票代码", "买/卖", "数量", "成交价（元）", "总成本（元）", "交易佣金（元）"])


def get_positions(results):
    overall_commission = 0.0
    row_count_limit = 10000

    output_all = []
    for index, row in results.iterrows():
        if row_count_limit <= 0:
            break

        positions = row["positions"]
        period_open = row["period_open"]
        # pos_factors = row["POS_FAC"]

        for od in row["orders"]:
            if od["status"] != 1:
                continue
            overall_commission += od["commission"]

        has_date = False
        overall_total = 0.0
        overall_return = 0.0
        if len(positions) <= 0:
            output_all.append([period_open.strftime("%Y-%m-%d"), "--", "--", "--", "--", "--"])
            row_count_limit -= 1
        for pos in positions:
            date = ""
            if not has_date:
                date = period_open.strftime("%Y-%m-%d")
                has_date = True
            symbol = pos["sid"].symbol
            # close = round(pos["last_sale_price"] / pos_factors[symbol], 2)
            close = round(pos["last_sale_price"], 2)
            # amount = round(pos["amount"] * pos_factors[symbol], 2)
            amount = round(pos["amount"], 2)
            total = round(close * amount, 2)
            rt = round((pos["last_sale_price"] - pos["cost_basis"]) * pos["amount"], 2)
            overall_total += total
            overall_return += rt
            output_all.append([date, symbol, close, amount, total, rt])
            row_count_limit -= 1

        stats_portfolio_value = "（总资产￥{}".format(round(row["portfolio_value"], 2))
        stats_ending_cash = "剩余金额￥{}".format(round(row["ending_cash"], 2))
        stats_overall_commission = "总交易费用￥{}）".format(round(overall_commission, 2))
        stats_overall_total = "￥{}".format(round(overall_total, 2))
        stats_overall_return = "￥{}".format(round(overall_return, 2))
        output_all.append(["", stats_portfolio_value, stats_ending_cash, stats_overall_commission, stats_overall_total, stats_overall_return])
        row_count_limit -= 1
    return pd.DataFrame(data=output_all, columns=["日期", "股票代码", "收盘价（元）", "股数", "持仓（元）", "收益（元）"])


def get_logs(results):
    output_all = []
    for index, row in results.iterrows():
        if "LOG" not in row:
            # print('LOG key is not in row when v6 perf_render get_logs')
            continue
        logs = row["LOG"]
        if len(logs) <= 0:
            continue

        period_open = row["period_open"]
        for log in logs:
            if log["level"] == "ERROR":
                output_all.append([period_open.strftime("%Y-%m-%d 00:00:00"), log["level"], log["msg"]])

    return pd.DataFrame(data=output_all, columns=["时间", "级别", "内容"])


def calculate_alpha_sharp_ir(results):
    beta = results["beta"].tail(1)[0]
    risk_free = results["treasury_period_return"].tail(1)[0]
    cum_algo_return = results["algorithm_period_return"].tail(1)[0]
    cum_benchmark_return = results["benchmark_period_return"].tail(1)[0]
    num_trading_days = results["trading_days"].tail(1)[0]
    annualized_algo_return = (cum_algo_return + 1.0) ** (252.0 / num_trading_days) - 1.0
    annualized_benchmark_return = (cum_benchmark_return + 1.0) ** (252.0 / num_trading_days) - 1.0
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
                algo_returns[i] = cum_algo_returns.ix[i]
                benchmark_returns[i] = cum_benchmark_returns.ix[i]
            else:
                algo_returns[i] = (1.0 + cum_algo_returns.ix[i]) / (1.0 + cum_algo_returns.ix[i - 1]) - 1.0
                benchmark_returns[i] = (1.0 + cum_benchmark_returns.ix[i]) / (1.0 + cum_benchmark_returns.ix[i - 1]) - 1.0
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


def calculate_win_loss_rate(results):
    result = results
    num_trading_days = result["trading_days"].tail(1)[0]

    win_count = 0
    win_value = 0.0
    loss_count = 0
    loss_value = 0.0

    global_order_map = {}

    for i in range(num_trading_days):
        if i == 0:
            continue
        transactions = result.ix[i]["transactions"]
        if len(transactions) <= 0:
            continue

        # 上一个交易日的持仓
        positions_map = {}
        for lp in result.ix[i - 1]["positions"]:
            positions_map[lp["sid"].symbol] = lp

        # 当前交易日的下单
        orders_map = {}
        for od in result.ix[i]["orders"]:
            if od["filled"] == 0:
                continue
            orders_map[od["id"]] = od

        # 当前交易日的交易记录
        for t in transactions:
            if t["amount"] >= 0:
                continue

            symbol = t["sid"].symbol
            order_id = t["order_id"]
            if symbol not in positions_map:
                print("could not find last positions for symbol {}".format(symbol))
                continue
            if order_id not in orders_map:
                print("could not find order for id {}".format(order_id))
                continue

            # 前一个交易日的持仓
            position = positions_map[symbol]
            # 当天的下单
            order = orders_map[order_id]

            if order["status"] == 1 and order_id not in global_order_map:
                # 当天的新下单，当天均消化完毕
                commission = order["commission"] / order["filled"] * 1.0
                cost_basis = position["cost_basis"]
                sold_price = t["price"]

                if cost_basis <= 0:
                    # 为什么会出现这种情况呢？
                    # 假设第一天花10元买了100股Ａ股票，第二天以20元卖了60，第三天的时候剩下的40股
                    # cost_basis = (20 * 60 - 10 * 100) / 40，这种情况下再怎么卖，都时盈利的，这种case直接过滤掉
                    continue

                if sold_price > cost_basis + commission:
                    win_value = win_value * win_count + (sold_price - (cost_basis + commission)) * abs(t["amount"])
                    win_value /= win_count + 1.0
                    win_count += 1
                elif sold_price < cost_basis + commission:
                    loss_value = loss_value * loss_count + ((cost_basis + commission) - sold_price) * abs(t["amount"])
                    loss_value /= loss_count + 1.0
                    loss_count += 1

            if order["status"] == 1 and order_id in global_order_map:
                # 历史的单子，当天才消化完毕
                hist_order_info = global_order_map[order_id]
                total_commission = orders_map[order_id]["commission"]
                total_cost = hist_order_info["cost_basis"] * abs(order["amount"])
                hist_profit = hist_order_info["profit"]
                total_profit = t["price"] * abs(t["amount"]) + hist_profit

                del global_order_map[order_id]

                if total_profit > (total_cost + total_commission):
                    win_value = win_value * win_count + (total_profit - (total_cost + total_commission))
                    win_value /= win_count + 1.0
                    win_count += 1
                elif total_profit < (total_cost + total_commission):
                    loss_value = loss_value * loss_count + ((total_cost + total_commission) - total_profit)
                    loss_value /= loss_count + 1.0
                    loss_count += 1

            if order["status"] == 0:
                # 当天消化不完的单子
                if order_id not in global_order_map:
                    # 当天是这个order_id第一次卖此股票, 则前一天此股票持仓的cost_basis记下来, 日后全部卖出后用作持仓成本。
                    global_order_map[order_id] = {"cost_basis": position["cost_basis"], "profit": 0.0}

                cost_basis = global_order_map[order_id]["cost_basis"]
                hist_profit = global_order_map[order_id]["profit"]
                total_profit = t["price"] * abs(t["amount"]) + hist_profit

                global_order_map[order_id] = {"cost_basis": cost_basis, "profit": total_profit}

    return "{}".format(round(win_count * 1.0 / (win_count + loss_count), 3) if win_count + loss_count != 0 else "--"), "{}".format(
        round(win_value / loss_value, 3) if loss_value != 0 else "--"
    )


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
