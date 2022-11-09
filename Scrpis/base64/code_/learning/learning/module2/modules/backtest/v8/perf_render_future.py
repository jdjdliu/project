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

perf_round_num = 2
perf_frequency = "daily"


def render(results, buy_moment, sell_moment, output="display", round_num=2, data_frequency="daily", return_script=None, perf_raw_object=0):

    global perf_round_num
    perf_round_num = round_num
    global perf_frequency
    perf_frequency = data_frequency

    stats = get_stats(results)
    general_html = stats if output == "object" else Template(stats_template).render(stats=stats, plot_data=plot_backtest_result(results))
    transactions = get_transactions(results, buy_moment, sell_moment, round_num=round_num)
    transactions_html = transactions if output == "object" else plot_datatable(transactions, output="script")
    positions = get_positions(results, round_num=round_num)
    positions_html = positions if output == "object" else plot_datatable(positions, output="script")
    logs = get_logs(results)
    logs_html = logs if output == "object" else plot_datatable(logs, output="script")

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
    if return_script:
        return plot_tabs(odict, output="script")
    plot_tabs(odict, title="Trade(回测/模拟)")


def get_stats(results):
    return_value = results["algorithm_period_return"].tail(1)[0]
    return_ratio = round(return_value * 100, 2)
    num_trading_days = results["trading_days"].tail(1)[0]
    annual_return_ratio = round((pow(1 + return_value, 252.0 / num_trading_days) - 1) * 100, 2)
    benchmark_ratio = round(results["benchmark_period_return"].tail(1)[0] * 100, 2)
    alpha = round(results["alpha"].tail(1)[0], 2)
    beta = round(results["beta"].tail(1)[0], 2)
    sharp_ratio = round(results["sharpe"].tail(1)[0], 2)
    ir = round(results["information"].tail(1)[0], 2)
    # alpha, sharp_ratio, ir = calculate_alpha_sharp_ir(results)

    if sharp_ratio < -1.0e6:
        sharp_ratio = "n/a"

    return_volatility = round(results["algo_volatility"].tail(1)[0] * 100, 2)
    max_drawdown = round(abs(results["max_drawdown"].tail(1)[0] * 100), 2)
    ret = {
        "return_ratio": return_ratio,
        "annual_return_ratio": annual_return_ratio,
        "benchmark_ratio": benchmark_ratio,
        "beta": beta,
        "alpha": alpha,
        "sharp_ratio": sharp_ratio,
        "ir": ir,
        "return_volatility": return_volatility,
        "max_drawdown": max_drawdown,
        "win_ratio": round(results["win_percent"].tail(1)[0], 2),
        "profit_loss_ratio": round(results["pnl_ratio"].tail(1)[0], 2),
    }
    return ret


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
        "need_settle",
        "win_percent",
        "pnl_ratio",
        "trade_times",
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
                "name": "风险度",
                "color": "#66ff99",
                "tooltip": {
                    "valueDecimals": 2,
                    "valueSuffix": " %",
                },
                "visible": False,
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
                    "text": "风险度",
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


def get_transactions(results, buy_moment, sell_moment, round_num=2):
    from zipline.finance.constants import get_position_effect_description

    row_count_limit = 10000

    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        if row_count_limit <= 0:
            break

        transactions = row["transactions"]
        if len(transactions) <= 0:
            continue

        commission_map = {}
        for od in row["orders"]:
            commission_map[od["id"]] = od["commission"]

        # period_close = row["period_close"]
        # tra_factors = row["TRA_FAC"]

        transaction_num = 0
        total_buy = 0.0
        total_sell = 0.0
        total_commission = 0.0
        output_trs = []
        for t in transactions:
            asset = t["sid"]
            symbol = asset.symbol
            # amount = abs(round(t["amount"] * tra_factors[symbol], 2))
            amount = abs(round(t["amount"], 2))
            # price = round(t["price"] / tra_factors[symbol], 2)
            price = round(t["price"], round_num)
            total = round(t["amount"] * t["price"] * asset.multiplier, 2)
            position_effect_display = get_position_effect_description(t["position_effect"])
            commission = round(t["commission"], 2)
            total_commission += commission
            time = t["dt"]
            # time = pd.Timestamp(time.value, tz='Asia/Shanghai').strftime("%Y-%m-%d %H:%M")
            time = pd.Timestamp(time.value).strftime("%Y-%m-%d %H:%M")
            if t["amount"] > 0:
                direction = "买入"
                total_buy += total
            else:
                direction = "卖出"
                total_sell += total

            asset_name = t["name"] if t.get("name") else asset.asset_name
            transaction_num += 1
            output_row = [time, symbol, asset_name, direction, position_effect_display, amount, price, total, commission]
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


def get_positions(results, round_num=2):
    from zipline.assets import Equity

    row_count_limit = 10000

    output_all = []
    columns = None
    last_period_date = None

    for index, row in results.iloc[::-1].iterrows():  # reverse
        overall_commission = 0.0
        if row_count_limit <= 0:
            break
        need_settle = row["need_settle"]
        positions = row["positions"]
        period_close = row["period_close"]
        # pos_factors = row["POS_FAC"]

        for od in row["orders"]:
            if od["status"] != 1:
                continue
            overall_commission += od["commission"]

        date = period_close.strftime("%Y-%m-%d")
        overall_total = 0.0
        overall_margin_used_long = 0.0
        overall_margin_used_short = 0.0
        overall_return = 0.0
        overall_realized_pnl = 0.0
        if len(positions) <= 0:
            # @20180608 when minutes test, we don't append empty record for each minute
            night_open_time = pd.Timestamp(period_close).replace(hour=21, minute=0, second=0)
            if perf_frequency == "daily" or last_period_date is None:
                output_all.append([date, "--", "--", "--", "--", "--", "--"])
                row_count_limit -= 1
            elif last_period_date == date or period_close >= night_open_time:
                continue
            else:
                output_all.append([date, "--", "--", "--", "--", "--", "--"])
                row_count_limit -= 1

        last_period_date = date

        for pos in positions:
            asset = pos["sid"]
            symbol = asset.symbol

            try:
                asset_name = pos["name"]
            except KeyError:
                asset_name = asset.asset_name

            if isinstance(asset, Equity):
                # @20190808 for option backtest, re-use margin as position value, no settle price
                close = round(pos["last_sale_price"], round_num)
                settle = "--"
                amount_str = round(pos["amount"], 2)
                carry_cost = round(pos["cost_basis"], round_num)
                total_margin = round(pos["cost_basis"] * pos["amount"], 2)
                margin_str = "{0}".format(total_margin)
                overall_margin_used_long += total_margin
                pnl = round(pos.get("holding_pnl", 0.0), 2)
                price = close
            else:
                # close = round(pos["last_sale_price"] / pos_factors[symbol], 2)
                close = round(pos["last_sale_price"], round_num)
                settle = round(pos["settle_price"], round_num)
                amount = round(pos["amount"], 2)
                amount_str = "{0}[多:{1},空:{2}]".format(amount, round(pos["amount_long"], 2), round(pos["amount_short"], 2))
                carry_cost = round(pos["cost_basis"], round_num)
                total_margin = round(pos["margin_used"], 2)
                margin_long = round(pos["margin_used_long"], 2)
                margin_short = round(pos["margin_used_short"], 2)
                margin_str = "{0}[多:{1},空:{2}]".format(total_margin, margin_long, margin_short)
                overall_margin_used_long += margin_long
                overall_margin_used_short += margin_short
                pnl = round(pos["settle_pnl"] if need_settle else pos["holding_pnl"], 2)
                overall_return += pnl

                # @20180724 如果要结算时，持仓详情上显示结算价
                price = settle if need_settle else close

            realized_pnl = round(pos["realized_pnl"], 2)
            overall_total += total_margin
            overall_realized_pnl += realized_pnl

            output_all.append([date, symbol, asset_name, carry_cost, price, amount_str, margin_str, pnl, realized_pnl])
            row_count_limit -= 1

        stats_portfolio_value = "（总资产￥{}".format(round(row["portfolio_value"], 2))
        stats_ending_cash = "剩余金额￥{}".format(round(row["portfolio_value"] - overall_total, 2))
        stats_overall_commission = "当日交易佣金￥{}）".format(round(overall_commission, 2))
        stats_overall_total = "￥{0}[多:{1},空:{2}]".format(
            round(overall_total, 2), round(overall_margin_used_long, 2), round(overall_margin_used_short, 2)
        )
        stats_overall_return = "￥{}".format(round(overall_return, 2))
        stats_overall_realized_pnl = "￥{}".format(round(overall_realized_pnl, 2))
        output_all.append(
            [
                "",
                "",
                "",
                stats_portfolio_value,
                stats_ending_cash,
                stats_overall_commission,
                stats_overall_total,
                stats_overall_return,
                stats_overall_realized_pnl,
            ]
        )
        row_count_limit -= 1
        if columns is None:
            if need_settle:
                columns = ["日期", "合约代码", "合约名称", "持仓均价", "结算价", "数量", "持仓保证金", "盯市盈亏", "平仓盈亏"]
            else:
                columns = ["日期", "合约代码", "合约名称", "持仓均价", "收盘价", "数量", "持仓保证金", "浮动盈亏", "平仓盈亏"]
    return pd.DataFrame(data=output_all, columns=columns)


def get_logs(results):
    output_all = []
    for index, row in results.iloc[::-1].iterrows():  # reverse
        if "LOG" not in row:
            # print('LOG key is not in row when v7 perf_render get_logs')
            continue
        logs = row["LOG"]
        if len(logs) <= 0:
            continue

        # @20180607 we show detail log time
        # @20190620 show more detail log time
        period_close = row["period_close"].strftime("%Y-%m-%d %H:%M:00")
        for log in logs:
            # if (log['level'] == 'ERROR'):
            try:
                output_all.append([log["dt"], log["level"], log["msg"]])
            except KeyError:
                output_all.append([period_close, log["level"], log["msg"]])

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
