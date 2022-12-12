from datetime import datetime, timedelta

import learning.api.tools as T
import pandas as pd
from bigcharts.datatale import plot as plot_datatable

from .perf_render import render as render_stock
from .perf_render_future import render as render_future
from .perf_render_portfolio import render as render_portfolio


def display(outputs, plot_series_compare=None):
    from bigtrader.constant import AcctType

    raw_perf = outputs.raw_perf.read()
    data_frequency = outputs.get_attr("data_frequency", "daily")
    if not isinstance(raw_perf, dict):
        # product_type = outputs.get_attr('product_type', 'stock')
        product_type = outputs.get_attr("product_type", "equity")
        acct_types = [AcctType.FUTURE if product_type in ["future", "option"] else AcctType.STOCK]
    else:
        acct_types = list(raw_perf.keys())
    if len(acct_types) == 1:
        if acct_types[0] in [AcctType.FUTURE, AcctType.OPTION]:
            render_future(
                outputs,
                buy_moment=outputs.order_price_field_buy,
                sell_moment=outputs.order_price_field_sell,
                round_num=3,
                data_frequency=data_frequency,
                acct_type=acct_types,
            )
        elif acct_types[0] in [AcctType.STOCK]:
            render_stock(
                outputs,
                buy_moment=outputs.order_price_field_buy,
                sell_moment=outputs.order_price_field_sell,
                round_num=3,
                data_frequency=data_frequency,
                plot_series_compare=plot_series_compare,
                acct_type=acct_types,
            )
    elif len(acct_types) > 1:
        render_portfolio(
            outputs,
            buy_moment=outputs.order_price_field_buy,
            sell_moment=outputs.order_price_field_sell,
            round_num=3,
            data_frequency=data_frequency,
            plot_series_compare=plot_series_compare,
            acct_type=acct_types,
        )


def read_raw_perf(outputs):
    return outputs.raw_perf.read()


def read_one_raw_perf(outputs, acct_type):
    raw_perfs = outputs.raw_perf.read()
    if not isinstance(raw_perfs, dict):
        return raw_perfs

    return raw_perfs.get(acct_type)


def analyze_pnl_per_trade(outputs, return_values=False):
    """每笔交易收益"""
    from bigtrader.constant import Direction

    all_trades = outputs.get_all_trades(as_zipline_data=True)

    if all_trades.empty:
        return

    pnl_dfs = []

    all_trades["trading_day"] = all_trades["dt"].apply(lambda d: d.strftime("%Y%m%d"))
    all_trades["amount"] = all_trades["amount"].apply(lambda a: a if a >= 0 else -a)
    all_trades["transaction_money"] = all_trades["transaction_money"].apply(lambda t: abs(t))
    symbols = all_trades["symbol"].unique().tolist()

    for symbol in symbols:
        trade = all_trades.loc[all_trades["symbol"] == symbol]

        if trade.empty:
            continue

        trade = trade.groupby(["order_id"], as_index=False).agg(
            {
                "trading_day": "first",
                "dt": "first",
                "offset": "first",
                "symbol": "first",
                "name": "first",
                "amount": "sum",
                "transaction_money": "sum",
                "commission": "sum",
            }
        )
        trade["order_id"] = pd.to_numeric(trade["order_id"])
        trade = trade.sort_values(["order_id"])
        trade = trade.reset_index(drop=True)

        all_orders = outputs.get_all_orders()

        def join_order_price(row):
            if row.name % 2 == 1:
                prev_order = all_orders[all_orders["order_sysid"] == str(int(row["order_id_prev"]))].iloc[0]
                row["order_price_prev"] = prev_order["order_price"]
                row["order_price_prev"] = (
                    abs(row["order_price_prev"]) if prev_order["direction"] == Direction.SHORT else -1 * abs(row["order_price_prev"])
                )
                row["direction_prev"] = prev_order["direction"]

                order = all_orders[all_orders["order_sysid"] == str(int(row["order_id"]))].iloc[0]
                row["order_price"] = order["order_price"]
                row["order_price"] = abs(row["order_price"]) if order["direction"] == Direction.SHORT else -1 * abs(row["order_price"])
                row["direction"] = order["direction"]
            return row

        trade["order_id_prev"] = trade["order_id"].shift(1)
        trade = trade.apply(join_order_price, axis=1)

        def join_transaction_money(row):
            if row.name % 2 == 1:
                row["transaction_money_prev"] = (
                    abs(row["transaction_money_prev"]) if row["direction_prev"] == Direction.SHORT else -1 * abs(row["transaction_money_prev"])
                )
                row["transaction_money"] = (
                    abs(row["transaction_money"]) if row["direction"] == Direction.SHORT else -1 * abs(row["transaction_money"])
                )
            return row

        trade["transaction_money_prev"] = trade["transaction_money"].shift(1)
        trade = trade.apply(join_transaction_money, axis=1)

        trade["commission_prev"] = trade["commission"].shift(1)
        trade["dt_prev"] = trade["dt"].shift(1)
        trade["amount_prev"] = trade["amount"].shift(1)

        trade["price_prev"] = trade["transaction_money_prev"] / trade["amount_prev"]
        trade["price"] = trade["transaction_money"] / trade["amount"]

        trade = trade.rename(columns={"commission": "commission_curr"})
        trade["commission"] = trade["commission_prev"] + trade["commission_prev"]
        trade["pnl"] = trade["transaction_money"] + trade["transaction_money_prev"]
        trade["pnl_with_commission"] = (
            trade["transaction_money"] + trade["transaction_money_prev"] - trade["commission_curr"] - trade["commission_prev"]
        )

        if len(trade) >= 2:
            trade = trade.iloc[1::2]

        pnl_dfs.append(trade)

    pnl_df = pd.concat(pnl_dfs).sort_values(["order_id"])

    pnl_df["direction_prev"] = pnl_df.apply(lambda r: "卖" if r["direction_prev"] == Direction.SHORT else "买", axis=1)
    pnl_df["direction"] = pnl_df.apply(lambda r: "卖" if r["direction"] == Direction.SHORT else "买", axis=1)

    pnl_df["prev_day"] = pnl_df["trading_day"].shift(1)
    pnl_df["trading_day"] = pnl_df.apply(lambda r: "" if r["trading_day"] == r["prev_day"] else r["trading_day"], axis=1)
    pnl_df["dt_prev"] = pnl_df["dt_prev"].apply(lambda d: d.strftime("%Y-%m-%d %H:%M:%S"))
    pnl_df["dt"] = pnl_df["dt"].apply(lambda d: d.strftime("%Y-%m-%d %H:%M:%S"))

    pnl_df = pnl_df.apply(
        format_money_number(
            [
                "order_price_prev",
                "price_prev",
                "transaction_money_prev",
                "order_price",
                "price",
                "transaction_money",
                "pnl",
                "pnl_with_commission",
            ]
        ),
        axis=1,
    )

    pnl_df = pnl_df[
        [
            "trading_day",
            "symbol",
            "name",
            "dt_prev",
            "dt",
            "direction_prev",
            "order_price_prev",
            "price_prev",
            "amount_prev",
            "transaction_money_prev",
            "commission_prev",
            "direction",
            "order_price",
            "price",
            "amount",
            "transaction_money",
            "commission_curr",
            "pnl",
            "pnl_with_commission",
        ]
    ]

    pnl_df = pnl_df.rename(
        columns={
            "trading_day": "交易日",
            "symbol": "股票代码",
            "name": "股票名称",
            "dt_prev": "开仓时间",
            "dt": "平仓时间",
            "direction_prev": "开仓方向",
            "order_price_prev": "开仓委托价格",
            "price_prev": "开仓均价",
            "amount_prev": "开仓数量",
            "transaction_money_prev": "开仓总额",
            "direction": "平仓方向",
            "order_price": "平仓委托价格",
            "price": "平仓均价",
            "amount": "平仓数量",
            "transaction_money": "平仓总额",
            "commission_prev": "开仓佣金",
            "commission_curr": "平仓佣金",
            "pnl": "收益(不计佣金)",
            "pnl_with_commission": "收益(计入佣金)",
        }
    )

    if return_values:
        return pnl_df
    else:
        plot_datatable(pnl_df)


def analyze_pnl_per_day(outputs, return_values=False):
    """每日盈亏"""
    from bigtrader.constant import Offset

    all_trades = outputs.get_all_trades(as_zipline_data=True)

    if all_trades.empty:
        return

    all_trades["trading_day"] = all_trades["dt"].apply(lambda d: d.strftime("%Y%m%d"))
    all_trades["transaction_money"] = all_trades.apply(
        lambda r: abs(r["transaction_money"]) if r["offset"] == Offset.CLOSE else -abs(r["transaction_money"]),
        axis=1,
    )

    pnl_df = all_trades.groupby(["trading_day"], as_index=False).agg({"transaction_money": "sum", "commission": "sum"})

    date_df = create_date_df(outputs.start_date, outputs.end_date, column="trading_day")

    pnl_df = date_df.merge(pnl_df, on="trading_day", how="left").fillna(0)
    pnl_df["pnl_with_commission"] = pnl_df["transaction_money"] - pnl_df["commission"]
    pnl_df.reindex(["trading_day", "commission", "transaction_money", "pnl_with_commission"])
    pnl_df = pnl_df.apply(format_money_number(["commission", "transaction_money", "pnl_with_commission"]), axis=1)

    pnl_df = pnl_df.rename(columns={"transaction_money": "pnl"})
    raw_df = pnl_df.copy()

    pnl_df = pnl_df.rename(
        columns={
            "trading_day": "日期",
            "pnl": "收益(不计佣金)",
            "commission": "交易佣金",
            "pnl_with_commission": "收益(计入佣金)",
        }
    )

    if return_values:
        return pnl_df, raw_df
    else:
        plot_datatable(pnl_df)


def plot_return_curve_per_stock(outputs, return_values=False):
    """每只股票收益率"""
    from bigtrader.constant import Offset

    all_trades = outputs.get_all_trades(as_zipline_data=True)

    if all_trades.empty:
        return

    all_trades["trading_day"] = all_trades["dt"].apply(lambda d: d.strftime("%Y%m%d"))
    all_trades["transaction_money"] = all_trades.apply(
        lambda r: abs(r["transaction_money"]) if r["offset"] == Offset.CLOSE else -abs(r["transaction_money"]),
        axis=1,
    )

    plot_dfs = {}

    for symbol in outputs.instruments:
        trade = all_trades.loc[all_trades["symbol"] == symbol]
        trade = trade.groupby(["trading_day"], as_index=False).agg({"transaction_money": "sum", "commission": "sum"})

        df = create_date_df(outputs.start_date, outputs.end_date, column="trading_day")
        df["transaction_money"] = 0
        df["commission"] = 0
        for _, row in trade.iterrows():
            if not df[df["trading_day"] == row["trading_day"]].empty:
                index = df[df["trading_day"] == row["trading_day"]].index
                df.at[index, "transaction_money"] = row["transaction_money"]
                df.at[index, "commission"] = row["commission"]

        df["capital"] = df["transaction_money"].cumsum() + outputs.capital
        df["capital_with_commission"] = (df["transaction_money"] - df["commission"]).cumsum() + outputs.capital
        df["return"] = df["capital"] / outputs.capital - 1
        df["return_with_commission"] = df["capital_with_commission"] / outputs.capital - 1

        df = df.rename(columns={"trading_day": "日期", "return": "收益率(不计佣金)", "return_with_commission": "收益率(计入佣金)"})

        plot_dfs[symbol] = df

    # tabs = OrderedDict({
    #     symbol: T.plot(df, x='日期', y=['收益率(不计佣金)', '收益率(计入佣金)'], title=symbol, output='script')
    #     for symbol, df in plot_dfs.items()
    # })

    # plot_tabs(tabs)
    if return_values:
        return plot_dfs
    else:
        for symbol, df in plot_dfs.items():
            T.plot(df, x="日期", y=["收益率(不计佣金)", "收益率(计入佣金)"], title=symbol)


def plot_curve_return(outputs, return_values=False):
    """总收益率"""
    _, df = analyze_pnl_per_day(outputs, return_values=True)

    df["capital"] = df["pnl"].cumsum()
    df["capital"] += outputs.capital
    df["rate_of_return"] = df["capital"] / outputs.capital - 1
    df["capital_with_commission"] = df["pnl_with_commission"].cumsum()
    df["capital_with_commission"] += outputs.capital
    df["rate_of_return_with_commission"] = df["capital_with_commission"] / outputs.capital - 1

    df = df.rename(columns={"trading_day": "日期", "rate_of_return": "收益率(不计佣金)", "rate_of_return_with_commission": "收益率(计入佣金)"})

    if return_values:
        return df
    else:
        T.plot(df, x="日期", y=["收益率(不计佣金)", "收益率(计入佣金)"], title="收益率")


def format_money_number(columns=None):
    def f(row):
        if columns is None:
            return row
        for col in columns:
            row[col] = round(row[col], 2)
        return row

    return f


def create_date_df(start_date, end_date, column="date"):
    curr_dt = pd.Timestamp(start_date).to_pydatetime()
    end_dt = pd.Timestamp(end_date).to_pydatetime()

    date_list = []

    while curr_dt <= end_dt:
        curr_date = datetime.strftime(curr_dt, "%Y%m%d")
        date_list.append(curr_date)
        curr_dt += timedelta(days=1)

    return pd.DataFrame(date_list, columns=[column])


def plot_trade_points(outputs, symbol, start_date=None, end_date=None, frequency=None):
    """plot trade point in kline or close series"""
    from bigtrader.constant import Direction, Frequency, Offset
    from bigtrader.utils.bar_generator import BarGenerator

    if not frequency:
        frequency = outputs.data_frequency

    dict_data = []
    if start_date:
        start_date = pd.Timestamp(start_date).strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_date = outputs.params["start_date"]
    if end_date:
        end_date = pd.Timestamp(end_date).strftime("%Y-%m-%d %H:%M:%S")
    else:
        end_date = outputs.params["end_date"]

    def _offset_desc(offset):
        if offset == Offset.OPEN:
            return "开仓"
        if offset == Offset.CLOSE:
            return "平仓"
        if offset == Offset.CLOSETODAY:
            return "平今"
        return offset if offset is not None else ""

    if frequency in [Frequency.TICK, Frequency.TICK2]:
        if outputs.tick_data:
            tick_df = outputs.tick_data.read()
        else:
            if symbol.endswith(("SHA", "SZA", "BJA")):
                table = "level1_snapshot_CN_STOCK_A" if outputs.data_frequency == Frequency.TICK else "level2_snapshot_CN_STOCK_A"
            elif symbol.endswith(("CFX", "SHF", "INE", "CZC", "DCE", "CFE", "GFE")):
                table = "level1_snapshot_CN_FUTURE"
            elif symbol.endswith(("HCB", "ZCB")):
                table = "level1_snapshot_CN_CONBOND"
            else:
                print("Unsupport symbol={} for frequency={}".format(symbol, frequency))

            from sdk.datasource import DataSource

            tick_df = DataSource(table).read([symbol], start_date, end_date, fields=["price"])
        tick_df.rename(columns={"instrument": "symbol", "date": "datetime"}, inplace=True)
        symbol_key = "symbol"
        tick_df = tick_df[tick_df[symbol_key] == symbol][[symbol_key, "price", "datetime"]]
        # print(tick_df.columns)
        tick_df.set_index("datetime", inplace=True)

        transactions = outputs.get_all_trades()
        transactions = transactions[transactions["symbol"] == symbol]
        if len(transactions) == 0:
            print("no transactions for symbol={}".format(symbol))
            return

        transactions["filled_price"] = round(transactions["filled_price"], 3)

        long_transactions = transactions[transactions["direction"] == Direction.LONG]
        short_transactions = transactions[transactions["direction"] == Direction.SHORT]
        common_transactions = pd.merge(long_transactions, short_transactions, on="trade_datetime", how="inner")

        for index in long_transactions.index:
            transaction = long_transactions.loc[index]
            text = "买入{} {}@{}".format(_offset_desc(transaction.get("offset")), transaction["filled_qty"], transaction["filled_price"])
            item = {"x": pd.to_datetime(transaction["trade_datetime"]), "title": "B", "color": "red", "text": text}
            dict_data.append(item)
        for index in short_transactions.index:
            transaction = short_transactions.loc[index]
            text = "卖出{} {}@{}".format(_offset_desc(transaction.get("offset")), transaction["filled_qty"], transaction["filled_price"])
            item = {"x": pd.to_datetime(transaction["trade_datetime"]), "title": "S", "color": "green", "text": text}
            dict_data.append(item)

        for index in common_transactions.index:
            item = {
                "x": pd.to_datetime(common_transactions.loc[index]["trade_datetime"]),
                "title": "T",
                "color": "black",
                "text": "买卖 @{}".format(common_transactions.loc[index]["filled_price"]),
            }
            dict_data.append(item)

        options = {
            "yAxis": [{"title": {"text": "价格"}}],
            "tooltip": {
                "split": "false",
                "shared": "true",
            },
            "scrollbar": {
                "enabled": "true",
            },
            "navigator": {"enabled": "true"},
            "series": [{"name": "价格", "id": "values"}, {"name": "买卖点", "type": "flags", "data": dict_data, "onSeries": "values"}],
        }
        T.plot(tick_df, options=options)
    else:
        transactions = outputs.get_all_trades()
        transactions = transactions[transactions["symbol"] == symbol]
        if len(transactions) == 0:
            print("no transactions for symbol={}".format(symbol))
            return

        if outputs.minute_data:
            ohlc_df = outputs.minute_data.read()
        elif frequency in ["1d", "daily"]:
            ohlc_df = outputs.daily_data.read()
        else:
            if symbol.endswith(("SHA", "SZA", "BJA")):
                table = "bar1d_CN_STOCK_A" if frequency in ["daily", "1d"] else "bar1m_CN_STOCK_A"
            elif symbol.endswith(("CFX", "SHF", "INE", "CZC", "DCE")):
                table = "bar1d_CN_FUTURE" if frequency in ["daily", "1d"] else "bar1m_CN_FUTURE"
            elif symbol.endswith(("HCB", "ZCB")):
                table = "bar1d_CN_CONBOND" if frequency in ["daily", "1d"] else "bar1m_CN_CONBOND"
            else:
                print("Unsupport symbol={} for frequency={}".format(symbol, frequency))

            from sdk.datasource import DataSource

            ohlc_df = DataSource(table).read([symbol], start_date, end_date)

        if frequency and int(frequency[:-1]) > 1:
            ohlc_df = BarGenerator.aggregate_df(ohlc_df, frequency)

        ohlc_df.rename(columns={"instrument": "symbol", "date": "datetime"}, inplace=True)

        symbol_key = "symbol"
        ohlc_df = ohlc_df[(ohlc_df[symbol_key] == symbol) & (ohlc_df["datetime"] >= pd.to_datetime(outputs.params["start_date"]))]
        ohlcv_df = ohlc_df[["datetime", "open", "high", "low", "close", "volume"]]
        ohlcv_df.set_index("datetime", inplace=True)

        transactions["filled_price"] = round(transactions["filled_price"], 3)

        long_transactions = transactions[transactions["direction"] == Direction.LONG]
        short_transactions = transactions[transactions["direction"] == Direction.SHORT]
        common_transactions = pd.merge(long_transactions, short_transactions, on="trade_datetime", how="inner")
        for index in long_transactions.index:
            transaction = long_transactions.loc[index]
            text = "买入{} {}@{}".format(_offset_desc(transaction.get("offset")), transaction["filled_qty"], transaction["filled_price"])
            item = {"x": pd.to_datetime(transaction["trade_datetime"]), "title": "B", "color": "red", "text": text}
            dict_data.append(item)
        for index in short_transactions.index:
            transaction = short_transactions.loc[index]
            text = "卖出{} {}@{}".format(_offset_desc(transaction.get("offset")), transaction["filled_qty"], transaction["filled_price"])
            item = {"x": pd.to_datetime(transaction["trade_datetime"]), "title": "S", "color": "green", "text": text}
            dict_data.append(item)
        for index in common_transactions.index:
            item = {
                "x": pd.to_datetime(common_transactions.loc[index]["trade_datetime"]),
                "title": "T",
                "color": "black",
                "text": "买卖 @{}".format(common_transactions.loc[index]["filled_price"]),
            }
            dict_data.append(item)

        options = {
            "title": symbol + " 分钟线",
            "yAxis": [{"title": {"text": "价格"}}],
            "xAxis": [
                {
                    "dateTimeLabelFormats": {
                        "millisecond": "%H:%M:%S.%L",
                        "second": "%H:%M:%S",
                        "minute": "%H:%M",
                        "hour": "%H:%M",
                        "day": "%m-%d",
                        "week": "%m-%d",
                        "month": "%y-%m",
                        "year": "%Y",
                    }
                }
            ],
            "tooltip": {
                "split": "false",
                "shared": "true",
            },
            "scrollbar": {
                "enabled": "true",
            },
            "navigator": {"enabled": "true"},
            "series": [
                {"name": "价格", "id": "values", "color": "green", "lineColor": "green", "upColor": "red", "upLineColor": "red"},
                {"type": "column"},
                {
                    "name": "买卖点",
                    "type": "flags",
                    "data": dict_data,
                    "onSeries": "values",
                },
            ],
        }
        T.plot(ohlcv_df, options=options, candlestick=True)
