import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

# from bigdata.api.datareader import DataReaderV2
from learning.module2.common.data import DataSource
from learning.module2.modules.backtest.v8 import bigquant_has_panel
from sdk.datasource.bigdata import constants
from sdk.utils import BigLogger

# log = logbook.Logger('forward_test')
log = BigLogger("forward_test")


# D2 = DataReaderV2()
# BENCHMARK_SYMBOL = "000300.SHA"  # 沪深300
RISK_FREE = 0.03
RISK_FREE_DAILY = 0.0001173
ANNUAL_FACTOR = 252


class PaperTestHelper(object):
    def __init__(self, price_type, is_stock, run_date, instruments, first_trading_date, benchmark_symbol):
        self.price_type = price_type
        self.is_stock = is_stock

        self.run_date_str = run_date
        terms = self.run_date_str.split("-")
        self.run_date = datetime(int(terms[0]), int(terms[1]), int(terms[2]), 0, 0, 0, 0, pytz.utc)

        self.instruments = instruments
        self.first_trading_date = first_trading_date
        self.benchmark_symbol = benchmark_symbol
        self.cum_return = 0.0  # 累计收益
        self.annual_return = 0.0  # 年化收益
        self.today_return = 0.0  # 当日收益
        self.max_drawdown = 0.0  # 最大回撤
        self.position_value = 0.0  # 持仓市值
        self.current_sharpe = 0.0
        self.current_pv = 0.0  # 当前总资产
        self.record_datas = None
        self.new_trading_days = 1
        self.adjust_factor_map = {}
        self.equity_name_map = {}

    def init_equity_name_map(self, instruments, start_date=None, end_date=None):
        if not instruments:
            # instruments = D2.instruments()
            instruments = None
        if not start_date:
            start_date = self.run_date_str
        if not end_date:
            end_date = self.run_date_str

        # equity_names = D2.history_data(
        #     instruments, start_date, end_date, fields=['name'])
        # equity_names = DataSource("instruments_CN_STOCK_A").read(instruments, start_date=start_date, end_date=end_date, fields=["name"])
        stock_instruments = DataSource("instruments_CN_STOCK_A").read(instruments, start_date=start_date, end_date=end_date, fields=["name"])
        fund_instruments = DataSource("instruments_CN_FUND").read(instruments, start_date=start_date, end_date=end_date, fields=["name"])

        dfs = []
        if stock_instruments is not None:
            dfs.append(stock_instruments)
        if fund_instruments is not None:
            dfs.append(fund_instruments)
        if not len(dfs):
            log.warn("instruments names is None for instruments={},start_date={},end_date={}".format(instruments, start_date, end_date))
        else:
            equity_names = pd.concat(dfs)
            equity_names.set_index("instrument", inplace=True)
            for symbol in equity_names.index:
                self.equity_name_map[symbol] = equity_names.loc[symbol]["name"]

    def get_equity_name(self, symbol):
        equity_name = None
        try:
            equity_name = self.equity_name_map.get(symbol, symbol)
        except KeyError:
            equity_name = symbol
        return equity_name

    def set_record_datas(self, record_datas):
        self.record_datas = record_datas
        if record_datas.last_record:
            self.new_trading_days = record_datas.last_record.trading_days + 1

    def get_amount(self, adjust_factor, orig_amount):
        if self.price_type == constants.price_type_post_right:
            return (int)(orig_amount * adjust_factor)
        return (int)(orig_amount)

    def get_price(self, adjust_factor, orig_price):
        if self.price_type == constants.price_type_post_right:
            return orig_price / adjust_factor
        return orig_price

    def get_adjust_factor_map(self, last_positions, ohlc_data):
        if not self.is_stock:
            return

        last_positions_adjust_factor_map = {}
        for lp in last_positions:
            last_positions_adjust_factor_map[lp["sid"]] = lp["adjust_factor"]

        if bigquant_has_panel and isinstance(ohlc_data, pd.Panel):
            adjust_factor_map = {}
            for symbol in ohlc_data.keys():
                adjust_factor_map[symbol] = ohlc_data[symbol]["adjust_factor"].values[-1]
        else:
            temp_df = ohlc_data.groupby("instrument").last()
            adjust_factor_map = temp_df["adjust_factor"].to_dict()

        for symbol, adjust_factor in adjust_factor_map.items():
            self.adjust_factor_map[symbol] = float(adjust_factor)
            if np.isnan(self.adjust_factor_map[symbol]):
                if symbol not in last_positions_adjust_factor_map:
                    # 这只股票当天也没有量价值
                    # 且同时不在前一个交易日的持仓中
                    # 那么这只股票的 adjust_factor 信息没有必要保存（后面也不会用到）
                    continue
                self.adjust_factor_map[symbol] = last_positions_adjust_factor_map[symbol]
            if np.isnan(self.adjust_factor_map[symbol]):
                log.error("could not get adjust factor for symbol {}".format(symbol))
                return None
        # log.info("get_adjust_factor_map: {}".format(self.adjust_factor_map))

    def get_last_sale_date_dict(self, positions, algo_result):
        last_sale_date_dict = {}
        for _p in positions:
            if _p.get("last_sale_date"):
                last_sale_date_dict[_p["sid"]] = _p["last_sale_date"]
        for orders in reversed(list(algo_result.orders)):
            for order in orders:
                if order["amount"] <= 0:
                    continue
                instrument = order["sid"].symbol
                if instrument in last_sale_date_dict:
                    continue
                last_sale_date_dict[instrument] = str(order["dt"])

        log.info("get_last_sale_date_dict: {}".format(last_sale_date_dict))
        return last_sale_date_dict

    def get_positions(self, result_last_row, last_sale_date_dict, last_positions):
        if self.is_stock:
            return self.get_positions_stocks(result_last_row, last_sale_date_dict, last_positions)
        else:
            return self.get_positions_futures(result_last_row, last_sale_date_dict, last_positions)

    def get_positions_stocks(self, result_last_row, last_sale_date_dict, last_positions):
        new_positions = result_last_row["positions"].values[0]

        json_positions = []
        lp_infos = {}

        # 对于退市的股票，价格直接清零，同时从持仓中删除（强行清零平仓）
        # available_instruments = D2.instruments(start_date=self.run_date_str)
        # available_instruments_df = DataSource("instruments_CN_STOCK_A").read(start_date=self.run_date_str, fields=["instrument"])
        stock_instruments_df = DataSource("instruments_CN_STOCK_A").read(start_date=self.run_date_str, fields=["instrument"])
        fund_instruments_df = DataSource("instruments_CN_FUND").read(start_date=self.run_date_str, fields=["instrument"])
        dfs = []
        if stock_instruments_df is not None:
            dfs.append(stock_instruments_df)
        if fund_instruments_df is not None:
            dfs.append(fund_instruments_df)
        available_instruments_df = pd.concat(dfs)
        available_instruments = list(available_instruments_df["instrument"].unique())
        de_list_instruments = list(set(self.instruments) - set(available_instruments))

        for p in new_positions:
            symbol = p["sid"].symbol
            # Fix support ETF papertrading: only check ashares @20201019
            if symbol.endswith(("SHA", "SZA")) and symbol in de_list_instruments:
                log.warn("instrument {} de_listed.".format(symbol))
                continue
            self.position_value += p["amount"] * float(p["last_sale_price"])  # 当日持仓市值
            lp_infos[symbol] = {
                "hold_days": 1,
                "first_hold_date": self.run_date_str,
            }

            json_positions.append(
                {
                    "hold_days": 1,
                    "first_hold_date": self.run_date_str,
                    "amount": p["amount"],
                    "cost_basis": round(float(p["cost_basis"]), 4),
                    "cost_basis_after_adjust": round(float(self.get_price(self.adjust_factor_map[symbol], p["cost_basis"])), 4),
                    "last_sale_price": round(float(p["last_sale_price"]), 2),
                    "price_after_adjust": round(float(self.get_price(self.adjust_factor_map[symbol], p["last_sale_price"])), 2),
                    "amount_after_adjust": self.get_amount(self.adjust_factor_map[symbol], p["amount"]),
                    "value": round(p["last_sale_price"] * p["amount"], 2),  # 当前市值
                    "cum_return": round((p["last_sale_price"] - p["cost_basis"]) * p["amount"], 2),
                    "sid": symbol,
                    "adjust_factor": self.adjust_factor_map[symbol],
                    "name": self.get_equity_name(symbol),
                    "date": self.run_date_str,
                    "last_sale_date": last_sale_date_dict.get(symbol),
                }
            )

        if len(last_positions) > 0:
            for last_p in last_positions:
                if last_p["sid"] in lp_infos:  # 昨日已经持有，不是今日新买入
                    lp_infos[last_p["sid"]]["hold_days"] = last_p["hold_days"] + 1
                    lp_infos[last_p["sid"]]["first_hold_date"] = last_p["first_hold_date"]

            for json_position_temp in json_positions:
                temp_sid = json_position_temp["sid"]
                if temp_sid in lp_infos:
                    json_position_temp["hold_days"] = lp_infos[temp_sid]["hold_days"]
                    json_position_temp["first_hold_date"] = lp_infos[temp_sid]["first_hold_date"]

        positions_str = ",\n".join([str(_newpos) for _newpos in new_positions])
        json_pos_str = ",\n".join([str(_jsonpos) for _jsonpos in json_positions])
        log.info(
            "get_positions_stocks position_value:{0} new_positions:\n{1} \njson_positions:\n{2}".format(
                self.position_value, positions_str, json_pos_str
            )
        )
        return new_positions, json.dumps(json_positions)

    def get_positions_futures(self, result_last_row, last_sale_date_dict, last_positions):
        new_positions = result_last_row["positions"].values[0]

        json_positions = []
        lp_infos = {}
        need_settle = result_last_row["need_settle"].values[0] if result_last_row.get("need_settle") else False
        for p in new_positions:
            asset = p["sid"]
            self.position_value += p["margin_used"]  # 当日保证金
            lp_infos[asset.symbol] = {
                "hold_days": 1,
                "first_hold_date": self.run_date_str,
            }
            position_long = {
                "hold_days": 1,
                "first_hold_date": self.run_date_str,
                "amount": p["amount"],
                "cost_basis": round(float(p["cost_basis"]), 4),
                "last_sale_price": round(float(p["last_sale_price"]), 2),
                # 当前市值
                "value": round(p["last_sale_price"] * p["amount"], 2),
                "cum_return": round((p["last_sale_price"] - p["cost_basis"]) * p["amount"], 2),
                "sid": asset.symbol,
                "name": p["sid"].asset_name,
                "date": self.run_date_str,
                "last_sale_date": last_sale_date_dict.get(asset.symbol),
                "settle_price": round(float(p["settle_price"]), 2),
                "margin_used": round(float(p["margin_used_long"]), 2),
                "realized_pnl": round(float(p["realized_pnl"]), 2),
                "pos_direction": "多",
            }

            position_short = {
                "hold_days": 1,
                "first_hold_date": self.run_date_str,
                "amount": p["amount"],
                "cost_basis": round(float(p["cost_basis"]), 4),
                "last_sale_price": round(float(p["last_sale_price"]), 2),
                # 当前市值
                "value": round(p["last_sale_price"] * p["amount"], 2),
                "cum_return": round((p["last_sale_price"] - p["cost_basis"]) * p["amount"] * -1, 2),
                "sid": asset.symbol,
                "name": p["sid"].asset_name,
                "date": self.run_date_str,
                "last_sale_date": last_sale_date_dict.get(asset.symbol),
                "settle_price": round(float(p["settle_price"]), 2),
                "margin_used": round(float(p["margin_used_short"]), 2),
                "realized_pnl": round(float(p["realized_pnl"]), 2),
                "pos_direction": "空",
            }

            if need_settle:
                position_long["settle_pnl"] = round(float(p["settle_pnl"]), 2)
                position_short["settle_pnl"] = round(float(p["settle_pnl"]), 2)
            else:
                position_long["holding_pnl"] = round(float(p["holding_pnl"]), 2)
                position_short["holding_pnl"] = round(float(p["holding_pnl"]), 2)

            json_positions.append(position_long)
            json_positions.append(position_short)

        if len(last_positions) > 0:
            for last_p in last_positions:
                if last_p["sid"] in lp_infos:  # 昨日已经持有，不是今日新买入
                    lp_infos[last_p["sid"]]["hold_days"] = last_p["hold_days"] + 1
                    lp_infos[last_p["sid"]]["first_hold_date"] = last_p["first_hold_date"]

            for json_position_temp in json_positions:
                temp_sid = json_position_temp["sid"]
                if temp_sid in lp_infos:
                    json_position_temp["hold_days"] = lp_infos[temp_sid]["hold_days"]
                    json_position_temp["first_hold_date"] = lp_infos[temp_sid]["first_hold_date"]

        positions_str = ",\n".join([str(_newpos) for _newpos in new_positions])
        json_pos_str = ",\n".join([str(_jsonpos) for _jsonpos in json_positions])
        log.info(
            "get_positions_futures position_value:{0}, new_positions:{1} \njson_positions:\n{2}".format(
                self.position_value, positions_str, json_pos_str
            )
        )
        return new_positions, json.dumps(json_positions)

    def get_portfolio(self, portfolio_value, original_capital_base, last_positions, max_pv, drawdown, trading_day_index):
        self.current_pv = portfolio_value

        last_pv = 0.0  # 上一交易日总资产
        if self.record_datas.last_record:
            last_pv += self.record_datas.last_record.cash
        else:
            last_pv = original_capital_base

        if len(last_positions) > 0:
            for last_pos in last_positions:
                # asset = last_pos['sid']
                last_amount = last_pos["amount"]
                last_price = last_pos["last_sale_price"]
                # print('position:', last_pos)
                if self.is_stock:
                    last_pv += last_amount * last_price
                else:
                    last_pv += last_pos["margin_used"]

        if self.current_pv > max_pv:
            max_pv = self.current_pv
        drawdown_current = float(max_pv - self.current_pv) / float(max_pv)
        self.max_drawdown = max(drawdown_current, drawdown)  # 得到迄今为止的最大回撤
        self.cum_return = (self.current_pv - original_capital_base) / original_capital_base  # 累计收益
        # if abs(self.cum_return) < 0.0001:
        #     self.cum_return = 0.0
        # print('-------------------------------cum_return:{0}, original:{1}, current_pv:{2}'.format(self.cum_return, original_capital_base, self.current_pv))
        self.today_return = (self.current_pv - last_pv) / last_pv  # 当日收益
        self.annual_return = pow(1.0 + self.cum_return, 252 / (trading_day_index + 1)) - 1.0  # 年化收益
        json_portfolio = {
            "cum_return": self.cum_return,
            "annual_return": self.annual_return,
            "today_return": self.today_return,
            "pv": self.position_value,
            "max_pv": max_pv,
            "drawdown": self.max_drawdown,
            "first_date": self.first_trading_date,
        }

        log.info("get_portfolio last_pv:{0}, current_pv:{1}, json_portfolio:{2}".format(last_pv, self.current_pv, json_portfolio))
        # print('last_pv:', last_pv, 'current_pv',  self.current_pv, ' portfolio:', json_portfolio)
        return json.dumps(json_portfolio)

    def get_orders(self, result_last_row, history_data, run_date):
        from zipline.finance.constants import get_position_effect_description
        from zipline.finance.order import ORDER_STATUS

        new_orders = result_last_row["orders"].values[0]

        # append price to orders
        new_orders_df = pd.DataFrame(new_orders)
        if len(new_orders_df) > 0:
            new_orders_df["created"] = new_orders_df.created.apply(lambda x: pd.to_datetime(run_date))
            new_orders_df["instrument"] = new_orders_df.sid.apply(lambda x: x.symbol)

            if bigquant_has_panel and isinstance(history_data, pd.Panel):
                instrument_list = sorted(set(new_orders_df.instrument) & set(history_data.keys()))
                data_df = pd.concat([df.assign(instrument=instrument) for instrument, df in history_data[instrument_list].iteritems()]).reset_index()
            else:
                instrument_list = sorted(set(new_orders_df.instrument) & set(list(history_data.instrument.unique())))
                data_df = history_data[history_data.instrument.isin(instrument_list)]

            data_df["created"] = data_df.date.apply(lambda x: pd.to_datetime(x.date()))

            new_orders_df = new_orders_df.merge(
                data_df[["created", "instrument", "close", "adjust_factor"]], on=["created", "instrument"], how="left"
            )
            new_orders_df = new_orders_df[new_orders_df.status == ORDER_STATUS.OPEN].copy()
            new_orders_df.fillna(0, inplace=True)
        json_orders = []
        for o in new_orders:
            if o["status"] == ORDER_STATUS.OPEN:
                symbol = o["sid"].symbol
                amount = o["amount"] - o["filled"]
                time_str = self._get_time_str(o["dt"])
                json_order = {
                    "amount": amount,
                    "sid": symbol,
                    "direction": "买" if amount > 0 else "卖",
                    "date": self.run_date_str,
                    "dt": time_str,
                }
                if self.is_stock:
                    close_price = new_orders_df[new_orders_df.instrument == symbol].iloc[0]["close"]
                    json_order["amount_after_adjust"] = abs(self.get_amount(self.adjust_factor_map[symbol], amount))
                    json_order["adjust_factor"] = self.adjust_factor_map[symbol]
                    json_order["name"] = self.get_equity_name(symbol)
                    json_order["price"] = round(float(self.get_price(json_order["adjust_factor"], close_price)), 2)
                else:
                    json_order["name"] = o["sid"].asset_name
                    json_order["position_effect"] = o["position_effect"]
                    json_order["offset_flag_display"] = get_position_effect_description(o["position_effect"])
                json_orders.append(json_order)

        # orders_str = ',\n'.join([str(_ord) for _ord in new_orders])
        json_orders_str = ",\n".join([str(_jsonord) for _jsonord in json_orders])
        log.info("get_orders new_orders {0}:\njson_orders:\n{1}".format(len(new_orders), json_orders_str))
        return new_orders_df, new_orders, json.dumps(json_orders)

    def get_transactions(self, result_last_row):
        from zipline.finance.constants import get_position_effect_description

        transactions = result_last_row["transactions"].values[0]
        # transactions_str = ',\n'.join([str(_trans) for _trans in transactions])
        # log.info('get_transactions transaction {0}:\n{1}'.format(
        #     len(transactions), transactions_str))
        log.info("get_transactions transaction num {0}".format(len(transactions)))

        json_transactions = []
        ret_transactions_buy = []
        ret_transactions_sell = []

        total_buy = 0.0
        total_sell = 0.0

        for t in transactions:
            symbol = t["sid"].symbol
            # @2018-05-24 fix BIGQUANT-666 由于V2修改时，为支持分钟数据，zipline返回的时间不再是UTC时间，而是自然交易时间
            # time_str = pd.Timestamp(t['dt'].value, tz='Asia/Shanghai').strftime("%Y-%m-%d %H:%M:%S")
            time_str = self._get_time_str(t["dt"])
            trans_temp = {
                "direction": None,
                "sid": symbol,
                "price": round(float(t["price"]), 2),
                "amount": t["amount"],
                "commission": round(t["commission"], 2),
                "cost": round(t["commission"], 2),
                "date": self.run_date_str,
                "dt": time_str,
            }
            if self.is_stock:
                trans_temp["price_after_adjust"] = round(float(self.get_price(self.adjust_factor_map[symbol], t["price"])), 2)
                trans_temp["amount_after_adjust"] = self.get_amount(self.adjust_factor_map[symbol], t["amount"])
                trans_temp["value"] = round(t["price"] * t["amount"], 2)
                trans_temp["adjust_factor"] = self.adjust_factor_map[symbol]
                trans_temp["name"] = self.get_equity_name(symbol)
            else:
                trans_temp["value"] = round(t["price"] * t["amount"], 2)  # old is 0.0
                trans_temp["name"] = t["sid"].asset_name
                trans_temp["position_effect"] = (t["position_effect"],)
                trans_temp["offset_flag_display"] = get_position_effect_description(t["position_effect"])
            if t["amount"] > 0:
                trans_temp["direction"] = "买"
                ret_transactions_buy.append(trans_temp)
                total_buy += trans_temp["value"]
            else:
                trans_temp["direction"] = "卖"
                # print('trans:', trans_temp)
                ret_transactions_sell.append(trans_temp)
                total_sell += trans_temp["value"]
        json_transactions += ret_transactions_sell
        json_transactions += ret_transactions_buy

        json_trans_str = ",\n".join([str(_json_trans) for _json_trans in json_transactions])
        log.info("get_transactions total_buy:{0}, total_sell:{1}, json_transaction:\n{2}".format(total_buy, total_sell, json_trans_str))
        return json.dumps(json_transactions)

    def get_json_logs(self, result_last_row):
        return json.dumps(result_last_row["LOG"].values[0]) if "LOG" in result_last_row else []

    # def get_bench_mark(self):
    #     benchmark_start_date = self.run_date - timedelta(days=15)
    #     benchmark = D.history_data([BENCHMARK_SYMBOL], benchmark_start_date, self.run_date_str, fields=['close'])
    #     benchmark_len = len(benchmark)
    #     return benchmark.iloc[benchmark_len - 1]['close'] / benchmark.iloc[benchmark_len - 2]['close'] - 1.0

    def get_risk_indicators(self, today_cum_benchmark):
        from empyrical import stats

        liverun_so_far = []
        benchmark_so_far = []
        liverun_previous = 0.0
        # benchmark_previous = 0.0
        current_beta = 0.0
        current_alpha = 0.0
        current_volatility = 0.0
        # current_sharpe = 0.0
        current_ir = 0.0
        count = 0
        # print('===============self.record_datas.all_previous_records:', self.record_datas.all_previous_records)
        # print('===============self.cum_return :', self.cum_return)
        for record in self.record_datas.all_previous_records:
            current_liverun = record.portfolio["cum_return"]
            json_benchmark = record.benchmark
            try:
                current_benchmark_return = json_benchmark[self.benchmark_symbol]
            except KeyError:
                # @202112 just compatible old .SHA benchmark symbol
                old_benchmark_symbol = self.benchmark_symbol.replace("HIX", "SHA").replace("ZIX", "SZA")
                if old_benchmark_symbol not in json_benchmark:
                    log.error("benchmark symbol keyerror for {},{},json={}".format(self.benchmark_symbol, old_benchmark_symbol, json_benchmark))
                current_benchmark_return = json_benchmark[old_benchmark_symbol]
            algo_return = (current_liverun - liverun_previous) / (1.0 + liverun_previous)
            liverun_so_far.append(algo_return)
            # benchmark_so_far.append((current_benchmark + 1.0) / (benchmark_previous + 1.0) - 1.0)
            benchmark_so_far.append(current_benchmark_return)
            liverun_previous = current_liverun
            # benchmark_previous = current_benchmark_return
            count += 1
        if self.record_datas.last_record:
            prev_return = self.record_datas.last_record.portfolio["cum_return"]
            prev_net = prev_return + 1.0
            algo_return = (self.cum_return - prev_return) / prev_net
            liverun_so_far.append(algo_return)
            # benchmark_so_far.append((cum_benchmark + 1.0) / (json.loads(self.record_datas.last_record.benchmark)[BENCHMARK_SYMBOL] + 1.0) - 1.0)
            benchmark_so_far.append(today_cum_benchmark)
        if count > 0:
            joint = np.vstack([liverun_so_far, benchmark_so_far])
            cov = np.cov(joint, ddof=0)
            liverun_so_far_series = pd.Series(liverun_so_far)
            # print ('liverun_so_far_series: {}'.format(len(liverun_so_far_series)))
            benchmark_so_far_series = pd.Series(benchmark_so_far)
            if np.absolute(cov[1, 1]) >= 1.0e-30:
                # beta
                # current_beta = cov[0, 1] / cov[1, 1]
                current_alpha, current_beta = stats.alpha_beta_aligned(liverun_so_far_series, benchmark_so_far_series, RISK_FREE_DAILY)
                # print('=============liverun_so_far:{0}, benchmark_so_far:{1},cum_benchmark:{2}, alpha:{3},beta:{4}'.format(
                #    liverun_so_far, benchmark_so_far, cum_benchmark, current_alpha, current_beta))

                # alpha
                # annualized_liverun_return = (self.cum_return + 1.0) ** (252.0 / self.new_trading_days) - 1.0
                # annualized_benchmark_return = (cum_benchmark + 1.0) ** (252.0 / self.new_trading_days) - 1.0
                # current_alpha = annualized_liverun_return - (RISK_FREE + current_beta * (annualized_benchmark_return - RISK_FREE))

                # volatility
                current_volatility = stats.annual_volatility(liverun_so_far_series)
                # print ('current_volatility:{}'.format(current_volatility))
                # sharpe
                # annualized_volatility = current_volatility * (ANNUAL_FACTOR ** 0.2)
                # if annualized_volatility != 0:
                #    current_sharpe = (annualized_liverun_return - RISK_FREE) / annualized_volatility
                if current_volatility != 0:
                    self.current_sharpe = stats.sharpe_ratio(liverun_so_far_series, RISK_FREE_DAILY)
                # print ('self.current_sharpe:{}'.format(self.current_sharpe))

                # information ratio
                # relative_so_far = np.subtract(liverun_so_far, benchmark_so_far)
                # relative_volatility = np.nanstd(relative_so_far)
                # annualized_relative_volatility = relative_volatility * (ANNUAL_FACTOR ** 0.2)
                # current_ir = (annualized_liverun_return - annualized_benchmark_return) / annualized_relative_volatility

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

                current_ir = information_ratio(liverun_so_far_series, benchmark_so_far_series)

        json_risk_indicator = {
            "alpha": current_alpha,
            "beta": current_beta,
            "volatility": current_volatility,
            "sharpe": self.current_sharpe,
            "ir": current_ir,
        }

        log.info("get_risk_indicators json_risk_indicator:{0}".format(json_risk_indicator))
        return json.dumps(json_risk_indicator)

    def get_json_extension(self, result_last_row, order_price_field_buy, order_price_field_sell):
        return json.dumps(
            {
                "order_price_field_buy": order_price_field_buy,
                "order_price_field_sell": order_price_field_sell,
                "is_stock": str(self.is_stock),
                "need_settle": str(result_last_row["need_settle"].values[0]),
            }
        )

    def _get_time_str(self, dt):
        """
        dt is from zipline's orders or transactions 'dt' field
        and now begin v2, zipline returns real trading 'date time'
        """
        # pd.Timestamp(dt.value, tz='Asia/Shanghai').strftime("%Y-%m-%d %H:%M:%S")
        return pd.Timestamp(dt.value).strftime("%Y-%m-%d %H:%M:%S")

    def init_live_positions(self, positions, ohlc_data):
        """初始化查询出来的实盘持仓"""
        for position in positions:
            symbol = position["sid"]
            if bigquant_has_panel and isinstance(ohlc_data, pd.Panel):
                last_price = float(ohlc_data[symbol]["close"].values[-1])
                adjust_factor = float(ohlc_data[symbol]["adjust_factor"].values[-1])
            else:
                last_price = ohlc_data[ohlc_data["instrument"] == symbol]["close"].iloc[-1]
                adjust_factor = ohlc_data[ohlc_data["instrument"] == symbol]["adjust_factor"].iloc[-1]

            if not position["first_hold_date"]:
                position["first_hold_date"] = self.run_date_str
            if not position["last_sale_date"]:
                position["last_sale_date"] = self.run_date_str
            position["cost_basis_after_adjust"] = position["cost_basis"]  # 真实价格（用户上传的）
            position["last_sale_price"] = last_price  # 复权或真实价格
            position["price_after_adjust"] = last_price  # 复权或真实价格
            if self.price_type == constants.price_type_post_right:
                position["cost_basis"] *= adjust_factor  # 复权价格
                position["price_after_adjust"] = last_price / adjust_factor  # 真实价格
            position["amount_after_adjust"] = position["amount"]  # 数量暂不做调整
            position["value"] = round(position["price_after_adjust"] * position["amount"], 2)  # 使用真实价格计算（因为数量为真实数量）
            position["cum_return"] = round((position["last_sale_price"] - position["cost_basis"]) * position["amount"], 2)
            position["adjust_factor"] = adjust_factor
            position["name"] = self.get_equity_name(symbol)
            position["date"] = self.run_date_str

            self.position_value += position["amount"] * float(position["price_after_adjust"])  # 当日持仓市值
