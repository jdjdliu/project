import datetime
import json
import os
import pickle
from typing import List

from sdk.auth import Credential
from sdk.strategy import StrategyClient
from sdk.strategy.schemas import StrategyDailySchema, StrategyPerformanceSchema
from sdk.strategy.schemas.performance import StrategyDailySchema
from sdk.utils import BigLogger

# log = logbook.Logger('forward_test')
log = BigLogger("forward_test")

credential = Credential.from_env()


class RecordDatas(object):
    def __init__(self, algo_id, run_date):
        try:
            self.all_previous_records_include_today: List[StrategyDailySchema] = StrategyClient.get_all_strategy_daily(
                strategy_id=algo_id, run_date=run_date, credential=credential
            )
        except Exception: 
            self.all_previous_records_include_today = []

        self.all_previous_records = []
        self.last_record = None
        self.first_record = None
        self.today_record = None

        self.all_previous_records_include_today_len = len(self.all_previous_records_include_today)

        if self.all_previous_records_include_today_len > 0:
            self.first_record = self.all_previous_records_include_today[0]
            if self.all_previous_records_include_today[-1].run_date.strftime("%Y-%m-%d") == run_date:
                self.today_record = self.all_previous_records_include_today[-1]
                self.all_previous_records = self.all_previous_records_include_today[:-1]
                if self.all_previous_records_include_today_len >= 2:
                    self.last_record = self.all_previous_records_include_today[-2]
            else:
                self.last_record = self.all_previous_records_include_today[-1]
                self.all_previous_records = self.all_previous_records_include_today[:]

        # log.info("first record: {}".format(self.first_record))
        # log.info("last record: {}".format(self.last_record))
        # log.info("today record: {}".format(self.today_record))

        # xxx_ago_portfolio_dict
        if isinstance(run_date, str):
            run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d")
        self.start_index = self.all_previous_records_include_today_len - 1

        week_ago = (run_date + datetime.timedelta(days=-7)).strftime("%Y-%m-%d")
        self.start_index, self.week_ago_portfolio_dict = self.sequential_search(week_ago, self.start_index)

        ten_days_ago = (run_date + datetime.timedelta(days=-10)).strftime("%Y-%m-%d")
        self.start_index, self.ten_days_ago_portfolio_dict = self.sequential_search(ten_days_ago, self.start_index)

        month_ago = (run_date + datetime.timedelta(days=-30)).strftime("%Y-%m-%d")
        self.start_index, self.month_ago_portfolio_dict = self.sequential_search(month_ago, self.start_index)

        three_month_ago = (run_date + datetime.timedelta(days=-30 * 3)).strftime("%Y-%m-%d")
        self.start_index, self.three_month_ago_portfolio_dict = self.sequential_search(three_month_ago, self.start_index)

        six_month_ago = (run_date + datetime.timedelta(days=-30 * 6)).strftime("%Y-%m-%d")
        self.start_index, self.six_month_ago_portfolio_dict = self.sequential_search(six_month_ago, self.start_index)

        year_ago = (run_date + datetime.timedelta(days=-365)).strftime("%Y-%m-%d")
        self.start_index, self.year_ago_portfolio_dict = self.sequential_search(year_ago, self.start_index)

    def sequential_search(self, xxx_ago, start_index):
        if start_index is None or start_index < 0:
            return None, {}
        for i in range(start_index, -1, -1):
            run_date = self.all_previous_records_include_today[i].run_date.strftime("%Y-%m-%d")
            if run_date <= xxx_ago:
                if run_date == xxx_ago and self.all_previous_records_include_today[i].portfolio:
                    return i, self.all_previous_records_include_today[i].portfolio
                elif (
                    run_date < xxx_ago
                    and i + 1 < len(self.all_previous_records_include_today)
                    and self.all_previous_records_include_today[i + 1].portfolio
                ):
                    return i + 1, self.all_previous_records_include_today[i + 1].portfolio
                else:
                    return i, {}
        return None, {}


def get_ago_portfolio_dict(timeperiod, record_datas):
    if timeperiod == "week":
        return record_datas.week_ago_portfolio_dict
    elif timeperiod == "ten_days":
        return record_datas.ten_days_ago_portfolio_dict
    elif timeperiod == "month":
        return record_datas.month_ago_portfolio_dict
    elif timeperiod == "three_month":
        return record_datas.three_month_ago_portfolio_dict
    elif timeperiod == "six_month":
        return record_datas.six_month_ago_portfolio_dict
    else:
        return record_datas.year_ago_portfolio_dict


def get_win_ratio(daily_records, equity_algo_run_date, win_loss_count=None, win_ratio=None):
    try:
        num_trading_days = daily_records[-1].trading_days
        win_count = 0
        loss_count = 0
        start_days = 0
        # and daily_records[-1].run_date
        cal_from_start = True
        if win_loss_count and win_ratio and equity_algo_run_date:
            cal_from_start = False
            # 判断从新运行，运行以前的日期的情况
            daily_records_run_date = daily_records[-1].run_date
            if isinstance(daily_records_run_date, str):
                daily_records_run_date = datetime.datetime.strptime(daily_records_run_date, "%Y-%m-%d").date()
            if equity_algo_run_date.date() == daily_records_run_date:
                return win_ratio, win_loss_count
            if equity_algo_run_date.date() < daily_records_run_date:
                cal_from_start = True
        if not cal_from_start:
            start_days = num_trading_days - 1
        for i in range(start_days, num_trading_days):
            if i == 0:
                continue
            transactions = daily_records[i].transactions
            if len(transactions) <= 0:
                continue
            # 上一个交易日的持仓
            positions_map = {}
            for lp in daily_records[i - 1].positions:
                positions_map[lp["sid"]] = lp
            # 当前交易日的交易记录
            for t in transactions:
                if t["amount"] >= 0:
                    continue
                symbol = t["sid"]
                if symbol not in positions_map:
                    continue
                # 前一个交易日的持仓
                position = positions_map[symbol]
                # 当天的新下单，当天均消化完毕
                commission = t["commission"]
                cost_basis = position["cost_basis"]
                sold_price = t["price"]
                if cost_basis <= 0:
                    # 为什么会出现这种情况呢？
                    # 假设第一天花10元买了100股Ａ股票，第二天以20元卖了60，第三天的时候剩下的40股
                    # cost_basis = (20 * 60 - 10 * 100) /
                    # 40，这种情况下再怎么卖，都时盈利的，这种case直接过滤掉
                    continue
                if sold_price * abs(t["amount"]) > cost_basis * abs(t["amount"]) + commission:
                    win_count += 1
                elif sold_price * abs(t["amount"]) < cost_basis * abs(t["amount"]) + commission:
                    loss_count += 1
        if not cal_from_start:
            return (
                round((win_count + win_loss_count * win_ratio) / (win_count + loss_count + win_loss_count), 9),
                win_count + loss_count + win_loss_count,
            )
        else:
            return (round(win_count / (win_count + loss_count), 9) if win_count + loss_count != 0 else None, win_count + loss_count)
    except Exception as e:
        log.error(
            "forward_test  get_win_ratio  daily_records_id={} run_date {} hit Exception, e {}".format(
                daily_records[-1].id, daily_records[-1].run_date, e
            )
        )
        return None, None
