import datetime
from typing import List
from uuid import UUID

from strategy.schemas import StrategyDailySchema


class RecordDatas(object):
    def __init__(self: "RecordDatas", daily_schema: List[StrategyDailySchema], run_date: datetime.datetime):
        self.all_previous_records_include_today: List = daily_schema

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

        # xxx_ago_portfolio_dict
        if isinstance(run_date, str):
            run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d")
        self.start_index = self.all_previous_records_include_today_len - 1

        week_ago = run_date + datetime.timedelta(days=-7)
        self.start_index, self.week_ago_portfolio_dict = self.sequential_search(week_ago, self.start_index)

        ten_days_ago = run_date + datetime.timedelta(days=-10)
        self.start_index, self.ten_days_ago_portfolio_dict = self.sequential_search(ten_days_ago, self.start_index)

        month_ago = run_date + datetime.timedelta(days=-30)
        self.start_index, self.month_ago_portfolio_dict = self.sequential_search(month_ago, self.start_index)

        three_month_ago = run_date + datetime.timedelta(days=-30 * 3)
        self.start_index, self.three_month_ago_portfolio_dict = self.sequential_search(three_month_ago, self.start_index)

        six_month_ago = run_date + datetime.timedelta(days=-30 * 6)
        self.start_index, self.six_month_ago_portfolio_dict = self.sequential_search(six_month_ago, self.start_index)

        year_ago = run_date + datetime.timedelta(days=-365)
        self.start_index, self.year_ago_portfolio_dict = self.sequential_search(year_ago, self.start_index)

    def sequential_search(self: "RecordDatas", xxx_ago: datetime.datetime, start_index: int):
        if start_index is None or start_index < 0:
            return None, {}
        for i in range(start_index, -1, -1):
            run_date = self.all_previous_records_include_today[i].run_date
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
