import pandas as pd

from bigdata.api.datareader import DataReaderV2
from bigdata.common import constants
from bigdata.common.datastorage import MainMergedStorage, MainMergedStorage_v2
from bigdata.common.market import Market
from bigdata.shared import parallel

D2 = DataReaderV2()
KEYS = ['date', 'instrument']


def timeme(func):
    # TODO: move to shared
    def func_wrapper(*args, **kwargs):
        import datetime
        start_time = datetime.datetime.now()
        ret = func(*args, **kwargs)
        msg = 'timeme: %s, timetaken=%ss' % (func.__qualname__, (datetime.datetime.now() - start_time).total_seconds())
        if len(args) > 0 and hasattr(args[0], 'log'):
            args[0].log.info(msg)
        else:
            print(msg)
        return ret
    return func_wrapper


# https://wiki2.bigquant.com/pages/viewpage.action?pageId=5570706
class BaseDataProcessor:
    def __init__(self, log, start_date, end_date, market, data_back_days_count, phase_main_input_groups,
                 phase_main_output_groups, phase_merged_input_groups, phase_merged_output_groups,
                 append, frequency=constants.frequency_daily):
        """
        :param frequency: 数据周期，日，分钟 传入'daily' or 'minute', 目前功能上只支持'daily'
        """
        self.log = log

        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.market = market
        # 需要额外加载的start_date之前的数据，在 process main的时候用到
        self.data_back_days_count = data_back_days_count

        self.phase_main_input_groups = phase_main_input_groups
        self.phase_main_output_groups = phase_main_output_groups
        # phase main 会自动加入 phase merged 的输入
        self.phase_merged_input_groups = phase_main_output_groups + phase_merged_input_groups
        self.phase_merged_output_groups = phase_merged_output_groups

        # TODO: for main and merge phase
        self.append = append

        self.storage = MainMergedStorage_v2(frequency, market, '.h5')

        self.log.info('BaseDataProcessor, %s, %s, %s' % (self.start_date, self.end_date, self.data_back_days_count))

    def _load_history_data_for_instrument(self, instrument, group_names, back_days_count):
        df = self.storage.main_read_instrument(
            instrument, range(self.start_date.year, self.end_date.year + 1), group_names)
        if df is None:
            return None
        df = df[df.date <= self.end_date]
        df = self.phase_main_filter_loaded_df(instrument, df)

        # 特征抽取需要历史数据，加载足够的历史数据：back_days_count
        if back_days_count <= 0:
            return df

        df_list = [df]
        back_rows_to_load = back_days_count - len(df[df.date < self.start_date])
        year = self.start_date.year
        while year > constants.DATA_START_YEAR and back_rows_to_load > 0:
            year -= 1
            y_df = self.storage.main_read_instrument(instrument, [year], group_names)
            if y_df is None:
                break

            y_df = self.phase_main_filter_loaded_df(instrument, y_df)

            if len(y_df) > 0:
                if len(y_df) > back_rows_to_load:
                    y_df = y_df[:back_rows_to_load]
                df_list.append(y_df)
                back_rows_to_load -= len(y_df)
        if len(df_list) > 1:
            df = pd.concat(reversed(df_list), ignore_index=True, copy=True)
        return df

    def process_main_worker(self, instrument):
        # self.log.info('process_main, instrument %s ..' % instrument)
        df = self._load_history_data_for_instrument(instrument, self.phase_main_input_groups,
                                                     self.data_back_days_count)
        if df is None:
            return
        df = self.process_instrument(instrument, df)
        df = df[df.date.between(self.start_date, self.end_date)]
        self.storage.main_save_instrument(instrument, df, self.phase_main_output_groups, self.append, keys=KEYS)

    @timeme
    def process_main(self):
        if not self.phase_main_output_groups:
            self.log.info('process_main: no output groups, skip')
            return
        instruments = D2.instruments(self.start_date, self.end_date, self.market)
        parallel.map(self.process_main_worker, instruments)

    @timeme
    def process_merged_worker(self, year):
        self.log.info('process_merged_worker %s ..' % year)
        instrument_list = D2.instruments('%s-01-01' % year, '%s-12-31' % year, self.market)
        self.log.info('main_read_instruments ..')
        df = self.storage.main_read_instruments(instrument_list, [year], self.phase_merged_input_groups, use_parallel=True)


        self.log.info('process_year to extract features, %s rows ..' % len(df))
        df = self.process_year(year, df)

        if self.phase_merged_output_groups:
            self.log.info('main_save_instruments, rows=%s, groups=%s ..' % (len(df), self.phase_merged_output_groups))
            self.storage.main_save_instruments(df, self.phase_merged_output_groups, self.append, keys=KEYS, use_parallel=True)

        self.log.info('merged_save_year, rows=%s, groups=%s ..' % (len(df), self.phase_main_output_groups + self.phase_merged_output_groups))
        self.storage.merged_save_year(
            year, df, self.phase_main_output_groups + self.phase_merged_output_groups, self.append, keys=KEYS)

    @timeme
    def process_merged(self):
        if not self.phase_merged_output_groups and not self.phase_main_output_groups:
            self.log.info('process_merged: no output groups, skip')
            return

        parallel.map(self.process_merged_worker, range(self.start_date.year, self.end_date.year + 1), processes_count=1)

    @timeme
    def process(self):
        self.process_main()
        self.process_merged()

    def phase_main_filter_loaded_df(self, instrument, df):
        return df

    def process_instrument(self, instrument, df):
        raise Exception('not implemented')

    def process_year(self, year, df):
        raise Exception('not implemented')
