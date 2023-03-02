from functools import partial

import pandas as pd

from bigdata.common import constants
from bigdata.common.datadefs import DataDefs
from bigdata.common.utils import build_data_dir, read_files, save_files, save_to_hdf_multi_files
from bigdata.shared import parallel
from bigdata.shared.utils import data_path, ensure_data_dir_for_file, remove_tzinfo


class BaseMergedStorage:
    def __init__(self):
        self._main_data_dir = ''
        self._merged_data_dir = ''
        self._merged_name_pattern = ''

    def __truncate(self, df, start_date, end_date):
        if start_date is not None and end_date is not None:
            cond = df.date.between(remove_tzinfo(start_date), remove_tzinfo(end_date))
        elif start_date is not None:
            cond = df.date >= remove_tzinfo(start_date)
        elif end_date is not None:
            cond = df.date <= remove_tzinfo(end_date)
        else:
            return df

        if cond is not None:
            df = df[cond]

        return df

    def __read_from_path_list(self, path_list, years,
                              filter_instruments=None, filter_fields=None, filter_start_date=None,
                              filter_end_date=None):
        df_merged = None
        years = ['y_%s' % y for y in years]
        for path in path_list:
            df = read_files(path=path, table=years, fields=filter_fields)
            # df = read_from_hdf_multi_files(path=path, table=years, fields=filter_fields)
            if df is None:
                continue

            df = self.__truncate(df, filter_start_date, filter_end_date)
            if filter_instruments is not None:
                df = df[df.instrument.isin(filter_instruments)]
            # if filter_fields is not None:
            #     df = df[[f for f in filter_fields if f in set(df.columns)]]

            if df_merged is None:
                df_merged = df
            else:
                assert len(df_merged) == len(df)
                # TODO: keys?
                df_merged = df_merged.merge(df, on=['date', 'instrument'], how='inner')
                assert len(df_merged) == len(df)
        return df_merged

    def main_read_instrument(self, instrument, years, group_names,
                             filter_instruments=None, filter_fields=None, filter_start_date=None, filter_end_date=None,
                             price_type=constants.price_type_post_right):
        path_list = []
        for group_name in group_names:
            if constants.price_type_post_right == price_type or 'G100' not in group_name:
                file_path = data_path(self._main_data_dir, instrument, '%s' % (group_name))
            else:
                file_path = data_path('/var/app/data/bigquant/backtest/main/bar1d', price_type, instrument,
                                      '%s' % (group_name))

            path_list.append(file_path)
        # print('path_list:', path_list, ' main dir')
        return self.__read_from_path_list(path_list, years, filter_instruments, filter_fields, filter_start_date,
                                          filter_end_date)

    def main_read_instruments(self, instruments, years, group_names, use_parallel=False,
                              filter_instruments=None, filter_fields=None, filter_start_date=None,
                              filter_end_date=None):
        if not use_parallel:
            df_list = []
            for instrument in instruments:
                df_list.append(
                    self.main_read_instrument(instrument, years, group_names, filter_instruments, filter_fields,
                                              filter_start_date, filter_end_date))
        else:
            df_list = parallel.map(partial(self.main_read_instrument,
                                           years=years,
                                           group_names=group_names,
                                           filter_instruments=filter_instruments,
                                           filter_fields=filter_fields,
                                           filter_start_date=filter_start_date,
                                           filter_end_date=filter_end_date),
                                   instruments)
        return pd.concat(df_list, ignore_index=True, copy=True)

    def main_save_instrument(self, instrument, df, group_names, append, keys=None):
        if len(df) == 0:
            return

        for year, df_year in df.groupby(df.date.dt.year):
            for group_name in group_names:
                df_year_group = df_year[list(set(df_year.columns) & set(DataDefs.GROUPS[group_name]))]
                path = ensure_data_dir_for_file(self._main_data_dir, instrument, '%s' % (group_name))
                save_files(path, df_year_group, 'y_%s' % year, append=append, keys=keys)
                # path = ensure_data_dir_for_file(self._main_data_dir, instrument, '%s.h5' % (group_name))
                # save_to_hdf_multi_files(path, df_year_group, 'y_%s' % year, append=append, keys=keys)

    def _main_save_instruments_worker(self, item):
        return self.main_save_instrument(*item)

    def main_save_instruments(self, df, group_names, append, keys=None, use_parallel=False):
        grouped = df.groupby('instrument')
        if not use_parallel:
            for instrument, instrument_df in grouped:
                self.main_save_instrument(instrument, instrument_df, group_names, append, keys)
        else:
            parallel.map(self._main_save_instruments_worker,
                         [(instrument, instrument_df, group_names, append, keys) for instrument, instrument_df in
                          grouped])

    def merged_read_years(self, years, group_names,
                          filter_instruments=None, filter_fields=None, filter_start_date=None, filter_end_date=None):
        path_list = [data_path(self._merged_data_dir, self._merged_name_pattern % (group_name)) \
                     for group_name in group_names]
        return self.__read_from_path_list(path_list, years, filter_instruments, filter_fields, filter_start_date,
                                          filter_end_date)

    def merged_save_year(self, year, df, group_names, append, keys=None):
        for group_name in group_names:
            path = ensure_data_dir_for_file(self._merged_data_dir, self._merged_name_pattern % group_name)
            save_to_hdf_multi_files(path, df[list(set(df.columns) & set(DataDefs.GROUPS[group_name]))], 'y_%s' % year,
                                    append=append, keys=keys)


class MainMergedStorage(BaseMergedStorage):
    """
        [已弃用] 请使用 MainMergedStorage_v2
    """

    # TODO: remove merged_name_pattern, use dir only
    def __init__(self, main_data_dir, merged_data_dir, merged_name_pattern, reset_index=True):
        self._main_data_dir = main_data_dir
        self._merged_data_dir = merged_data_dir
        self._merged_name_pattern = merged_name_pattern


class MainMergedStorage_v2(BaseMergedStorage):
    def __init__(self, frequency, market, extension='', reset_index=True):
        self._merged_data_dir = constants.merged_data_dir
        if constants.frequency_daily == frequency:
            self._main_data_dir = build_data_dir(market, constants.bar1d_data_dir)
            self._merged_name_pattern = 'bar1d.%s.' + market + extension
        elif constants.frequency_minute == frequency:
            self._main_data_dir = build_data_dir(market, constants.bar1m_data_dir)
            self._merged_name_pattern = 'bar1m.%s.' + market + extension
