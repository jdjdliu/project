# -*- coding:utf-8 -*-
import datetime
import glob
import smtplib
from email.mime.text import MIMEText

import numpy as np
import os
import pandas as pd
from bigdata.common import constants

## depercated, use save_to_hdf instead
def safe_to_hdf(df, path, table='main', append=False, keys=None):
    if append:
        if os.path.exists(path):
            existing_df = pd.read_hdf(path, 'main')
            df = pd.concat([existing_df, df])
        df = df.drop_duplicates(keys, keep='last')
        df.sort_values(keys, inplace=True)
        df.reset_index(drop=True, inplace=True)

    df.to_hdf(path + '.working', table)
    os.rename(path + '.working', path)
    return df


def __concat_dataframes(df_list, ignore_index=True, copy=True):
    df_list = [x for x in df_list if x is not None] if df_list else None
    if not df_list:
        df = None
    elif len(df_list) == 1:
        df = df_list[0]
    else:
        df = pd.concat(df_list, ignore_index=ignore_index, copy=copy)
    return df

def _filter_by_fields(df, fields):
    if fields:
        df = df[list(set(df.columns) & set(fields))]
    return df

def read_from_hdf(path, table=None, safe_read=True, ignore_index=True, fields=None):
    '''
    从hdf文件读取DataFrame，支持多表，支持数据检查
    :param path: 数据路径
    :param table: 表，字符串|数组|过滤函数(e.g. labmda t: t > 'y_1991')。默认None，表示所有表
    :param safe_read: 是否保证读前后，文件修改时间一致
    :param ignore_index: 合并(concat)多表时是否忽略索引
    :return: DataFrame
    '''
    if not os.path.exists(path):
        return None

    while True:
        mtime_before_read = os.path.getmtime(path)
        store = pd.HDFStore(path, mode='r')
        if isinstance(table, str):
            df_list = [_filter_by_fields(store[table], fields)]
        elif isinstance(table, list):
            df_list = [_filter_by_fields(store[t], fields) for t in table]
        else:
            df_list = [_filter_by_fields(store[t], fields) for t in sorted(store.keys()) if table is None or table(t)]
        store.close()
        del store

        df = __concat_dataframes(df_list, ignore_index=ignore_index, copy=True)

        if safe_read:
            mtime_after_read = os.path.getmtime(path)
            if mtime_before_read != mtime_after_read:
                print('WARNING: mtime changed for %s, re-read ..' % path)
                continue
        return df

def read_file(path, table=None, safe_read=True, ignore_index=True, fields=None):
    pkl_path = path + '.pkl'
    if os.path.exists(pkl_path):
        return read_from_pickle(pkl_path, safe_read, fields=fields)
    return read_from_hdf(path, table, safe_read=safe_read, ignore_index=ignore_index, fields=fields)

def read_files(path, table=None, safe_read=True, ignore_index=True, fields=None):
    df = read_from_pickle_multi_files(path+'.pkl', table, safe_read, ignore_index, fields=fields)
    if df is None:
        df = read_from_hdf_multi_files(path+'.h5', table, safe_read, ignore_index, fields=fields)
    return df

def read_from_pickle(path, safe_read=True, fields=None):
    '''
    从hdf文件读取DataFrame，支持多表，支持数据检查
    :param path: 数据路径
    :param safe_read: 是否保证读前后，文件修改时间一致
    :return: DataFrame
    '''
    if not os.path.exists(path):
        return None

    while True:
        mtime_before_read = os.path.getmtime(path)
        df = pd.read_pickle(path)
        df = _filter_by_fields(df, fields)

        if safe_read:
            mtime_after_read = os.path.getmtime(path)
            if mtime_before_read != mtime_after_read:
                print('WARNING: mtime changed for %s, re-read ..' % path)
                continue
        return df

def read_from_pickle_multi_files(path, table=None, safe_read=True, ignore_index=True, fields=None):
    '''
    read_from_pickle的多文件版。更多见 read_from_pickle
    '''
    filename, ext = os.path.splitext(path)
    if isinstance(table, str):
        path_list = [__get_path_for_table(filename, table, ext)]
    elif isinstance(table, list):
        path_list = [__get_path_for_table(filename, t, ext) for t in table]
    else:
        # TODO
        if table is not None:
            raise Exception('not implemented yet')
        path_list = sorted(glob.glob(__get_path_for_table(filename, '*', ext)))

    df_list = [read_from_pickle(p, safe_read=safe_read, fields=fields) for p in path_list]
    return __concat_dataframes(df_list, ignore_index=ignore_index, copy=True)


def save_to_pickle(path, df, append=True, keys=None, safe_append=True):
    '''
    保存df到pkl文件，支持追加，支持追加时按keys做去重与排序，支持先在备份里写
    :param path: 目标文件
    :param df: 数据，dataframe，假设按keys做了排序去重
    :param append: 是否追加，如果为否，旧数据讲不保留
    :param keys: 数据主键
    :param safe_append: 是否先在备份里写。safe_append如果为False，读的进程可能得到坏的数据
    :return:
    '''
    assert path.endswith('.pkl')
    if append:
        if safe_append:
            path_working = path + '.working'
        else:
            path_working = path
        if os.path.exists(path):
            df_old = pd.read_pickle(path)
            df_merged = pd.concat([df_old, df], ignore_index=True)
            if keys and len(df) > 0 and len(df_old) > 0:
                df_merged = df_merged.drop_duplicates(keys, keep='last')
                df_merged.sort_values(keys, inplace=True)
                df_merged.reset_index(drop=True, inplace=True)
            df = df_merged
    else:
        path_working = path + '.working'
    df.to_pickle(path_working)

    if path_working != path:
        os.rename(path_working, path)

def save_to_pickle_multi_files(path, df, table, append=True, keys=None, safe_append=True, show_progress=False, reset_index=True):
    '''
    save_to_pickle的多文件版。更多见 save_to_pickle
    '''
    if isinstance(table, str):
        items = [(table, df)]
    else:
        items = df.groupby(table)
    for t, df_t in items:
        start_time = datetime.datetime.now()
        if reset_index:
            df_t.reset_index(drop=True, inplace=True)
        filename, ext = os.path.splitext(path)
        path_t = __get_path_for_table(filename, t, ext)
        save_to_pickle(path_t, df_t, append, keys, safe_append)
        if show_progress:
            print('[%s] save_to_pickle_multi_files, timetaken=%ss, table=%s, rows=%s, path=%s' % (
                start_time, (datetime.datetime.now() - start_time).total_seconds(), t, len(df_t), path_t
            ))


def save_files(path, df, table, append=True, keys=None, safe_append=True, show_progress=False, reset_index=True):
    if df.size > 100000000:
        path += '.h5'
        save_to_hdf_multi_files(path, df, table, append, keys=keys, safe_append=safe_append, show_progress=show_progress,reset_index=reset_index)
    else:
        path += '.pkl'
        save_to_pickle_multi_files(path, df, table, append, keys=keys, safe_append=safe_append, show_progress=show_progress,reset_index=reset_index)

def save_file(path, df, table='main', append=True, keys=None, safe_append=True):
    if df.size > 1000000:
        path += '.h5'
        save_to_hdf(path, df, table=table, append=append, keys=keys, safe_append=safe_append)
    else:
        path += '.pkl'
        save_to_pickle(path, df, append=append, keys=keys, safe_append=safe_append)

def save_to_hdf(path, df, table='main', append=True, keys=None, safe_append=True):
    '''
    保存df到hdf文件，支持追加，支持追加时按keys做去重与排序，支持分表，支持先在备份里写
    :param path: 目标文件
    :param df: 数据，dataframe，假设按keys做了排序去重
    :param table: 表名，字符串或者series
    :param append: 是否追加，如果为否，旧数据讲不保留
    :param keys: 数据主键
    :param safe_append: 是否先在备份里写。safe_append如果为False，读的进程可能得到坏的数据
    :return:
    '''
    if append:
        if safe_append:
            path_working = path + '.working'
            if os.path.exists(path):
                store_old = pd.HDFStore(path, mode='r')
            else:
                store_old = None
            store_new = pd.HDFStore(path_working, mode='w')
        else:
            path_working = path
            store_old = pd.HDFStore(path, mode='a')
            store_new = store_old
    else:
        path_working = path + '.working'
        store_old = None
        store_new = pd.HDFStore(path_working, mode='w')

    table = '/' + table
    if isinstance(table, str):
        tables_new = set([table])
    else:
        tables_new = set(table)

    tables_old = []
    if store_old is not None:
        tables_old = set(store_old.keys())

    # 1. for tables not in new: just copy to new store
    if store_new is not store_old:
        for t in tables_old:
            if t not in tables_new:
                store_new[t] = store_old[t]
    # 2. for tables in new: merge and write
    for t in tables_new:
        if isinstance(table, str):
            df_new = df
        else:
            df_new = df[table==t]

        if t in tables_old:
            df_old = store_old[t]
            df_merged = pd.concat([df_old, df_new], ignore_index=True, copy=True)
            if keys and len(df_new) > 0 and len(df_old) > 0:
                # TODO: performance optimization for cases when df_new < df_old
                # old_less_than_new = False
                # for key in keys:
                # TODO: improve performance more
                df_merged = df_merged.drop_duplicates(keys, keep='last')
                df_merged.sort_values(keys, inplace=True)
                df_merged.reset_index(drop=True, inplace=True)
        else:
            df_merged = df_new
        store_new[t] = df_merged

    store_new.close()
    if store_old is not None and store_old is not store_new:
        store_old.close()

    if path_working != path:
        os.rename(path_working, path)


def __get_path_for_table(filename, table, ext):
    return '%s.__T_%s__%s' % (filename, table, ext)


def save_to_hdf_multi_files(path, df, table, append=True, keys=None, safe_append=True, show_progress=False, reset_index=True):
    '''
    save_to_hdf的多文件版。更多见 save_to_hdf
    '''
    if isinstance(table, str):
        items = [(table, df)]
    else:
        items = df.groupby(table)
    for t, df_t in items:
        start_time = datetime.datetime.now()
        if reset_index:
            df_t.reset_index(drop=True, inplace=True)
        filename, ext = os.path.splitext(path)
        path_t = __get_path_for_table(filename, t, ext)
        save_to_hdf(path_t, df_t, 'main', append, keys, safe_append)
        if show_progress:
            print('[%s] save_to_hdf_multi_files, timetaken=%ss, table=%s, rows=%s, path=%s' % (
                start_time, (datetime.datetime.now() - start_time).total_seconds(), t, len(df_t), path_t
            ))


def read_from_hdf_multi_files(path, table=None, safe_read=True, ignore_index=True, fields=None):
    '''
    read_from_hdf的多文件版。更多见 read_from_hdf
    '''
    filename, ext = os.path.splitext(path)
    if isinstance(table, str):
        path_list = [__get_path_for_table(filename, table, ext)]
    elif isinstance(table, list):
        path_list = [__get_path_for_table(filename, t, ext) for t in table]
    else:
        # TODO
        if table is not None:
            raise Exception('not implemented yet')
        path_list = sorted(glob.glob(__get_path_for_table(filename, '*', ext)))

    df_list = [read_from_hdf(p, table='main', safe_read=safe_read, ignore_index=ignore_index, fields=fields) for p in path_list]
    return __concat_dataframes(df_list, ignore_index=ignore_index, copy=True)


def assert_column(df, column):
    if df is None:
        raise Exception('df is None')
    if 'OUTMESSAGE' in df.columns:
        raise Exception('WARNING: ' + str(df['OUTMESSAGE']))
    if column is not None and column not in df.columns:
        raise Exception('column %s not in df.columns=%s' % (column, df.columns))


def build_quarters(start_date, end_date, start_date_delta=0):
    QUARTERS = ['0331', '0630', '0930', '1231']

    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if start_date_delta != 0:
        start_date = start_date + datetime.timedelta(days=start_date_delta)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    quarters = []
    for year in range(start_date.year, end_date.year + 1):
        for quarter in QUARTERS:
            quarter = '%s%s' % (year, quarter)
            if start_date <= pd.to_datetime(quarter) <= end_date:
                quarters.append(quarter)
    return quarters


def build_dividend_quarters(start_date, end_date):
    def _generate_last_quarter(cur_quarter):
        DAYS = ['31', '30', '30', '31']
        left = int(cur_quarter[:6]) - 3
        if left % 100 == 0:
            left -= 88
        right = DAYS[(left % 100) // 3 - 1]
        return '{}{}'.format(left, right)
    quarters = build_quarters(start_date, end_date)
    one_year_ago = format(datetime.date.today() - datetime.timedelta(days=365), '%Y%m%d')
    while quarters[0] > one_year_ago:
        quarters.insert(0, _generate_last_quarter(quarters[0]))
    return quarters


def ensure_data_type(df, col, to_type):
    from_type = df[col].dtype.name
    if to_type == from_type:
        return
    if to_type == 'datetime64' and from_type.startswith('datetime64'):
        return
    if to_type == 'str' and from_type == 'object':
        return

    if to_type.startswith('datetime'):
        df[col] = pd.to_datetime(df[col])
    else:
        df[col] = df[col].astype(np.dtype(to_type))


def ensure_data_types(df, dtypes):
    for col in df.columns:
        if col in dtypes:
            ensure_data_type(df, col, dtypes[col])

# TODO: move to shared
def notify_by_email(to, title, body):
    sender = 'notification@bigquant.com'
    if isinstance(to, str):
        to = [to]
    smtpserver = smtplib.SMTP("mail.bigquant.com", 587)
    smtpserver.starttls()
    # TODO: protect the password
    smtpserver.login(sender, '20e8LPsalfdoi29dvf4eIUFfdsaWHCo')

    # https://docs.python.org/3/library/email-examples.html
    msg = MIMEText(body, 'html')
    msg['Subject'] = title
    msg['From'] = sender
    msg['To'] = ','.join(to)
    smtpserver.sendmail(sender, to, msg.as_string())
    smtpserver.quit()


def build_data_dir(market, data_dir=constants.bar1d_data_dir):
    from bigdata.common.market import Market
    from bigdata.common import constants

    if market == Market.MG_CHINA_STOCK_A.symbol:
        # TODO: move for MG_CHINA_STOCK_A
        market_suffix = ''
    else:
        market_suffix = '_' + market
    return data_dir + market_suffix

def build_bar1d_data_dir(market):
    """
        [已弃用] 请使用 build_data_dir
    """
    from bigdata.common.market import Market
    from bigdata.common import constants

    if market == Market.MG_CHINA_STOCK_A.symbol:
        # TODO: move for MG_CHINA_STOCK_A
        market_suffix = ''
    else:
        market_suffix = '_' + market
    return constants.bar1d_data_dir + market_suffix
