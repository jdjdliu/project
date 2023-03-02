import glob
import smtplib
import datetime
import argparse
import pandas as pd
# from bigshared.common.biglogger import BigLogger
from sdk.utils.logger import BigLogger

DAY_FORMAT = "%Y-%m-%d"
log = BigLogger("bigdatabuild")


def default_args_parser():
    """ 默认参数 """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, conflict_handler="resolve")
    parser.add_argument('--start_date', help='start_date eg: 2020-01-01',
                        default=datetime.datetime.now().strftime(DAY_FORMAT))  # noqa
    parser.add_argument('--end_date', help='end_date eg: 2020-01-01',
                        default=datetime.datetime.now().strftime(DAY_FORMAT))  # noqa
    parser.add_argument('--date', help='start_date eg: 2020-01-01',
                        default=datetime.datetime.now().strftime(DAY_FORMAT))  # noqa
    parser.add_argument('--markets', nargs='*', help='markets to build', default=[])
    parser.add_argument('--market', help='market CN / US / HK / CN_STOCK_A', default="")
    parser.add_argument('--fields', nargs='*', help='fields', default=[])
    parser.add_argument('--tables', nargs='*', help='tables, DataSource表名', default=[])
    parser.add_argument('--instruments', nargs='*', help='instruments，股票代码', default=[])
    parser.add_argument('--scode', nargs='*', help='scode，代码', default=[])
    parser.add_argument('--mode', help='mode', default='daily')
    parser.add_argument('--flag', help='flag', default='daily')
    parser.add_argument('--merged', action='store_true', help='按天获取的数据直接保存合并版本（而不是按instrument再分一次）')

    args, unknown = parser.parse_known_args()
    if unknown:
        log.error("unknown args to parser: {}".format(unknown))
    log.info("arguments: {}".format(args))

    if args.markets and not args.market:
        args.market = args.markets[0]
    if not args.markets and args.market:
        args.markets = [args.market]
    return args


args = default_args_parser()


def truncate(df, date_col, start_date, end_date):
    # TODO: support minutes?
    return df[(df[date_col] >= remove_tzinfo(start_date)) & (df[date_col] <= remove_tzinfo(end_date))]


def remove_tzinfo(date):
    if isinstance(date, pd.Timestamp) or isinstance(date, datetime.datetime):
        return date.replace(tzinfo=None)
    else:
        return date
