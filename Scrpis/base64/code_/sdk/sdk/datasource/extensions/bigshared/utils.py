import datetime
import logging
import os

BASE_DATA_DIR = "/var/app/data"


def data_path(*parts):
    s = BASE_DATA_DIR
    for p in parts:
        s = os.path.join(s, p)
    return s


def ensure_data_dir(*parts):
    s = data_path(*parts)
    if not os.path.exists(s):
        os.makedirs(s)
    return s


def ensure_data_dir_for_file(*parts):
    s = data_path(*parts)
    ensure_data_dir(os.path.dirname(s))
    return s


def read_hdf(path, key=None):
    import pandas as pd

    store = pd.HDFStore(path, mode="r")
    if key is not None:
        df = pd.read_hdf(store, "y_%s" % key)
    else:
        df_list = []
        for key in sorted(store.keys()):
            df_list.append(pd.read_hdf(store, key))
        df = pd.concat(df_list)
        df.reset_index(drop=True, inplace=True)
    store.close()
    return df


def extend_class_methods(obj, **kwargs):
    import types

    for k, v in list(kwargs.items()):
        setattr(obj, k, types.MethodType(v, obj))


def console_out(logFilename):
    """'' Output log to file and console """
    # Define a Handler and set a format which output to file
    logging.basicConfig(
        level=logging.DEBUG,  # 定义输出到文件的log级别，大于此级别的都被输出
        format="%(asctime)s  %(filename)s : %(levelname)s  %(message)s",  # 定义输出log的格式
        datefmt="%Y-%m-%d %A %H:%M:%S",  # 时间
        filename=logFilename,  # log文件名
        filemode="a",
    )  # 写入模式“w”或“a”
    # Define a Handler and set a format which output to console
    console = logging.StreamHandler()  # 定义console handler
    console.setLevel(logging.INFO)  # 定义该handler级别
    formatter = logging.Formatter("%(asctime)s  %(filename)s : %(levelname)s  %(message)s")  # 定义该handler格式
    console.setFormatter(formatter)
    # Create an instance
    logging.getLogger().addHandler(console)  # 实例化添加handler

    # Print information              # 输出日志级别
    # logging.debug('logger debug message')
    # logging.info('logger info message')
    # logging.warning('logger warning message')
    # logging.error('logger error message')
    # logging.critical('logger critical message')


def current_user():
    return os.environ.get("JPY_USER", None)


def is_trading_day(date=datetime.datetime.now(), country_code="CN", delta_days=0,
                   trading_ds="all_trading_days", holidays_ds="holidays_CN"):
    """
    判断是否是交易日

    Args:
        date: str/datetime.datetime   日期，默认是当前日期
        country_code: str 国家, 默认 CN
        delta_days: int  延迟时间，负值表示向前取天数
        trading_ds: str  交易日历 表名
        holidays_ds: str  假期表 表名

    Returns: bool

    """
    from bigdatasource.impl.bigdatasource import DataSource

    day_format = "%Y-%m-%d"
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, day_format)

    if delta_days:
        date += datetime.timedelta(days=delta_days)
    date_str = date.strftime(day_format)
    # 判断是否是周末
    if date.weekday() in [5, 6]:
        return False

    # 判断是否在交易日历中
    trading_day_df = DataSource(trading_ds).read(start_date=date_str, end_date=date_str)
    trading_day_df = trading_day_df[trading_day_df.country_code == country_code]
    if trading_day_df is not None and len(trading_day_df):
        return True

    # 判断是否是假期
    holidays_df = DataSource(holidays_ds).read()
    holidays_df = holidays_df[holidays_df.date == date_str]
    if not holidays_df.empty:
        return False

    return True
