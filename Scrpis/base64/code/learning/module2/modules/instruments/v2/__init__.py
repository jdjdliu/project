import datetime
import collections

from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list


# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = True
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = "数据输入输出"
bigquant_friendly_name = "代码列表"
bigquant_doc_url = "https://bigquant.com/docs/"


MARKETS = collections.OrderedDict(
    [
        ("中国A股", "CN_STOCK_A"),
        ("中国基金", "CN_FUND"),
        ("中国期货", "CN_FUTURE"),
        ("美国股市", "US_STOCK"),
        ("香港股市", "HK_STOCK"),
        # ('中国期权', 'CN_OPTION'),
        ("中国可转债", "CN_CONBOND"),
    ]
)


def get_instruments(start_date="2005-01-01", end_date=datetime.datetime.today().isoformat(), market="CN_STOCK_A"):
    assert str(start_date) <= str(end_date)
    df = DataSource("basic_info_%s" % market).read()
    instruments = sorted(set(df[~((end_date < df.list_date) | (df.delist_date < start_date))].instrument))
    return instruments


def bigquant_run(
    rolling_conf: I.port("滚动训练参数，如果指定，将覆盖参数开始日期和结束日期", optional=True) = None,
    start_date: I.str("开始日期，示例 2017-01-01", can_set_liverun_param=True) = "",
    end_date: I.str("结束日期，示例 2017-01-01", can_set_liverun_param=True) = "",
    # TODO:由于美股和港股股票信息表中没有上市\退市日期，所以暂时去掉这两个股的选项
    # market: I.choice('交易市场，见文档[交易市场](https://bigquant.com/docs/develop/datasource/deprecated/trading_market.html)', ['CN_STOCK_A', 'CN_FUND', 'CN_FUTURE', 'US_STOCK', 'HK_STOCK']) = 'CN_STOCK_A',
    market: I.choice(
        "交易市场，见文档[交易市场](https://bigquant.com/docs/develop/datasource/deprecated/trading_market.html)",
        ["CN_STOCK_A", "CN_FUND", "CN_FUTURE", "CN_CONBOND"],
    ) = "CN_STOCK_A",
    instrument_list: I.code("代码列表，每行一个，如果指定，market参数将被忽略", auto_complete_type="stocks") = None,
    max_count: I.int("最大数量，0表示没有限制，一般用于在小数据上测试和调试问题") = 0,
) -> [I.port("数据", "data")]:
    """


    获取指定市场和指定时间区间有效的代码列表


    """
    market = MARKETS.get(market, market)
    if rolling_conf is not None:
        rolling_data = rolling_conf.read_pickle()
        if not rolling_data is None:
            start_date = rolling_data[0]["start_date"]
            end_date = rolling_data[-1]["end_date"]

    if isinstance(instrument_list, list) and len(instrument_list):
        instruments = instrument_list
    elif isinstance(instrument_list, str) and instrument_list.strip():
        instruments = smart_list(instrument_list, sort=True)
    else:
        instruments = get_instruments(start_date=start_date, end_date=end_date, market=market)

    if max_count != 0:
        instruments = instruments[:max_count]

    instruments = {
        "market": market,
        "instruments": instruments,
        "start_date": start_date,
        "end_date": end_date,
    }

    return Outputs(data=DataSource.write_pickle(instruments))


def bigquant_cache_key(kwargs):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    if kwargs["end_date"] >= today:
        return None
    if kwargs.get("instrument_list", None) is not None:
        kwargs["instrument_list"] = smart_list(kwargs["instrument_list"], sort=True)
    return kwargs


if __name__ == "__main__":
    # 测试代码
    pass
