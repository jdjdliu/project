# NOTICE: DONOT import anything here if you are sure what you're doing. be careful about imports here, it will be exposed to users
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from sdk.utils import BigLogger

# from learning.api.opt import PORTFOLIO_OPTIMIZERS  # noqa: F401
from learning.graphimpl.graph import Graph, GraphContinue  # noqa: F401
from learning.module2.common.utils import append_function  # noqa: F401
from learning.toolimpl import parallel_map, picklable  # noqa: F401

# default logger: T.log.info('xxxx')
log = BigLogger("AI")


def norm(values: List[Any]) -> float:
    s = sum(values)
    values = [v / s for v in values]
    return values


def deep_update(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    合并dict，递归的用dict2里的数据更新dict1。对于值为dict或者list类型的，做递归更新。
    :param dict1: 原始数据，dict类型
    :param dict2: 新数据，dict类型
    :return: 更新后的dict
    """
    from bigcharts import highstock

    return highstock.deep_update(dict1, dict2)


def plot(
    df: pd.DataFrame,
    output: Literal["object", "json", "script", "display"] = "display",
    stock: bool = None,
    double_precision: int = 4,
    title: Optional[str] = None,
    chart_type: Literal[
        None, "line", "spline", "area", "areaspline", "column", "bar", "pie", "scatter", "gauge", "arearange", "areasplinerange"
    ] = None,
    x: Optional[str] = None,
    y: Optional[str] = None,
    candlestick: bool = False,
    panes: List[List[str]] = None,
    options: Any = None,
):
    """

        绘制DataFrame数据到可交互的图表，支持多种图表格式

        用highcharts/HighStock实现DataFrame的多种方式的可交互的可视化

        :param DataFrame|Series|dict df: 输入数据，一般是DataFrame类型，索引对应x轴，数据列用作y轴数据；如果不为DataFrame，数据将直接传给Highcharts，可参考Highcharts文档
        :param boolean stock: 是否使用Highstock。默认为None，会根据df.index类型来判断，如果为时间类型，则使用Highstock，否则使用Highcharts
        :param 字符串 title: 标题，也可以通过 options={'title': {'text': 'Your Title Here'}} 设置
        :param 字符串 chart_type: 图表类型，也可以通过 options={'chart': {'type': 'column'}} 设置，支持所有 `Highcharts图表类型 <http://www.highcharts.com/docs/chart-and-series-types/chart-types>`_
        :param 字符串 x: x轴对应的列，默认为None，表示使用index
        :param 字符串数组 y: y轴数据列，默认为None，表示使用所有数据列
        :param 浮点数 double_precision: 浮点数输出精度，默认为4，表示4位小数
        :param dict|function options: 图表设置，dict或者回调函数 (参数 df_options，为df转化后的highcharts输入对象)，参考Highcharts文档
        :param boolean candlestick: 是否绘制蜡烛图，需要输入数据有 open、high、low、close四列
        :param 二维字符串数组 panes: 分栏显示，[['col1', 'col2'], ['col3', 'col4', '50%'], ..]

    示例代码
    --------------
    `Pandas DataFrame数据图表可视化 <https://community.bigquant.com/t/Pandas-DataFrame%E6%95%B0%E6%8D%AE%E5%9B%BE%E8%A1%A8%E5%8F%AF%E8%A7%86%E5%8C%96/46>`_
    """
    from bigcharts import highstock

    return highstock.plot(
        df,
        output=output,
        stock=stock,
        double_precision=double_precision,
        title=title,
        chart_type=chart_type,
        x=x,
        y=y,
        candlestick=candlestick,
        panes=panes,
        options=options,
    )


def plot_tabs(
    tab_list: Any,
    output: Literal["object", "json", "script", "display"] = "display",
    title: Optional[str] = None,
) -> Optional[str]:
    """

        定制输出面板

        :param list tab_list: tab内容列表
        :param 字符串 output: display | script
        :param 字符串 title: 标题

    示例代码
    --------------
    a=T.plot(D.history_data(['000001.SZA']), output='script')
    b=T.plot(D.history_data(['000002.SZA']), output='script',title='b')
    T.plot_tabs([('a', [a,b]),('b', b)])
    """
    from collections import OrderedDict

    from bigcharts import tabs

    odict = OrderedDict(tab_list)
    return tabs.plot(odict, output, title)


# TODO: remove this
def display_df(df: pd.DataFrame) -> None:
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        log.error("数据数据必须是 Pandas DataFrame")
        return

    from IPython.display import display

    display(df)


# TODO: remove this
def render_perf(results: Any, buy_moment: Any, sell_moment: Any) -> Any:
    from biglearning.module2.modules.backtest.v7.perf_render import render

    render(results, buy_moment, sell_moment)


# TODO: remove this
def get_stock_ranker_default_transforms() -> List[str]:
    """
    获取stocker_ranker默认的变换函数。

    变换函数即，对于输入数据源的每一列，从transforms里依序寻找到第一个匹配的表达式，用对应的变换函数对列数据做处理
    """
    from bigdata.common.featuredefs import FeatureDefs

    s = []
    for f in FeatureDefs.FEATURE_LIST:
        if not f.range:
            field = f.field
        elif "$i" in f.field:
            field = f.field.replace("$i", r"\d+")
        else:
            field = f.field + r"_\d+"
        s.append(("^%s$" % field, f.transform_for_ranker))
    return s


def live_run_param(param_name: str, non_live_run_value: str) -> Optional[str]:
    import os

    KNOWN_PARAMS = {
        "trading_date": "TRADING_DATE",
        "trading_date_start": "TRADING_DATE_START",
        "trading_date_end": "TRADING_DATE_END",
    }
    if param_name not in KNOWN_PARAMS:
        raise Exception("unknown live run param: %s" % param_name)

    run_mode = os.environ.get("RUN_MODE", None)
    if run_mode is None:
        return non_live_run_value
    return os.environ.get(KNOWN_PARAMS[param_name], None)


# TODO: remove this
def open_temp_file(name: str, *args: List[Any]) -> Any:
    """
    读写临时文件。注意临时文件随时可能会被清理，请勿用来存放持久数据。
    返回 打开后的文件句柄
    """
    import os

    temp_dir = "/tmp/usertemp/"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    name = os.path.basename(name)
    path = temp_dir + name
    return open(path, *args)
