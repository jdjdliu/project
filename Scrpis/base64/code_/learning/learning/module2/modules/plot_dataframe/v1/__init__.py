import learning.api.tools as T
from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list


bigquant_cacheable = False
bigquant_deprecated = None
bigquant_public = True

# 模块接口定义
bigquant_category = "数据可视化"
bigquant_friendly_name = "绘制DataFrame"
bigquant_doc_url = "https://bigquant.com/docs/"


CHART_TYPES = [
    "area",
    "areaspline",
    "areasplinerange",
    "bar",
    "boxplot",
    "bubble",
    "column",
    "columnrange",
    "errorbar",
    "funnel",
    "gauge",
    "heatmap",
    "line",
    "pie",
    "polygon",
    "pyramid",
    "scatter",
    "solidgauge",
    "spline",
    "treemap",
    "waterfall",
    "candlestick",
    "ohlc",
    "flag",
]


DEFAULT_OPTIONS = """{
    'chart': {
        'height': 400
    }
}"""


def bigquant_run(
    input_data: I.port("数据"),
    title: I.str("标题") = "",
    chart_type: I.choice("图表类型", CHART_TYPES) = "line",
    x: I.str("x轴数据字段，如果不指定，默认用index") = "",
    y: I.str("y轴数据字段，如果不指定，使用x轴外的所有字段，多个字段用英文逗号分隔") = "",
    options: I.code("自定义参数，例如 {'chart': {'height': 500}}，参考Highcharts文档", I.code_python, default=DEFAULT_OPTIONS, specific_type_name="dict") = {},
    candlestick: I.bool("绘制蜡烛图，需要输入数据有 open、high、low、close四列") = False,
    pane_1: I.str("分栏1，用于将输入分为多个栏显示，这里输入访问本栏的字段，多个字段用英文逗号分隔，可以在最后添加一个栏的百分比高度，e.g. close,high,70%") = "",
    pane_2: I.str("分栏2，用于将输入分为多个栏显示，这里输入访问本栏的字段，多个字段用英文逗号分隔，可以在最后添加一个栏的百分比高度，e.g. close,high,70%") = "",
    pane_3: I.str("分栏3，用于将输入分为多个栏显示，这里输入访问本栏的字段，多个字段用英文逗号分隔，可以在最后添加一个栏的百分比高度，e.g. close,high,70%") = "",
    pane_4: I.str("分栏4，用于将输入分为多个栏显示，这里输入访问本栏的字段，多个字段用英文逗号分隔，可以在最后添加一个栏的百分比高度，e.g. close,high,70%") = "",
) -> []:
    """
    绘制DataFrame。该绘图函数拥有更友好的交互体验和更强大的绘图功能，更多[详情](https://bigquant.com/community/t/topic/46)
    """
    y = smart_list(y, sep=",")
    panes = [
        smart_list(pane_1, sep=","),
        smart_list(pane_2, sep=","),
        smart_list(pane_3, sep=","),
        smart_list(pane_4, sep=","),
    ]
    panes = [pane for pane in panes if pane]

    df = input_data.read()
    T.plot(df, title=title, chart_type=chart_type, x=x or None, y=y or None, options=options, candlestick=candlestick, panes=panes or None)

    return Outputs()
