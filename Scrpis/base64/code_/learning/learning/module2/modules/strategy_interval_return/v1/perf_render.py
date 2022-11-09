from collections import OrderedDict

import learning.api.tools as T
import pandas as pd
from bigcharts.tabs import plot as plot_tabs
from jinja2 import Template

stats_template = """
<table class="factor_table">
    <thead>
        <th>&nbsp;</th>
        {% for date in dates %}
            <th>{{ date }}</th>
        {% endfor %}
    </thead>
    <tbody>
        {% for k, v in ret_summary.items() %}
            <tr>
                {% for v_ in v %}
                    <td>{{ v_ }}</td>
                {% endfor %}
            </tr>
        {% endfor %}
    </tbody>
</table>
{{ plot_data }}
"""


def _plot_hist(index, strategy, bm, title):
    """
    绘制每个tab的hist

    """
    df = pd.DataFrame({"策略收益": strategy[1:], "基准收益": bm[1:]}, index=index)
    df = df[df != "--"].dropna(axis=0).astype(float)

    options = {"colors": ["#FA5858", "#5882FA"]}
    return T.plot(df, chart_type="column", title=title, options=options, output="script")


def _plot_tabs(dict, title):
    """
    绘制每个tab图像

    """
    index = dict["index"]
    strategy = dict["strategy"]
    bm = dict["bm"]
    return Template(stats_template).render(
        dates=index, ret_summary=OrderedDict([("strategy", strategy), ("bm", bm)]), plot_data=_plot_hist(index, strategy, bm, title)
    )


def render(results):
    """
    绘制策略收益分布图

    Params:
    -----
    results：dict，包含 period_ret、range_ret 三个key

    """
    period_ret = results["period_ret"]
    range_ret = results["range_ret"]
    year = range_ret["year"]
    quarter = range_ret["quarter"]
    month = range_ret["month"]

    period_ret_html = _plot_tabs(period_ret, "区间收益")
    year_html = _plot_tabs(year, "年度收益")
    quarter_html = _plot_tabs(quarter, "季度收益")
    month_html = _plot_tabs(month, "月度收益")

    odict = OrderedDict(
        [
            ("区间收益", period_ret_html),
            ("年度收益", year_html),
            ("季度收益", quarter_html),
            ("月度收益", month_html),
        ]
    )

    plot_tabs(odict, title="策略区间收益")
